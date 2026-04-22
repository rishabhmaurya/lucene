/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.fsst.FSSTDecompressor;
import org.apache.lucene.codecs.lucene90.fsst.FSSTPlusPrefixChunker;
import org.apache.lucene.codecs.lucene90.fsst.FSSTSymbolTable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * FSST+ term dictionary reader. Reads blocks of 128 sorted terms with DP-optimized prefix
 * extraction and per-term FSST compression. Supports O(1) random access via lookupOrd and
 * efficient sequential iteration via next().
 */
final class FSSTPlusTermsDict {
  final long termsDictSize;
  final LongValues blockOffsets;
  final IndexInput dataInput;
  final FSSTDecompressor decompressor;
  final BytesRef term;
  final int blockSize;
  final long dataStart;
  final byte[] resultBuf;
  final byte[] decodeBuf;
  // Inlined symbol table for fast decompression (avoids method call to FSSTDecompressor)
  final int[] symLen;
  final long[] symVal;

  // Cached block — entire block loaded into memory
  int cachedBlockIdx = -1;
  // Cached decompressed prefix — avoids re-decompressing for consecutive terms in same chunk
  int cachedPrefixPos = -1;
  int cachedPrefixDecodedLen = 0;
  final byte[] cachedPrefixDecoded;
  byte[] cachedBlock;
  int cachedBlockLen;
  int cachedNumStrings;
  int cachedNumChunks;
  int cachedHeaderLen; // bytes before prefix area
  int cachedPrefixAreaLen;
  int cachedSuffixAreaOffset; // offset within cachedBlock where suffix area starts
  // Sequential iteration state — for incremental next()
  long seqOrd = -1;
  int seqLocalIdx = -1;
  int seqSuffixPos = -1;

  FSSTPlusTermsDict(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data) throws IOException {
    this.termsDictSize = entry.termsDictSize;
    this.blockSize = FSSTPlusPrefixChunker.BLOCK_SIZE;
    IndexInput dataSlice = data.slice(
        "fsstplus-terms",
        entry.termsDataOffset - entry.symbolTableLength,
        entry.termsDataLength + entry.symbolTableLength);
    byte[] tableBytes = new byte[entry.symbolTableLength];
    dataSlice.readBytes(tableBytes, 0, tableBytes.length);
    FSSTSymbolTable symbolTable = FSSTSymbolTable.load(tableBytes);
    this.decompressor = new FSSTDecompressor(symbolTable);
    this.symLen = new int[symbolTable.len.length];
    for (int i = 0; i < symbolTable.len.length; i++) symLen[i] = symbolTable.len[i] & 0xFF;
    this.symVal = symbolTable.decodeLong;      this.dataInput = dataSlice;
    this.dataStart = entry.symbolTableLength;
    // Size buffers from metadata — no growth needed (matches LZ4 TermsDict pattern)
    // +8: FSST symbols are up to 8 bytes; byte-by-byte fallback writes output[outPos..outPos+7]
    int slack = 8;
    this.resultBuf = new byte[entry.maxTermLength + slack];
    this.decodeBuf = new byte[entry.maxTermLength + slack];
    this.cachedPrefixDecoded = new byte[entry.maxTermLength + slack];
    this.term = new BytesRef(resultBuf);
    this.cachedBlock = new byte[65536];
    RandomAccessInput addrSlice = data.randomAccessSlice(
        entry.termsAddressesOffset, entry.termsAddressesLength);
    this.blockOffsets = DirectMonotonicReader.getInstance(
        entry.termsAddressesMeta, addrSlice, false);
  }

  BytesRef lookupOrd(int ord) throws IOException {
    int blockIdx = ord / blockSize;
    int localIdx = ord % blockSize;
    if (blockIdx != cachedBlockIdx) loadBlock(blockIdx);

    int suffixOffsetInBlock = readInt24(2 + localIdx * 3);
    int suffixPos = cachedSuffixAreaOffset + suffixOffsetInBlock;

    int prefixLen = (cachedBlock[suffixPos] & 0xFF) | ((cachedBlock[suffixPos + 1] & 0xFF) << 8);
    int resultLen = 0;

    if (prefixLen > 0) {
      int prefixOff = (cachedBlock[suffixPos + 2] & 0xFF) | ((cachedBlock[suffixPos + 3] & 0xFF) << 8);
      int prefixPos = cachedHeaderLen + prefixOff;
      if (prefixPos != cachedPrefixPos) {
        int prefixCompLen = (cachedBlock[prefixPos] & 0xFF) | ((cachedBlock[prefixPos + 1] & 0xFF) << 8);
        cachedPrefixDecodedLen = inlineDecode(cachedBlock, prefixPos + 2, prefixCompLen, cachedPrefixDecoded);
        cachedPrefixPos = prefixPos;
      }
      resultLen = Math.min(cachedPrefixDecodedLen, prefixLen);
      System.arraycopy(cachedPrefixDecoded, 0, resultBuf, 0, resultLen);
      suffixPos += 4;
    } else {
      suffixPos += 2;
    }

    // Compute suffix compressed length
    int suffixEnd;
    if (localIdx + 1 < cachedNumStrings) {
      int nextSuffixOffset = readInt24(2 + (localIdx + 1) * 3);
      suffixEnd = cachedSuffixAreaOffset + nextSuffixOffset;
    } else {
      suffixEnd = cachedBlockLen;
    }
    int suffixCompLen = suffixEnd - suffixPos;

    if (suffixCompLen > 0) {
      // Inline FSST decompress for suffix — hot path, avoids method call overhead
      final int[] sl = symLen;
      final long[] sv = symVal;
      final byte[] in = cachedBlock;
      final byte[] out = resultBuf;
      int pos = suffixPos, end = suffixPos + suffixCompLen, outPos = resultLen;
      while (pos < end) {
        int code = in[pos++] & 0xFF;
        if (code != 0xFF) {
          BitUtil.VH_LE_LONG.set(out, outPos, sv[code]);
          outPos += sl[code];
        } else {
          out[outPos++] = in[pos++];
        }
      }
      resultLen = outPos;
    }

    // Track sequential state for next()
    seqOrd = ord;
    seqLocalIdx = localIdx;
    seqSuffixPos = suffixEnd;

    term.bytes = resultBuf;
    term.offset = 0;
    term.length = resultLen;
    return term;
  }

  /**
   * Incremental sequential iteration — avoids division, modulo, and header reads.
   * Used by termsEnum() for OrdinalMap.build and merge.
   */
  BytesRef next() throws IOException {
    long nextOrd = seqOrd + 1;
    if (nextOrd >= termsDictSize) return null;

    int localIdx = seqLocalIdx + 1;
    if (localIdx >= cachedNumStrings || cachedBlockIdx < 0) {
      // Cross block boundary — fall back to lookupOrd which loads the new block
      return lookupOrd((int) nextOrd);
    }

    // Incremental: we already know suffixPos from previous call
    int suffixPos = seqSuffixPos;

    int prefixLen = (cachedBlock[suffixPos] & 0xFF) | ((cachedBlock[suffixPos + 1] & 0xFF) << 8);
    int resultLen = 0;

    if (prefixLen > 0) {
      int prefixOff = (cachedBlock[suffixPos + 2] & 0xFF) | ((cachedBlock[suffixPos + 3] & 0xFF) << 8);
      int prefixPos = cachedHeaderLen + prefixOff;
      if (prefixPos != cachedPrefixPos) {
        int prefixCompLen = (cachedBlock[prefixPos] & 0xFF) | ((cachedBlock[prefixPos + 1] & 0xFF) << 8);
        cachedPrefixDecodedLen = inlineDecode(cachedBlock, prefixPos + 2, prefixCompLen, cachedPrefixDecoded);
        cachedPrefixPos = prefixPos;
        resultLen = Math.min(cachedPrefixDecodedLen, prefixLen);
        System.arraycopy(cachedPrefixDecoded, 0, resultBuf, 0, resultLen);
      } else {
        // Same prefix chunk — already in resultBuf from previous next(), skip copy
        resultLen = Math.min(cachedPrefixDecodedLen, prefixLen);
      }
      suffixPos += 4;
    } else {
      suffixPos += 2;
    }

    // Compute suffix end from next entry's offset (or block end)
    int suffixEnd;
    if (localIdx + 1 < cachedNumStrings) {
      int nextSuffixOffset = readInt24(2 + (localIdx + 1) * 3);
      suffixEnd = cachedSuffixAreaOffset + nextSuffixOffset;
    } else {
      suffixEnd = cachedBlockLen;
    }
    int suffixCompLen = suffixEnd - suffixPos;

    if (suffixCompLen > 0) {
      // Inline FSST decompress directly into resultBuf
      final int[] sl = symLen;
      final long[] sv = symVal;
      final byte[] in = cachedBlock;
      final byte[] out = resultBuf;
      int pos = suffixPos, end = suffixPos + suffixCompLen, outPos = resultLen;
      while (pos < end) {
        int code = in[pos++] & 0xFF;
        if (code != 0xFF) {
          BitUtil.VH_LE_LONG.set(out, outPos, sv[code]);
          outPos += sl[code];
        } else {
          out[outPos++] = in[pos++];
        }
      }
      resultLen = outPos;
    }

    seqOrd = nextOrd;
    seqLocalIdx = localIdx;
    seqSuffixPos = suffixEnd;

    term.bytes = resultBuf;
    term.offset = 0;
    term.length = resultLen;
    return term;
  }

  /** Inline FSST decompress — avoids method call overhead on hot path. */
  int inlineDecode(byte[] in, int off, int len, byte[] out) {
    final int[] sl = symLen;
    final long[] sv = symVal;
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      int code = in[pos++] & 0xFF;
      if (code != 0xFF) {
        BitUtil.VH_LE_LONG.set(out, outPos, sv[code]);
        outPos += sl[code];
      } else {
        out[outPos++] = in[pos++];
      }
    }
    return outPos;
  }

  int readShort(int pos) {
    return (cachedBlock[pos] & 0xFF) | ((cachedBlock[pos + 1] & 0xFF) << 8);
  }

  int readInt24(int pos) {
    return (cachedBlock[pos] & 0xFF) | ((cachedBlock[pos + 1] & 0xFF) << 8) | ((cachedBlock[pos + 2] & 0xFF) << 16);
  }

  void loadBlock(int blockIdx) throws IOException {
    cachedBlockIdx = blockIdx;
    cachedPrefixPos = -1;
    seqLocalIdx = -1;
    seqSuffixPos = -1;
    long blockStart = dataStart + blockOffsets.get(blockIdx);
    long blockEnd = getBlockEnd(blockIdx);
    cachedBlockLen = (int) (blockEnd - blockStart);
    if (cachedBlock.length < cachedBlockLen) {
      cachedBlock = new byte[cachedBlockLen];
    }
    dataInput.seek(blockStart);
    dataInput.readBytes(cachedBlock, 0, cachedBlockLen);

    cachedNumStrings = cachedBlock[0] & 0xFF;
    cachedNumChunks = cachedBlock[1] & 0xFF;
    cachedHeaderLen = 2 + cachedNumStrings * 3;

    // Scan prefix area to find suffix area offset
    int pos = cachedHeaderLen;
    for (int c = 0; c < cachedNumChunks; c++) {
      int compLen = (cachedBlock[pos] & 0xFF) | ((cachedBlock[pos + 1] & 0xFF) << 8);
      pos += 2 + compLen;
    }
    cachedSuffixAreaOffset = pos;
  }

  long getBlockEnd(int blockIdx) {
    int numBlocks = (int) ((termsDictSize + blockSize - 1) / blockSize);
    if (blockIdx + 1 < numBlocks) return dataStart + blockOffsets.get(blockIdx + 1);
    return dataInput.length();
  }
  }
