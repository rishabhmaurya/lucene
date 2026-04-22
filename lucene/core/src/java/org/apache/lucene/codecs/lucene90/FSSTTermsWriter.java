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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.lucene90.fsst.FSSTCompressor;
import org.apache.lucene.codecs.lucene90.fsst.FSSTPlusPrefixChunker;
import org.apache.lucene.codecs.lucene90.fsst.FSSTSymbolTable;
import org.apache.lucene.codecs.lucene90.fsst.FSSTSymbolTableBuilder;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/** Static writer methods for FSST and FSST+ term dictionaries. */
final class FSSTTermsWriter {
  private FSSTTermsWriter() {}

  static void writeFSST(IndexOutput meta, IndexOutput data, SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeVLong(size);
    meta.writeByte(Lucene90DocValuesConsumer.TERMS_DICT_FSST);

    // Collect evenly-spaced sample terms for symbol table training
    // Sample 1% of terms, min 1000, max 50000 — evenly spaced across sorted order
    int sampleCount = (int) Math.min(Math.max(1000, size / 100), 50000);
    sampleCount = (int) Math.min(sampleCount, size);
    long stride = Math.max(1, size / sampleCount);
    List<BytesRef> sampleTerms = new ArrayList<>(sampleCount);
    {
      TermsEnum iterator = values.termsEnum();
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next(), ord++) {
        if (ord % stride == 0) {
          sampleTerms.add(BytesRef.deepCopyOf(term));
        }
      }
    }

    // Train symbol table
    FSSTSymbolTable symbolTable = FSSTSymbolTableBuilder.build(sampleTerms);
    FSSTCompressor compressor = new FSSTCompressor(symbolTable);
    byte[] tableBytes = symbolTable.toBytes();

    // Write symbol table to data
    meta.writeVInt(tableBytes.length);
    data.writeBytes(tableBytes, 0, tableBytes.length);

    // Pass 2: compress each term, collect offsets
    long start = data.getFilePointer();
    int maxTermLength = 0;
    byte[] compressBuf = new byte[65536];
    int[] offsets = new int[(int) size + 1];
    int termIdx = 0;
    {
      TermsEnum iterator = values.termsEnum();
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        offsets[termIdx++] = (int) (data.getFilePointer() - start);
        int compLen = compressor.compress(term.bytes, term.offset, term.length, compressBuf);
        data.writeBytes(compressBuf, 0, compLen);
        maxTermLength = Math.max(maxTermLength, term.length);
      }
    }
    offsets[termIdx] = (int) (data.getFilePointer() - start);

    meta.writeInt(maxTermLength);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);

    // Write offsets using DirectMonotonicWriter (compact on disk)
    meta.writeInt(Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    DirectMonotonicWriter addrWriter =
        DirectMonotonicWriter.getInstance(
            meta, addressOutput, size + 1, Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
    for (int i = 0; i <= size; i++) {
      addrWriter.add(offsets[i]);
    }
    addrWriter.finish();
    long addrStart = data.getFilePointer();
    addressBuffer.copyTo(data);
    meta.writeLong(addrStart);
    meta.writeLong(data.getFilePointer() - addrStart);

    // Block shift for reverse index DirectMonotonic
    meta.writeInt(Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);

    // Reverse index (reuse existing method)
    Lucene90DocValuesConsumer.writeTermsIndex(meta, data, values);
  }


  /**
   * FSST+ term dictionary: prefix extraction via DP + FSST compression on suffixes.
   *
   * <p>On-disk layout per block of up to 128 terms:
   * <ul>
   *   <li>Block header: numStrings(1B), numChunks(1B), suffixOffsets[numStrings](2B each)
   *   <li>Prefix data: per chunk [prefixCompressedLen(2B), FSST(prefix)]
   *   <li>Suffix data: per string [prefixLen(1B), jumpBack(2B if prefixLen>0), FSST(suffix)]
   * </ul>
   */

  static void writeFSSTPlus(IndexOutput meta, IndexOutput data, SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeVLong(size);
    meta.writeByte(Lucene90DocValuesConsumer.TERMS_DICT_FSST_PLUS);

    final int blockSize = FSSTPlusPrefixChunker.BLOCK_SIZE;
    int numBlocks = (int) ((size + blockSize - 1) / blockSize);

    // === Pass 1: Run DP on all blocks, store prefix lengths ===
    byte[] blockBuf = new byte[blockSize * 256];
    int[] termOffsets = new int[blockSize + 1];
    FSSTPlusPrefixChunker chunker = new FSSTPlusPrefixChunker();

    // Also collect suffix samples for FSST training
    List<BytesRef> sampleSuffixes = new ArrayList<>();
    int sampleCount = (int) Math.min(Math.max(1000, size / 100), 50000);
    long sampleStride = Math.max(1, size / sampleCount);

    int maxTermLength = 0;

    {
      TermsEnum iterator = values.termsEnum();
      long ord = 0;
      int posInBlock = 0;
      int bufPos = 0;

      for (BytesRef term = iterator.next(); term != null; term = iterator.next(), ord++) {
        maxTermLength = Math.max(maxTermLength, term.length);
        if (bufPos + term.length > blockBuf.length)
          blockBuf = java.util.Arrays.copyOf(blockBuf, Math.max(blockBuf.length * 2, bufPos + term.length));
        termOffsets[posInBlock] = bufPos;
        System.arraycopy(term.bytes, term.offset, blockBuf, bufPos, term.length);
        bufPos += term.length;
        posInBlock++;

        if (posInBlock == blockSize || ord == size - 1) {
          termOffsets[posInBlock] = bufPos;
          chunker.solve(blockBuf, termOffsets, posInBlock, 0.67f);

          // Store prefix lengths and sample suffixes
          for (int c = 0; c < chunker.numChunks(); c++) {
            int pLen = chunker.chunkPrefixLen(c);
            for (int t = chunker.chunkStart(c); t < chunker.chunkEnd(c); t++) {
              long globalOrd = ord - posInBlock + 1 + t;
              // Sample suffixes for FSST training
              if (globalOrd % sampleStride == 0) {
                int tLen = termOffsets[t + 1] - termOffsets[t];
                int sLen = tLen - pLen;
                if (sLen > 0) sampleSuffixes.add(BytesRef.deepCopyOf(new BytesRef(blockBuf, termOffsets[t] + pLen, sLen)));
                if (pLen > 0 && t == chunker.chunkStart(c))
                  sampleSuffixes.add(BytesRef.deepCopyOf(new BytesRef(blockBuf, termOffsets[t], pLen)));
              }
            }
          }
          posInBlock = 0;
          bufPos = 0;
        }
      }
    }

    // === Build FSST symbol table from sampled suffixes ===
    FSSTSymbolTable symbolTable = FSSTSymbolTableBuilder.build(sampleSuffixes);
    FSSTCompressor compressor = new FSSTCompressor(symbolTable);
    byte[] tableBytes = symbolTable.toBytes();

    // Write symbol table to data
    meta.writeVInt(tableBytes.length);
    data.writeBytes(tableBytes, 0, tableBytes.length);

    // === Pass 2: Compress and write blocks using stored prefixLens ===
    meta.writeVInt(maxTermLength);
    meta.writeVInt(numBlocks);
    long[] blockStartOffsets = new long[numBlocks];
    long dataStart = data.getFilePointer();

    byte[] compBuf = new byte[65536];
    byte[] prefixArea = new byte[65536];
    byte[] suffixArea = new byte[65536];

    {
      TermsEnum iterator = values.termsEnum();
      long ord = 0;
      int blockNum = 0;
      int posInBlock = 0;
      int bufPos = 0;

      for (BytesRef term = iterator.next(); term != null; term = iterator.next(), ord++) {
        if (bufPos + term.length > blockBuf.length)
          blockBuf = java.util.Arrays.copyOf(blockBuf, Math.max(blockBuf.length * 2, bufPos + term.length));
        termOffsets[posInBlock] = bufPos;
        System.arraycopy(term.bytes, term.offset, blockBuf, bufPos, term.length);
        bufPos += term.length;
        posInBlock++;

        if (posInBlock == blockSize || ord == size - 1) {
          termOffsets[posInBlock] = bufPos;
          blockStartOffsets[blockNum] = data.getFilePointer() - dataStart;

          // Re-run DP chunker (fast — O(N^2) with N=128)
          chunker.solve(blockBuf, termOffsets, posInBlock, 0.67f);
          int numChunks = chunker.numChunks();
          int[] chunkStarts = new int[numChunks];
          int[] chunkEnds = new int[numChunks];
          int[] chunkPrefixLens = new int[numChunks];
          for (int c = 0; c < numChunks; c++) {
            chunkStarts[c] = chunker.chunkStart(c);
            chunkEnds[c] = chunker.chunkEnd(c);
            chunkPrefixLens[c] = chunker.chunkPrefixLen(c);
          }

          // Build prefix area
          int prefixAreaLen = 0;
          int[] chunkPrefixOffset = new int[numChunks];
          for (int c = 0; c < numChunks; c++) {
            int pLen = chunkPrefixLens[c];
            chunkPrefixOffset[c] = prefixAreaLen;
            if (pLen > 0) {
              if (compBuf.length < pLen * 2 + 2) compBuf = new byte[Math.max(compBuf.length * 2, pLen * 2 + 2)];
              int compLen = compressor.compress(blockBuf, termOffsets[chunkStarts[c]], pLen, compBuf);
              if (prefixAreaLen + 2 + compLen > prefixArea.length) prefixArea = java.util.Arrays.copyOf(prefixArea, Math.max(prefixArea.length * 2, prefixAreaLen + 2 + compLen));
              prefixArea[prefixAreaLen++] = (byte) (compLen & 0xFF);
              prefixArea[prefixAreaLen++] = (byte) ((compLen >> 8) & 0xFF);
              System.arraycopy(compBuf, 0, prefixArea, prefixAreaLen, compLen);
              prefixAreaLen += compLen;
            } else {
              if (prefixAreaLen + 2 > prefixArea.length) prefixArea = java.util.Arrays.copyOf(prefixArea, prefixArea.length * 2);
              prefixArea[prefixAreaLen++] = 0;
              prefixArea[prefixAreaLen++] = 0;
            }
          }

          // Build suffix area
          int suffixAreaLen = 0;
          int[] suffixOffsets = new int[posInBlock];
          int chunkIdx = 0;
          for (int t = 0; t < posInBlock; t++) {
            while (chunkIdx < numChunks - 1 && t >= chunkEnds[chunkIdx]) chunkIdx++;
            suffixOffsets[t] = suffixAreaLen;
            int pLen = chunkPrefixLens[chunkIdx];
            int termLen = termOffsets[t + 1] - termOffsets[t];
            int suffixLen = termLen - (pLen > 0 ? pLen : 0);

            // Ensure compBuf can hold compressed output (worst case = input size + overhead)
            if (compBuf.length < suffixLen * 2 + 2) compBuf = new byte[Math.max(compBuf.length * 2, suffixLen * 2 + 2)];

            if (pLen > 0) {
              if (suffixAreaLen + 4 > suffixArea.length) suffixArea = java.util.Arrays.copyOf(suffixArea, suffixArea.length * 2);
              suffixArea[suffixAreaLen++] = (byte) (pLen & 0xFF);
              suffixArea[suffixAreaLen++] = (byte) ((pLen >> 8) & 0xFF);
              int prefixOff = chunkPrefixOffset[chunkIdx];
              suffixArea[suffixAreaLen++] = (byte) (prefixOff & 0xFF);
              suffixArea[suffixAreaLen++] = (byte) ((prefixOff >> 8) & 0xFF);
            } else {
              if (suffixAreaLen + 2 > suffixArea.length) suffixArea = java.util.Arrays.copyOf(suffixArea, suffixArea.length * 2);
              suffixArea[suffixAreaLen++] = 0;
              suffixArea[suffixAreaLen++] = 0;
            }
            int compLen = compressor.compress(blockBuf, termOffsets[t] + (pLen > 0 ? pLen : 0), suffixLen, compBuf);
            if (suffixAreaLen + compLen > suffixArea.length) suffixArea = java.util.Arrays.copyOf(suffixArea, Math.max(suffixArea.length * 2, suffixAreaLen + compLen));
            System.arraycopy(compBuf, 0, suffixArea, suffixAreaLen, compLen);
            suffixAreaLen += compLen;
          }

          // Write block
          data.writeByte((byte) posInBlock);
          data.writeByte((byte) numChunks);
          for (int t = 0; t < posInBlock; t++) {
            data.writeByte((byte) (suffixOffsets[t] & 0xFF));
            data.writeByte((byte) ((suffixOffsets[t] >> 8) & 0xFF));
            data.writeByte((byte) ((suffixOffsets[t] >> 16) & 0xFF));
          }
          data.writeBytes(prefixArea, 0, prefixAreaLen);
          data.writeBytes(suffixArea, 0, suffixAreaLen);

          posInBlock = 0;
          bufPos = 0;
          blockNum++;
        }
      }
    }

    long dataEnd = data.getFilePointer();
    meta.writeLong(dataStart);
    meta.writeLong(dataEnd - dataStart);

    // Write block start offsets
    meta.writeInt(Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    DirectMonotonicWriter addrWriter =
        DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
    for (int b = 0; b < numBlocks; b++) addrWriter.add(blockStartOffsets[b]);
    addrWriter.finish();
    long addrStart = data.getFilePointer();
    addressBuffer.copyTo(data);
    meta.writeLong(addrStart);
    meta.writeLong(data.getFilePointer() - addrStart);

    // Reverse index
    meta.writeInt(Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
    Lucene90DocValuesConsumer.writeTermsIndex(meta, data, values);
  }
}
