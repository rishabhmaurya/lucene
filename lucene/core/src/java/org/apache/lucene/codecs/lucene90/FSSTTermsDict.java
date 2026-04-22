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
import org.apache.lucene.codecs.lucene90.fsst.FSSTSymbolTable;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * FSST term dictionary reader. Per-term FSST compression with O(1) random access.
 * Uses a buffered refill strategy (16 terms at a time) and inlined decompression.
 */
final class FSSTTermsDict extends BaseTermsEnum {
  final long termsDictSize;
  final LongValues dmOffsets; // lazy offset access via DirectMonotonicReader
  final IndexInput bytes;
  final FSSTDecompressor decompressor;
  final BytesRef term;
  final BytesRef compressedTerm;
  long ord = -1;

  private static final int BUF_SIZE = 128;
  private static final int BUF_MAX_TERMS = 16;
  private final byte[] buf;
  private final IndexInput bufInput;
  private final long dataEnd;
  private final int[] symLen; // symbol lengths as int[] (avoids & 0xFF in hot loop)
  private final long[] symVal; // pre-decoded symbol values for VH_LE_LONG writes
  // Unified buffer: holds compressed bytes + offsets for up to BUF_MAX_TERMS terms
  private int bufOrdStart = -1; // first ord in buffer
  private int bufOrdCount = 0;  // number of terms in buffer
  private final int[] bufOffsets = new int[BUF_MAX_TERMS + 1]; // relative offsets within buf

  FSSTTermsDict(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data) throws IOException {
    this.termsDictSize = entry.termsDictSize;
    IndexInput dataSlice =
        data.slice(
            "fsst-terms",
            entry.termsDataOffset - entry.symbolTableLength,
            entry.termsDataLength + entry.symbolTableLength);
    byte[] tableBytes = new byte[entry.symbolTableLength];
    dataSlice.readBytes(tableBytes, 0, tableBytes.length);
    FSSTSymbolTable symbolTable = FSSTSymbolTable.load(tableBytes);
    this.decompressor = new FSSTDecompressor(symbolTable);
    bytes = data.slice("fsst-terms-data", entry.termsDataOffset, entry.termsDataLength);
    bufInput = bytes.clone();
    RandomAccessInput addrSlice =
        data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
    dmOffsets =
        DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addrSlice, false);
    dataEnd = dmOffsets.get(termsDictSize);
    term = new BytesRef(entry.maxTermLength + 7); // +7 for VH_LE_LONG overwrite slack
    compressedTerm = new BytesRef(entry.maxTermLength * 2);
    symLen = new int[symbolTable.len.length];
    for (int i = 0; i < symbolTable.len.length; i++) symLen[i] = symbolTable.len[i] & 0xFF;
    symVal = symbolTable.decodeLong;
    buf = new byte[Math.max(BUF_SIZE, entry.maxTermLength * 2)];
  }

  /** Buffered lookupOrd — loads offsets and compressed bytes together on cache miss. */
  BytesRef lookupOrd(int ord) throws IOException {
    int localOrd = ord - bufOrdStart;
    if (localOrd < 0 || localOrd >= bufOrdCount) {
      // Refill: load offsets and compressed bytes for consecutive terms
      long start = dmOffsets.get(ord);
      bufOrdStart = ord;
      bufOffsets[0] = 0;
      int count = 0;
      for (int i = 1; i <= BUF_MAX_TERMS && ord + i <= termsDictSize; i++) {
        int rel = (int) (dmOffsets.get(ord + i) - start);
        if (rel > BUF_SIZE) break;
        bufOffsets[i] = rel;
        count = i;
      }
      if (count == 0) {
        // Single term larger than BUF_SIZE
        int compLen = (int) (dmOffsets.get(ord + 1) - start);
        bufInput.seek(start);
        bufInput.readBytes(buf, 0, compLen);
        bufOffsets[1] = compLen;
        count = 1;
      } else {
        bufInput.seek(start);
        bufInput.readBytes(buf, 0, bufOffsets[count]);
      }
      bufOrdCount = count;
      localOrd = 0;
    }
    final int off = bufOffsets[localOrd];
    final int end = bufOffsets[localOrd + 1];
    final byte[] in = buf;
    final byte[] out = term.bytes;
    final int[] sl = symLen;
    final long[] sv = symVal;
    int pos = off, outPos = 0;
    while (pos < end) {
      int code = in[pos++] & 0xFF;
      if (code != 0xFF) {
        BitUtil.VH_LE_LONG.set(out, outPos, sv[code]);
        outPos += sl[code];
      } else {
        out[outPos++] = in[pos++];
      }
    }
    term.length = outPos;
    return term;
  }




  @Override
  public BytesRef next() throws IOException {
    if (++ord >= termsDictSize) return null;
    return lookupOrd((int) ord);
  }

  @Override
  public void seekExact(long ord) throws IOException {
    this.ord = ord;
    lookupOrd((int) ord);
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    long lo = 0, hi = termsDictSize - 1;
    while (lo <= hi) {
      long mid = (lo + hi) >>> 1;
      lookupOrd((int) mid);
      int cmp = term.compareTo(text);
      if (cmp < 0) lo = mid + 1;
      else if (cmp > 0) hi = mid - 1;
      else {
        ord = mid;
        return SeekStatus.FOUND;
      }
    }
    if (lo >= termsDictSize) {
      ord = termsDictSize;
      return SeekStatus.END;
    }
    ord = lo;
    lookupOrd((int) ord);
    return SeekStatus.NOT_FOUND;
  }

  @Override
  public BytesRef term() {
    return term;
  }

  @Override
  public long ord() {
    return ord;
  }

  @Override
  public long totalTermFreq() {
    return -1L;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ImpactsEnum impacts(int flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int docFreq() {
    throw new UnsupportedOperationException();
  }
  }

