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
package org.apache.lucene.codecs.lucene90.fsst;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import org.apache.lucene.util.BytesRef;

/**
 * Builds an FSST symbol table. Direct port of the C reference implementation's {@code
 * buildSymbolTable} algorithm from <a href="https://github.com/cwida/fsst">cwida/fsst</a>.
 *
 * <p>Key algorithm features ported from C:
 *
 * <ul>
 *   <li>Progressive sampling (sampleFrac = 8, 38, 68, 98, 128)
 *   <li>count1 for both matched symbol AND single-byte alternative
 *   <li>count2 for adjacent symbol pairs (concatenation candidates)
 *   <li>8x gain boost for single-byte symbols
 *   <li>Minimum frequency threshold (5*sampleFrac/128)
 *   <li>Best-table tracking across rounds
 * </ul>
 */
public final class FSSTSymbolTableBuilder {

  private static final int MAX_SYMBOL_LEN = 8;
  private static final int CODE_BASE = 256; // first 256 are pseudo-codes for escaped bytes
  private static final int CODE_MAX = 511; // 256 pseudo + 255 real
  private static final int MAX_SAMPLE_BYTES = 1 << 20; // 1MB — enough for high-cardinality fields
  private static final long HASH_PRIME = 2971215073L;
  private static final VarHandle VH_LE_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private FSSTSymbolTableBuilder() {}

  private static final int SAMPLE_TARGET = 1 << 17; // 128KB
  private static final int SAMPLE_MAX = SAMPLE_TARGET * 2;

  public static FSSTSymbolTable build(List<BytesRef> terms) {
    if (terms.isEmpty()) {
      return FSSTSymbolTable.load(new byte[FSSTSymbolTable.SERIALIZED_SIZE]);
    }
    // Sub-sample to ~16KB total bytes (C reference: FSST_SAMPLETARGET)
    long totalBytes = 0;
    for (BytesRef t : terms) totalBytes += t.length;
    List<byte[]> lines;
    if (totalBytes <= SAMPLE_MAX) {
      lines = new ArrayList<>(terms.size());
      for (BytesRef t : terms) {
        byte[] b = new byte[t.length];
        System.arraycopy(t.bytes, t.offset, b, 0, t.length);
        lines.add(b);
      }
    } else {
      // Uniformly sub-sample to fit within SAMPLE_TARGET bytes
      lines = new ArrayList<>();
      long accumulated = 0;
      int stride = Math.max(1, (int) (totalBytes / SAMPLE_TARGET));
      int skip = 0;
      for (BytesRef t : terms) {
        if (skip++ % stride != 0) continue;
        byte[] b = new byte[t.length];
        System.arraycopy(t.bytes, t.offset, b, 0, t.length);
        lines.add(b);
        accumulated += t.length;
        if (accumulated >= SAMPLE_MAX) break;
      }
    }
    return buildSymbolTable(lines);
  }

  public static FSSTSymbolTable build(TermSupplier termIterator, long totalTerms) {
    List<BytesRef> sample = new ArrayList<>();
    long stride = Math.max(1, totalTerms / 10000);
    long ord = 0;
    try {
      BytesRef term;
      while ((term = termIterator.next()) != null) {
        if (ord % stride == 0) sample.add(BytesRef.deepCopyOf(term));
        ord++;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to sample terms for FSST training", e);
    }
    return build(sample);
  }

  @FunctionalInterface
  public interface TermSupplier {
    BytesRef next() throws Exception;
  }

  // --- Three-tier hash lookup (matches compressor behavior, C reference faithful) ---

  private static final int HASH_SHIFT = 15;
  private static final int HASH_TAB_SIZE = 1024;
  private static final int ICL_FREE = (15 << 28) | (0xFFF << 16);

  /** Rebuild three-tier lookup from current symbols. Called after each makeTable. */
  private static void rebuildLookup(byte[][] symbols, int nSymbols,
      int[] byteCodes, int[] shortCodes, long[] hashTabVal, int[] hashTabIcl) {
    Arrays.fill(byteCodes, -1);
    Arrays.fill(shortCodes, -1);
    Arrays.fill(hashTabIcl, ICL_FREE);
    for (int i = 0; i < nSymbols; i++) {
      int code = CODE_BASE + i;
      byte[] sym = symbols[code];
      int len = sym.length;
      if (len == 0) continue;
      long val = 0;
      for (int j = 0; j < len; j++) val |= (long) (sym[j] & 0xFF) << (j * 8);
      if (len == 1) {
        byteCodes[sym[0] & 0xFF] = code;
      } else if (len == 2) {
        shortCodes[(int) (val & 0xFFFF)] = code;
      } else {
        int idx = hash3(val);
        if (hashTabIcl[idx] == ICL_FREE || len > (hashTabIcl[idx] >>> 28)) {
          hashTabIcl[idx] = (len << 28) | (code << 16) | ((8 - len) * 8);
          hashTabVal[idx] = val;
        }
      }
    }
  }

  private static int hash3(long val) {
    long w = val & 0xFFFFFFL;
    long h = w * HASH_PRIME;
    return (int) ((h ^ (h >>> HASH_SHIFT)) & 0xFFFFFFFFL) & (HASH_TAB_SIZE - 1);
  }

  /** Find longest matching symbol using three-tier hash lookup (C reference faithful). */
  private static int findLongestSymbol(
      byte[][] symbols, int nSymbols, byte[] data, int pos, int end,
      int[] byteCodes, int[] shortCodes, long[] hashTabVal, int[] hashTabIcl) {
    long word;
    if (pos + 8 <= end) {
      word = (long) VH_LE_LONG.get(data, pos);
    } else {
      word = 0;
      for (int i = 0, n = end - pos; i < n; i++) word |= (long) (data[pos + i] & 0xFF) << (i * 8);
    }

    if (pos + 2 < end) {
      int idx = hash3(word);
      int icl = hashTabIcl[idx];
      if (icl != ICL_FREE) {
        long masked = word & (0xFFFFFFFFFFFFFFFFL >>> (icl & 0xFFFF));
        if (hashTabVal[idx] == masked) return (icl >>> 16) & 0xFFF;
      }
    }
    if (pos + 1 < end) {
      int code = shortCodes[(int) (word & 0xFFFF)];
      if (code >= 0) return code;
    }
    int code = byteCodes[(int) (word & 0xFF)];
    return code >= 0 ? code : (data[pos] & 0xFF);
  }

  private static int symbolLength(byte[][] symbols, int code) {
    return symbols[code].length;
  }

  private static long fsstHash(long w) {
    return ((w * HASH_PRIME) ^ ((w * HASH_PRIME) >>> HASH_SHIFT));
  }

  private static FSSTSymbolTable buildSymbolTable(List<byte[]> lines) {
    // symbols[0..255] = pseudo-symbols (single escaped bytes)
    // symbols[256..510] = real symbols
    byte[][] symbols = new byte[CODE_MAX + 1][];
    for (int i = 0; i < 256; i++) symbols[i] = new byte[] {(byte) i};
    for (int i = 256; i <= CODE_MAX; i++) symbols[i] = new byte[0];
    int nSymbols = 0;

    // Thread-local lookup tables (not shared across threads)
    int[] byteCodes = new int[256];
    int[] shortCodes = new int[65536];
    long[] hashTabVal = new long[HASH_TAB_SIZE];
    int[] hashTabIcl = new int[HASH_TAB_SIZE];

    int[] count1 = new int[CODE_MAX + 1];
    int[][] count2 = null; // lazily allocated — large

    int bestGain = -MAX_SAMPLE_BYTES;
    byte[][] bestSymbols = null;
    int bestNSymbols = 0;
    int[] bestCount1 = null;

    for (int sampleFrac = 8; ; sampleFrac += 30) {
      boolean lastRound = sampleFrac >= 128;
      if (lastRound) sampleFrac = 128;

      // Rebuild lookup before compressCount (first round has empty table)
      rebuildLookup(symbols, nSymbols, byteCodes, shortCodes, hashTabVal, hashTabIcl);

      Arrays.fill(count1, 0);
      if (!lastRound) {
        count2 = new int[CODE_MAX + 1][]; // sparse allocation
      }

      // --- compressCount ---
      int gain = 0;
      for (int li = 0; li < lines.size(); li++) {
        if (sampleFrac < 128) {
          long rnd = 1 + (fsstHash(((long) li + 1) * sampleFrac) & 127);
          if (rnd > sampleFrac) continue;
        }
        byte[] line = lines.get(li);
        int cur = 0, end = line.length;
        if (cur >= end) continue;

        int code1 = findLongestSymbol(symbols, nSymbols, line, cur, end, byteCodes, shortCodes, hashTabVal, hashTabIcl);
        int len1 = symbolLength(symbols, code1);
        cur += len1;
        gain += len1 - (code1 < CODE_BASE ? 2 : 1); // escape costs 2, symbol costs 1

        while (true) {
          // count this symbol
          count1[code1]++;
          // count single-byte alternative (don't double-count single-byte symbols)
          if (symbolLength(symbols, code1) != 1) {
            count1[line[cur - len1] & 0xFF]++;
          }

          if (cur >= end) break;

          int start = cur;
          int code2 = findLongestSymbol(symbols, nSymbols, line, cur, end, byteCodes, shortCodes, hashTabVal, hashTabIcl);
          int len2 = symbolLength(symbols, code2);
          cur += len2;
          gain += len2 - (code2 < CODE_BASE ? 2 : 1);

          if (!lastRound) {
            // count pair (code1, code2)
            count2Inc(count2, code1, code2);
            // count pair (code1, first-byte-alternative)
            if (len2 > 1) {
              count2Inc(count2, code1, line[start] & 0xFF);
            }
          }

          len1 = len2;
          code1 = code2;
        }
      }

      if (gain >= bestGain) {
        bestGain = gain;
        bestCount1 = count1.clone();
        bestSymbols = symbols.clone();
        bestNSymbols = nSymbols;
      }

      if (lastRound) break;

      // --- makeTable ---
      nSymbols = makeTable(symbols, nSymbols, count1, count2, sampleFrac);
    }

    // Final makeTable with best counters (C reference: re-ranks with 8x boost)
    System.arraycopy(bestSymbols, CODE_BASE, symbols, CODE_BASE, bestNSymbols);
    nSymbols = makeTable(symbols, bestNSymbols, bestCount1, null, 128);

    return exportTable(symbols, nSymbols);
  }

  private static void count2Inc(int[][] count2, int pos1, int pos2) {
    if (count2[pos1] == null) count2[pos1] = new int[CODE_MAX + 1];
    count2[pos1][pos2]++;
  }

  private static int makeTable(
      byte[][] symbols, int nSymbols, int[] count1, int[][] count2, int sampleFrac) {
    boolean lastRound = sampleFrac >= 128;
    long minCount = (5L * sampleFrac) / 128;

    // Open-addressing hash map: key = packed long (symbol bytes), value = gain + length
    // Encode key as: symbol bytes in low bits (LE), length in bits 60-62
    // Use separate arrays for speed
    final int MAP_SIZE = 1 << 15; // 32768 — sufficient for ~10K candidates at <50% load
    final int MASK = MAP_SIZE - 1;
    long[] mapKey = new long[MAP_SIZE];   // 0 = empty
    long[] mapGain = new long[MAP_SIZE];
    int[] mapLen = new int[MAP_SIZE];

    for (int pos1 = 0; pos1 <= CODE_MAX; pos1++) {
      int cnt1 = count1[pos1];
      if (cnt1 == 0) continue;
      if (pos1 >= CODE_BASE + nSymbols && pos1 >= CODE_BASE) continue;

      byte[] s1 = symbols[pos1];
      if (s1.length == 0) continue;

      long boostedCount = (s1.length == 1 ? 8L : 1L) * cnt1;
      if (boostedCount < minCount) continue;
      long gain = boostedCount * s1.length;
      long key1 = packSymbol(s1, 0, s1.length);
      addCandidate(mapKey, mapGain, mapLen, MASK, key1, s1.length, gain);

      if (lastRound || s1.length >= MAX_SYMBOL_LEN) continue;

      if (count2 != null && count2[pos1] != null) {
        long s1packed = key1;
        for (int pos2 = 0; pos2 <= CODE_MAX; pos2++) {
          int cnt2 = count2[pos1][pos2];
          if (cnt2 == 0) continue;
          if (pos2 >= CODE_BASE + nSymbols && pos2 >= CODE_BASE) continue;

          byte[] s2 = symbols[pos2];
          if (s2.length == 0) continue;
          int concatLen = s1.length + s2.length;
          if (concatLen > MAX_SYMBOL_LEN) continue;
          if (cnt2 < minCount) continue;

          // Pack concat: s1 bytes | s2 bytes shifted
          long key2 = s1packed | (packSymbol(s2, 0, s2.length) << (s1.length * 8));
          long concatGain = (long) cnt2 * concatLen;
          addCandidate(mapKey, mapGain, mapLen, MASK, key2, concatLen, concatGain);
        }
      }
    }

    // Collect all candidates, partial sort for top 255
    int candCount = 0;
    for (int i = 0; i < MAP_SIZE; i++) {
      if (mapLen[i] > 0) candCount++;
    }

    // Extract to arrays for sorting
    long[] gains = new long[candCount];
    long[] keys = new long[candCount];
    int[] lens = new int[candCount];
    int ci = 0;
    for (int i = 0; i < MAP_SIZE; i++) {
      if (mapLen[i] > 0) {
        gains[ci] = mapGain[i];
        keys[ci] = mapKey[i];
        lens[ci] = mapLen[i];
        ci++;
      }
    }

    // Partial sort: find top 255 by gain (selection via PQ of size 255)
    for (int i = CODE_BASE; i <= CODE_MAX; i++) symbols[i] = new byte[0];
    int newNSymbols = 0;
    if (candCount <= 255) {
      for (int i = 0; i < candCount; i++) {
        symbols[CODE_BASE + newNSymbols++] = unpackSymbol(keys[i], lens[i]);
      }
    } else {
      // Find 255th largest gain via partial sort
      Integer[] indices = new Integer[candCount];
      for (int i = 0; i < candCount; i++) indices[i] = i;
      Arrays.sort(indices, (a, b) -> Long.compare(gains[b], gains[a]));
      for (int i = 0; i < 255; i++) {
        int idx = indices[i];
        symbols[CODE_BASE + newNSymbols++] = unpackSymbol(keys[idx], lens[idx]);
      }
    }
    return newNSymbols;
  }

  private static long packSymbol(byte[] sym, int off, int len) {
    long v = 0;
    for (int i = 0; i < len; i++) v |= (long) (sym[off + i] & 0xFF) << (i * 8);
    return v;
  }

  private static byte[] unpackSymbol(long packed, int len) {
    byte[] b = new byte[len];
    for (int i = 0; i < len; i++) b[i] = (byte) (packed >>> (i * 8));
    return b;
  }

  private static void addCandidate(long[] mapKey, long[] mapGain, int[] mapLen, int mask,
      long key, int len, long gain) {
    // Encode length into high bits of key for uniqueness (same bytes, different length = different)
    long fullKey = key | ((long) len << 56);
    int h = Long.hashCode(fullKey) & mask;
    while (true) {
      if (mapLen[h] == 0) {
        // Empty slot
        mapKey[h] = key;
        mapGain[h] = gain;
        mapLen[h] = len;
        return;
      }
      if (mapKey[h] == key && mapLen[h] == len) {
        // Same symbol — merge gain
        mapGain[h] += gain;
        return;
      }
      h = (h + 1) & mask;
    }
  }

  private static FSSTSymbolTable exportTable(byte[][] symbols, int nSymbols) {
    byte[] tableData = new byte[FSSTSymbolTable.SERIALIZED_SIZE];
    for (int i = 0; i < nSymbols; i++) {
      byte[] sym = symbols[CODE_BASE + i];
      tableData[i] = (byte) sym.length;
      System.arraycopy(sym, 0, tableData, FSSTSymbolTable.MAX_SYMBOLS + i * 8, sym.length);
    }
    return FSSTSymbolTable.load(tableData);
  }

}
