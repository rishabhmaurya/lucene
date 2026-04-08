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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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

  private FSSTSymbolTableBuilder() {}

  public static FSSTSymbolTable build(List<BytesRef> terms) {
    if (terms.isEmpty()) {
      return FSSTSymbolTable.load(new byte[FSSTSymbolTable.SERIALIZED_SIZE]);
    }
    // Convert all terms to byte arrays — sampling is done by the caller
    List<byte[]> lines = new ArrayList<>(terms.size());
    for (BytesRef t : terms) {
      byte[] b = new byte[t.length];
      System.arraycopy(t.bytes, t.offset, b, 0, t.length);
      lines.add(b);
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

  // --- Symbol representation: byte[] of length 1-8 ---

  /** Find longest matching symbol in the table. Returns code (0-255 for pseudo, 256+ for real). */
  private static int findLongestSymbol(
      byte[][] symbols, int nSymbols, byte[] data, int pos, int end) {
    int bestCode = data[pos] & 0xFF; // default: pseudo-code for the byte itself
    int bestLen = 1;
    for (int i = 0; i < nSymbols; i++) {
      byte[] sym = symbols[CODE_BASE + i];
      if (sym.length > bestLen && pos + sym.length <= end) {
        boolean match = true;
        for (int j = 0; j < sym.length; j++) {
          if (data[pos + j] != sym[j]) {
            match = false;
            break;
          }
        }
        if (match) {
          bestCode = CODE_BASE + i;
          bestLen = sym.length;
        }
      }
    }
    return bestCode;
  }

  private static int symbolLength(byte[][] symbols, int code) {
    return symbols[code].length;
  }

  private static long fsstHash(long w) {
    return ((w * HASH_PRIME) ^ ((w * HASH_PRIME) >>> 13));
  }

  private static FSSTSymbolTable buildSymbolTable(List<byte[]> lines) {
    // symbols[0..255] = pseudo-symbols (single escaped bytes)
    // symbols[256..510] = real symbols
    byte[][] symbols = new byte[CODE_MAX + 1][];
    for (int i = 0; i < 256; i++) symbols[i] = new byte[] {(byte) i};
    for (int i = 256; i <= CODE_MAX; i++) symbols[i] = new byte[0];
    int nSymbols = 0;

    int[] count1 = new int[CODE_MAX + 1];
    int[][] count2 = null; // lazily allocated — large

    int bestGain = -MAX_SAMPLE_BYTES;
    byte[][] bestSymbols = null;
    int bestNSymbols = 0;
    int[] bestCount1 = null;

    for (int sampleFrac = 8; ; sampleFrac += 30) {
      boolean lastRound = sampleFrac >= 128;
      if (lastRound) sampleFrac = 128;

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

        int code1 = findLongestSymbol(symbols, nSymbols, line, cur, end);
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
          int code2 = findLongestSymbol(symbols, nSymbols, line, cur, end);
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

    // Final makeTable with best counters
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

    // Candidates with gain
    Map<SymbolKey, Long> cands = new HashMap<>();

    // Add single and existing symbols
    for (int pos1 = 0; pos1 <= CODE_MAX; pos1++) {
      int cnt1 = count1[pos1];
      if (cnt1 == 0) continue;
      if (pos1 >= CODE_BASE + nSymbols && pos1 >= CODE_BASE) continue; // unused real symbol slot

      byte[] s1 = symbols[pos1];
      if (s1.length == 0) continue;

      // 8x boost for single-byte symbols
      long boostedCount = (s1.length == 1 ? 8L : 1L) * cnt1;
      if (boostedCount < minCount) continue;
      long gain = boostedCount * s1.length;
      SymbolKey key = new SymbolKey(s1);
      cands.merge(key, gain, Long::sum);

      if (lastRound || s1.length >= MAX_SYMBOL_LEN) continue;

      // Add concatenation candidates from count2
      if (count2 != null && count2[pos1] != null) {
        for (int pos2 = 0; pos2 <= CODE_MAX; pos2++) {
          int cnt2 = count2[pos1][pos2];
          if (cnt2 == 0) continue;
          if (pos2 >= CODE_BASE + nSymbols && pos2 >= CODE_BASE) continue;

          byte[] s2 = symbols[pos2];
          if (s2.length == 0 || s1.length + s2.length > MAX_SYMBOL_LEN) continue;

          if (cnt2 < minCount) continue;
          byte[] concat = new byte[s1.length + s2.length];
          System.arraycopy(s1, 0, concat, 0, s1.length);
          System.arraycopy(s2, 0, concat, s1.length, s2.length);
          long concatGain = (long) cnt2 * concat.length;
          cands.merge(new SymbolKey(concat), concatGain, Long::sum);
        }
      }
    }

    // Select top 255 by gain
    PriorityQueue<Map.Entry<SymbolKey, Long>> pq =
        new PriorityQueue<>((a, b) -> Long.compare(b.getValue(), a.getValue()));
    pq.addAll(cands.entrySet());

    // Clear real symbols
    for (int i = CODE_BASE; i <= CODE_MAX; i++) symbols[i] = new byte[0];
    int newNSymbols = 0;
    while (newNSymbols < 255 && !pq.isEmpty()) {
      Map.Entry<SymbolKey, Long> entry = pq.poll();
      symbols[CODE_BASE + newNSymbols] = entry.getKey().bytes;
      newNSymbols++;
    }
    return newNSymbols;
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

  private static class SymbolKey {
    final byte[] bytes;
    private final int hash;

    SymbolKey(byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof SymbolKey s && Arrays.equals(bytes, s.bytes);
    }
  }
}
