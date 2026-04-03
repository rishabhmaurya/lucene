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
import org.apache.lucene.util.BytesRef;

/**
 * Builds an FSST symbol table from a sample of terms using the generational algorithm described in
 * the FSST paper. Iteratively refines the symbol table over multiple generations by:
 *
 * <ol>
 *   <li>Compressing the sample with the current table
 *   <li>Counting symbol frequencies and adjacent symbol pairs
 *   <li>Selecting the top-255 symbols by effective gain (length × frequency)
 *   <li>Concatenating adjacent symbols as candidates for the next generation
 * </ol>
 *
 * <p>The training sample should be representative of the data. The paper recommends ~16KB of data
 * sampled as 512-byte chunks uniformly across the input.
 *
 * @see <a href="https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf">FSST paper, Section 4</a>
 */
public final class FSSTSymbolTableBuilder {

  /** Maximum symbol length in bytes (fits in a 64-bit register). */
  private static final int MAX_SYMBOL_LEN = 8;

  /** Number of training generations. The paper uses 5. */
  private static final int NUM_GENERATIONS = 5;

  /** Maximum sample size in bytes for training. */
  private static final int MAX_SAMPLE_BYTES = 1 << 16; // 64KB

  private FSSTSymbolTableBuilder() {}

  /**
   * Build a symbol table from a list of terms.
   *
   * @param terms sample terms to train on (will be sampled if too large)
   * @return a trained symbol table
   */
  public static FSSTSymbolTable build(List<BytesRef> terms) {
    // Collect sample bytes (up to MAX_SAMPLE_BYTES)
    byte[] sample = collectSample(terms);
    if (sample.length == 0) {
      return FSSTSymbolTable.load(new byte[FSSTSymbolTable.SERIALIZED_SIZE]);
    }
    return trainOnSample(sample);
  }

  /**
   * Build a symbol table by sampling terms from a SortedSetDocValues-like iterator.
   *
   * @param termIterator provides terms in order; called repeatedly until null
   * @param totalTerms total number of terms (for sampling stride)
   * @return a trained symbol table
   */
  public static FSSTSymbolTable build(TermSupplier termIterator, long totalTerms) {
    List<BytesRef> sample = new ArrayList<>();
    long stride = Math.max(1, totalTerms / 10000); // sample ~10K terms
    long ord = 0;
    try {
      BytesRef term;
      while ((term = termIterator.next()) != null) {
        if (ord % stride == 0) {
          sample.add(BytesRef.deepCopyOf(term));
        }
        ord++;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to sample terms for FSST training", e);
    }
    return build(sample);
  }

  /** Functional interface for iterating terms. */
  @FunctionalInterface
  public interface TermSupplier {
    BytesRef next() throws Exception;
  }

  private static byte[] collectSample(List<BytesRef> terms) {
    int totalBytes = 0;
    for (BytesRef t : terms) totalBytes += t.length;

    if (totalBytes <= MAX_SAMPLE_BYTES) {
      // Use all terms
      byte[] sample = new byte[totalBytes];
      int pos = 0;
      for (BytesRef t : terms) {
        System.arraycopy(t.bytes, t.offset, sample, pos, t.length);
        pos += t.length;
      }
      return sample;
    }

    // Sample uniformly
    byte[] sample = new byte[MAX_SAMPLE_BYTES];
    int pos = 0;
    int stride = Math.max(1, terms.size() / (MAX_SAMPLE_BYTES / 64)); // ~64 bytes per term avg
    for (int i = 0; i < terms.size() && pos < MAX_SAMPLE_BYTES; i += stride) {
      BytesRef t = terms.get(i);
      int len = Math.min(t.length, MAX_SAMPLE_BYTES - pos);
      System.arraycopy(t.bytes, t.offset, sample, pos, len);
      pos += len;
    }
    return Arrays.copyOf(sample, pos);
  }

  private static FSSTSymbolTable trainOnSample(byte[] sample) {
    // Start with empty symbol table (all bytes will be escaped)
    List<Symbol> currentSymbols = new ArrayList<>();

    for (int gen = 0; gen < NUM_GENERATIONS; gen++) {
      // Compress sample with current symbols and count frequencies
      Map<Symbol, Long> symbolFreq = new HashMap<>();
      Map<Long, Long> pairFreq = new HashMap<>(); // packed pair of (code1, code2)

      // Initialize single-byte symbols if first generation
      if (gen == 0) {
        int[] byteFreq = new int[256];
        for (byte b : sample) byteFreq[b & 0xFF]++;
        for (int b = 0; b < 256; b++) {
          if (byteFreq[b] > 0) {
            currentSymbols.add(new Symbol(new byte[] {(byte) b}));
          }
        }
      }

      // Build a quick lookup for greedy compression
      FSSTSymbolTable tempTable = buildTable(currentSymbols);
      FSSTCompressor tempComp = new FSSTCompressor(tempTable);

      // Compress and count
      byte[] compressed = new byte[sample.length * 2];
      int compLen = tempComp.compress(sample, 0, sample.length, compressed);

      // Count codes, single-byte alternatives, and escaped bytes
      int[] codeFreq = new int[256];
      int[] singleByteFreq = new int[256]; // frequency of first byte as alternative
      int[] escapedFreq = new int[256]; // frequency of escaped literal bytes
      for (int i = 0; i < compLen; i++) {
        int code = compressed[i] & 0xFF;
        if (code == FSSTSymbolTable.ESCAPE) {
          i++; // skip literal byte
          if (i < compLen) escapedFreq[compressed[i] & 0xFF]++;
        } else {
          codeFreq[code]++;
          // Also count the single-byte alternative (like C reference does)
          if (code < currentSymbols.size() && currentSymbols.get(code).bytes.length > 1) {
            singleByteFreq[currentSymbols.get(code).bytes[0] & 0xFF]++;
          }
        }
      }

      // Score candidates: existing symbols use codeFreq from compression,
      // new concatenations need frequency counting
      Map<Symbol, Long> candidateGain = new HashMap<>();

      // Existing symbols: gain = codeFreq * length
      for (int code = 0; code < 255; code++) {
        if (codeFreq[code] > 0 && code < currentSymbols.size()) {
          Symbol s = currentSymbols.get(code);
          candidateGain.put(s, (long) codeFreq[code] * s.bytes.length);
        }
      }

      // Single-byte alternatives: ensure common bytes always have a chance
      for (int b = 0; b < 256; b++) {
        long freq = singleByteFreq[b] + escapedFreq[b];
        if (freq > 0) {
          Symbol s = new Symbol(new byte[]{(byte) b});
          candidateGain.merge(s, freq, Long::max);
        }
      }

      // New concatenation candidates: compress sample and count how often each concat appears
      // Use a hash map for O(1) lookup instead of O(n) scanning
      Map<Symbol, Long> concatFreq = new HashMap<>();
      for (int i = 0; i < compLen - 1; i++) {
        int code1 = compressed[i] & 0xFF;
        if (code1 == FSSTSymbolTable.ESCAPE) { i++; continue; }
        int j = i + 1;
        if (j >= compLen) break;
        int code2 = compressed[j] & 0xFF;
        if (code2 == FSSTSymbolTable.ESCAPE) continue;
        int len1 = tempTable.symbolLength(code1);
        int len2 = tempTable.symbolLength(code2);
        if (len1 + len2 <= MAX_SYMBOL_LEN) {
          byte[] concat = new byte[len1 + len2];
          tempTable.symbolBytes(code1, concat, 0);
          tempTable.symbolBytes(code2, concat, len1);
          Symbol s = new Symbol(concat);
          concatFreq.merge(s, 1L, Long::sum);
        }
      }
      for (var e : concatFreq.entrySet()) {
        long gain = e.getValue() * e.getKey().bytes.length;
        candidateGain.merge(e.getKey(), gain, Long::max);
      }

      // Select top 255 by gain
      currentSymbols =
          candidateGain.entrySet().stream()
              .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
              .limit(FSSTSymbolTable.MAX_SYMBOLS)
              .map(Map.Entry::getKey)
              .toList()
              .stream()
              .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    return buildTable(currentSymbols);
  }

  private static FSSTSymbolTable buildTable(List<Symbol> symbols) {
    byte[] tableData = new byte[FSSTSymbolTable.SERIALIZED_SIZE];
    int code = 0;
    for (Symbol s : symbols) {
      if (code >= FSSTSymbolTable.MAX_SYMBOLS) break;
      tableData[code] = (byte) s.bytes.length;
      System.arraycopy(s.bytes, 0, tableData, FSSTSymbolTable.MAX_SYMBOLS + code * 8, s.bytes.length);
      code++;
    }
    return FSSTSymbolTable.load(tableData);
  }

  private static class Symbol {
    final byte[] bytes;

    Symbol(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Symbol s && Arrays.equals(bytes, s.bytes);
    }
  }
}
