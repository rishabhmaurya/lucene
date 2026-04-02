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

/**
 * FSST compressor using greedy longest-match encoding. For each position in the input, finds the
 * longest symbol that matches and emits its code. Unmatched bytes are escaped with 0xFF prefix.
 *
 * <p>Uses per-first-byte lists for symbol lookup — symbols are ordered by gain (not by first byte),
 * so contiguous range assumptions don't hold.
 */
public final class FSSTCompressor {

  private final FSSTSymbolTable table;

  /**
   * For each possible first byte (0-255), the list of symbol codes whose first byte matches. Used
   * for greedy longest-match lookup.
   */
  private final int[][] codesByFirstByte;

  /** Symbol lengths indexed by code, for fast access during compression. */
  private final int[] symLen;

  /** Symbol bytes indexed by code, for matching during compression. */
  private final byte[][] symBytes;

  public FSSTCompressor(FSSTSymbolTable table) {
    this.table = table;
    this.symLen = new int[FSSTSymbolTable.MAX_SYMBOLS];
    this.symBytes = new byte[FSSTSymbolTable.MAX_SYMBOLS][];

    // Build per-first-byte lists
    int[] counts = new int[256];
    for (int code = 0; code < FSSTSymbolTable.MAX_SYMBOLS; code++) {
      symLen[code] = table.symbolLength(code);
      if (symLen[code] > 0) {
        symBytes[code] = new byte[symLen[code]];
        table.symbolBytes(code, symBytes[code], 0);
        counts[symBytes[code][0] & 0xFF]++;
      }
    }

    codesByFirstByte = new int[256][];
    int[] offsets = new int[256];
    for (int b = 0; b < 256; b++) {
      codesByFirstByte[b] = new int[counts[b]];
    }
    for (int code = 0; code < FSSTSymbolTable.MAX_SYMBOLS; code++) {
      if (symLen[code] > 0) {
        int fb = symBytes[code][0] & 0xFF;
        codesByFirstByte[fb][offsets[fb]++] = code;
      }
    }
  }

  /**
   * Compresses {@code input[off..off+len)} into {@code output}.
   *
   * @return the number of compressed bytes written to output
   */
  public int compress(byte[] input, int off, int len, byte[] output) {
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      int bestCode = -1, bestLen = 0;
      int fb = input[pos] & 0xFF;
      for (int code : codesByFirstByte[fb]) {
        int sLen = symLen[code];
        if (sLen > bestLen && pos + sLen <= end) {
          if (matches(input, pos, symBytes[code], sLen)) {
            bestCode = code;
            bestLen = sLen;
          }
        }
      }
      if (bestCode >= 0) {
        output[outPos++] = (byte) bestCode;
        pos += bestLen;
      } else {
        output[outPos++] = (byte) FSSTSymbolTable.ESCAPE;
        output[outPos++] = input[pos++];
      }
    }
    return outPos;
  }

  private static boolean matches(byte[] input, int inputOff, byte[] symbol, int len) {
    for (int i = 0; i < len; i++) {
      if (input[inputOff + i] != symbol[i]) return false;
    }
    return true;
  }
}
