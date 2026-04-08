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

import java.util.Arrays;

/**
 * FSST compressor using hash-based O(1) symbol lookup per position. For each position, hashes the
 * first 2 bytes to find candidate symbols of length 1 and 2+. Falls back to escape for unmatched
 * bytes.
 *
 * <p>Based on the C reference implementation's encoding approach: a 65536-entry hash table indexed
 * by the first two bytes, storing the longest matching symbol code for each hash slot.
 */
public final class FSSTCompressor {

  /** Direct lookup: single-byte symbols. code1[byte] = symbol code, or -1 if none. */
  private final int[] code1 = new int[256];

  /**
   * Hash lookup for multi-byte symbols. For each 2-byte hash, stores the code of the longest symbol
   * whose first 2 bytes hash to that slot. -1 if empty.
   */
  private final int[] code2 = new int[65536];

  /** Symbol lengths indexed by code. */
  private final int[] symLen;

  /** Symbol values as longs (first 8 bytes, little-endian packed). */
  private final long[] symVal;

  public FSSTCompressor(FSSTSymbolTable table) {
    this.symLen = new int[FSSTSymbolTable.MAX_SYMBOLS];
    this.symVal = new long[FSSTSymbolTable.MAX_SYMBOLS];

    Arrays.fill(code1, -1);
    Arrays.fill(code2, -1);

    for (int code = 0; code < FSSTSymbolTable.MAX_SYMBOLS; code++) {
      int len = table.symbolLength(code);
      symLen[code] = len;
      symVal[code] = table.decodeLong[code];
      if (len == 0) continue;

      byte[] sb = new byte[len];
      table.symbolBytes(code, sb, 0);

      if (len == 1) {
        code1[sb[0] & 0xFF] = code;
      } else {
        // Hash on first 2 bytes
        int h = hash2(sb[0], sb[1]);
        int existing = code2[h];
        // Keep the longer symbol on collision
        if (existing == -1 || len > symLen[existing]) {
          code2[h] = code;
        }
      }
    }
  }

  private static int hash2(byte b0, byte b1) {
    return ((b0 & 0xFF) << 8) | (b1 & 0xFF);
  }

  /**
   * Compresses {@code input[off..off+len)} into {@code output}.
   *
   * @return the number of compressed bytes written to output
   */
  public int compress(byte[] input, int off, int len, byte[] output) {
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      // Try multi-byte symbol first (if at least 2 bytes remain)
      if (pos + 1 < end) {
        int h = hash2(input[pos], input[pos + 1]);
        int code = code2[h];
        if (code >= 0) {
          int sLen = symLen[code];
          if (pos + sLen <= end && matchSymbol(input, pos, symVal[code], sLen)) {
            output[outPos++] = (byte) code;
            pos += sLen;
            continue;
          }
        }
      }
      // Try single-byte symbol
      int code = code1[input[pos] & 0xFF];
      if (code >= 0) {
        output[outPos++] = (byte) code;
        pos++;
      } else {
        output[outPos++] = (byte) FSSTSymbolTable.ESCAPE;
        output[outPos++] = input[pos++];
      }
    }
    return outPos;
  }

  /** Compare input bytes at pos against symbol value stored as a long. */
  private static boolean matchSymbol(byte[] input, int pos, long symValue, int len) {
    for (int i = 0; i < len; i++) {
      if (input[pos + i] != (byte) (symValue >>> (i * 8))) return false;
    }
    return true;
  }
}
