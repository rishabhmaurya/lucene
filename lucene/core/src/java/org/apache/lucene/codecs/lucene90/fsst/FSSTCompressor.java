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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * FSST compressor faithful to the C reference (cwida/fsst). Three-tier lookup:
 *
 * <ol>
 *   <li>{@code hashTab[1024]} — symbols of length 3-8, hashed on first 3 bytes
 *   <li>{@code shortCodes[65536]} — symbols of length 2, direct-indexed on first 2 bytes
 *   <li>{@code byteCodes[256]} — symbols of length 1, direct-indexed on byte value
 * </ol>
 */
public final class FSSTCompressor {

  private static final int HASH_TAB_SIZE = 1024;
  private static final long FSST_HASH_PRIME = 2971215073L;
  private static final int FSST_SHIFT = 15;
  private static final int ICL_FREE = (15 << 28) | (0xFFF << 16);

  private static final VarHandle VH_LE_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private final long[] hashTabVal;
  private final int[] hashTabIcl;
  private final int[] shortCodes;
  private final int[] byteCodes;

  public FSSTCompressor(FSSTSymbolTable table) {
    hashTabVal = new long[HASH_TAB_SIZE];
    hashTabIcl = new int[HASH_TAB_SIZE];
    shortCodes = new int[65536];
    byteCodes = new int[256];
    java.util.Arrays.fill(shortCodes, -1);
    java.util.Arrays.fill(byteCodes, -1);
    for (int i = 0; i < HASH_TAB_SIZE; i++) hashTabIcl[i] = ICL_FREE;

    for (int code = 0; code < FSSTSymbolTable.MAX_SYMBOLS; code++) {
      int len = table.symbolLength(code);
      if (len == 0) continue;
      long val = table.decodeLong[code];

      if (len == 1) {
        byteCodes[(int) (val & 0xFF)] = code;
      } else if (len == 2) {
        shortCodes[(int) (val & 0xFFFF)] = code;
      } else {
        long w3 = val & 0xFFFFFFL;
        int idx =
            (int)
                    (((w3 * FSST_HASH_PRIME) ^ ((w3 * FSST_HASH_PRIME) >>> FSST_SHIFT))
                        & 0xFFFFFFFFL)
                & (HASH_TAB_SIZE - 1);
        if (hashTabIcl[idx] == ICL_FREE || len > (hashTabIcl[idx] >>> 28)) {
          hashTabIcl[idx] = (len << 28) | (code << 16) | ((8 - len) * 8);
          hashTabVal[idx] = val;
        }
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
    final long[] htVal = this.hashTabVal;
    final int[] htIcl = this.hashTabIcl;
    final int[] sc = this.shortCodes;
    final int[] bc = this.byteCodes;
    final int safeEnd = end - 8;

    // Main loop: safe to do VH_LE_LONG (8-byte read)
    while (pos <= safeEnd) {
      long word = (long) VH_LE_LONG.get(input, pos);

      // Tier 1: hashTab (length 3-8) — inlined hash3
      long w3 = word & 0xFFFFFFL;
      int idx =
          (int) (((w3 * FSST_HASH_PRIME) ^ ((w3 * FSST_HASH_PRIME) >>> FSST_SHIFT)) & 0xFFFFFFFFL)
              & (HASH_TAB_SIZE - 1);
      int icl = htIcl[idx];
      if (icl != ICL_FREE) {
        long masked = word & (0xFFFFFFFFFFFFFFFFL >>> (icl & 0xFFFF));
        if (htVal[idx] == masked) {
          output[outPos++] = (byte) ((icl >>> 16) & 0xFFF);
          pos += icl >>> 28;
          continue;
        }
      }

      // Tier 2: shortCodes (length 2)
      int code = sc[(int) (word & 0xFFFF)];
      if (code >= 0) {
        output[outPos++] = (byte) code;
        pos += 2;
        continue;
      }

      // Tier 3: byteCodes or escape
      code = bc[(int) (word & 0xFF)];
      if (code >= 0) {
        output[outPos++] = (byte) code;
        pos++;
      } else {
        output[outPos++] = (byte) FSSTSymbolTable.ESCAPE;
        output[outPos++] = input[pos++];
      }
    }

    // Tail: last <8 bytes (slow path)
    while (pos < end) {
      long word = 0;
      for (int i = 0, n = end - pos; i < n; i++)
        word |= (long) (input[pos + i] & 0xFF) << (i * 8);

      if (pos + 2 < end) {
        long w3 = word & 0xFFFFFFL;
        int idx =
            (int)
                    (((w3 * FSST_HASH_PRIME) ^ ((w3 * FSST_HASH_PRIME) >>> FSST_SHIFT))
                        & 0xFFFFFFFFL)
                & (HASH_TAB_SIZE - 1);
        int icl = htIcl[idx];
        if (icl != ICL_FREE) {
          long masked = word & (0xFFFFFFFFFFFFFFFFL >>> (icl & 0xFFFF));
          if (htVal[idx] == masked) {
            output[outPos++] = (byte) ((icl >>> 16) & 0xFFF);
            pos += icl >>> 28;
            continue;
          }
        }
      }
      if (pos + 1 < end) {
        int code = sc[(int) (word & 0xFFFF)];
        if (code >= 0) {
          output[outPos++] = (byte) code;
          pos += 2;
          continue;
        }
      }
      int code = bc[(int) (word & 0xFF)];
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
}
