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

import org.apache.lucene.util.BitUtil;

/**
 * Stateless FSST decompressor. Mirrors the C reference implementation's optimized scalar loop:
 * processes 4 codes at a time when no escapes are present, uses unaligned 8-byte writes via
 * little-endian long stores.
 */
public final class FSSTDecompressor {

  private final FSSTSymbolTable table;

  public FSSTDecompressor(FSSTSymbolTable table) {
    this.table = table;
  }

  /**
   * Decompresses {@code compressed[off..off+len)} into {@code output}. Caller must ensure output
   * has at least 7 bytes of slack beyond the actual decompressed size.
   *
   * @return the number of decompressed bytes written to output
   */
  public int decompress(byte[] compressed, int off, int len, byte[] output) {
    final byte[] symbolLen = table.len;
    final long[] symbol = table.decodeLong;
    int pos = off;
    int end = off + len;
    int outPos = 0;
    int outSafe = output.length - 8; // safe zone for unconditional 8-byte writes

    // Fast path: process 4 codes at a time when no escapes present
    while (pos + 4 <= end && outPos <= outSafe - 24) {
      // Read 4 input bytes
      int b0 = compressed[pos] & 0xFF;
      int b1 = compressed[pos + 1] & 0xFF;
      int b2 = compressed[pos + 2] & 0xFF;
      int b3 = compressed[pos + 3] & 0xFF;

      // Check if any of the 4 bytes is an escape (0xFF)
      if ((b0 & b1 & b2 & b3) != 0xFF && b0 != 0xFF && b1 != 0xFF && b2 != 0xFF && b3 != 0xFF) {
        // No escapes — process all 4 unconditionally
        writeLongLE(output, outPos, symbol[b0]);
        outPos += symbolLen[b0] & 0xFF;
        writeLongLE(output, outPos, symbol[b1]);
        outPos += symbolLen[b1] & 0xFF;
        writeLongLE(output, outPos, symbol[b2]);
        outPos += symbolLen[b2] & 0xFF;
        writeLongLE(output, outPos, symbol[b3]);
        outPos += symbolLen[b3] & 0xFF;
        pos += 4;
      } else {
        // At least one escape — find first escape and process codes before it
        if (b0 == 0xFF) {
          output[outPos++] = compressed[pos + 1];
          pos += 2;
        } else {
          writeLongLE(output, outPos, symbol[b0]);
          outPos += symbolLen[b0] & 0xFF;
          pos++;
          if (b1 == 0xFF) {
            output[outPos++] = compressed[pos + 1];
            pos += 2;
          } else {
            writeLongLE(output, outPos, symbol[b1]);
            outPos += symbolLen[b1] & 0xFF;
            pos++;
            if (b2 == 0xFF) {
              output[outPos++] = compressed[pos + 1];
              pos += 2;
            } else {
              writeLongLE(output, outPos, symbol[b2]);
              outPos += symbolLen[b2] & 0xFF;
              pos++;
              // b3 must be the escape
              output[outPos++] = compressed[pos + 1];
              pos += 2;
            }
          }
        }
      }
    }

    // Tail: process remaining codes one at a time
    while (pos < end) {
      int code = compressed[pos++] & 0xFF;
      if (code != 0xFF) {
        if (outPos <= outSafe) {
          writeLongLE(output, outPos, symbol[code]);
        } else {
          int l = symbolLen[code] & 0xFF;
          long v = symbol[code];
          for (int i = 0; i < l; i++) {
            output[outPos + i] = (byte) (v >>> (i * 8));
          }
        }
        outPos += symbolLen[code] & 0xFF;
      } else {
        output[outPos++] = compressed[pos++];
      }
    }
    return outPos;
  }

  private static void writeLongLE(byte[] b, int off, long v) {
    BitUtil.VH_LE_LONG.set(b, off, v);
  }
}
