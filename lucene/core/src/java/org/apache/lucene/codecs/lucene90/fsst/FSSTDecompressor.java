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
 * Stateless FSST decompressor. For each input byte: if not ESCAPE (0xFF), copy the corresponding
 * symbol bytes from the table; otherwise copy the next raw byte verbatim.
 */
public final class FSSTDecompressor {

  private final FSSTSymbolTable table;

  public FSSTDecompressor(FSSTSymbolTable table) {
    this.table = table;
  }

  /**
   * Decompresses {@code compressed[off..off+len)} into {@code output}. Uses pre-decoded long values
   * to write up to 8 symbol bytes with a single store, avoiding per-symbol arraycopy.
   *
   * @return the number of decompressed bytes written to output
   */
  public int decompress(byte[] compressed, int off, int len, byte[] output) {
    final byte[] symbolLen = table.len;
    final long[] decodeLong = table.decodeLong;
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      int code = compressed[pos++] & 0xFF;
      if (code != FSSTSymbolTable.ESCAPE) {
        int l = symbolLen[code] & 0xFF;
        long v = decodeLong[code];
        // Write up to 8 bytes — we always write 8 but only advance by symbol length.
        // Caller must ensure output has at least 7 bytes of slack beyond actual decompressed size.
        if (outPos + 8 <= output.length) {
          writeLongLE(output, outPos, v);
        } else {
          // Fallback for last few bytes
          for (int i = 0; i < l; i++) {
            output[outPos + i] = (byte) (v >>> (i * 8));
          }
        }
        outPos += l;
      } else {
        output[outPos++] = compressed[pos++];
      }
    }
    return outPos;
  }

  /** Write a long in little-endian order to a byte array. */
  private static void writeLongLE(byte[] b, int off, long v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
    b[off + 2] = (byte) (v >>> 16);
    b[off + 3] = (byte) (v >>> 24);
    b[off + 4] = (byte) (v >>> 32);
    b[off + 5] = (byte) (v >>> 40);
    b[off + 6] = (byte) (v >>> 48);
    b[off + 7] = (byte) (v >>> 56);
  }
}
