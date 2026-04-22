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
 * Stateless FSST decompressor. Uses pre-decoded long values and unaligned 8-byte writes via
 * VarHandle for efficient symbol output.
 *
 * <p>Output buffer must have at least 8 bytes of slack beyond the actual decompressed size to
 * allow the VarHandle fast path to overwrite safely.
 */
public final class FSSTDecompressor {

  private final FSSTSymbolTable table;

  /** Expose symbol table for inlined decompression in hot paths. */
  public FSSTSymbolTable symbolTable() {
    return table;
  }

  public FSSTDecompressor(FSSTSymbolTable table) {
    this.table = table;
  }

  /**
   * Decompresses {@code compressed[off..off+len)} into {@code output}.
   *
   * <p>The fast path uses 8-byte VarHandle writes, so the output buffer must be at least
   * {@code decompressedSize + 8} bytes. The returned value is the exact decompressed size.
   *
   * @return the number of decompressed bytes written to output
   */
  public int decompress(byte[] compressed, int off, int len, byte[] output) {
    final byte[] symbolLen = table.len;
    final long[] symbol = table.decodeLong;
    int pos = off, end = off + len, outPos = 0;
    // Safe limit for 8-byte VarHandle writes: need outPos + 8 <= output.length
    int outSafe = output.length - 8;
    while (pos < end) {
      int code = compressed[pos++] & 0xFF;
      if (code != 0xFF) {
        if (outPos <= outSafe) {
          // Fast path: 8-byte write (may overwrite beyond actual symbol length, but within buffer)
          BitUtil.VH_LE_LONG.set(output, outPos, symbol[code]);
        } else {
          // Slow path near end of buffer: write only the actual symbol bytes
          long v = symbol[code];
          int l = symbolLen[code] & 0xFF;
          for (int i = 0; i < l && outPos + i < output.length; i++) {
            output[outPos + i] = (byte) (v >>> (i * 8));
          }
        }
        outPos += symbolLen[code] & 0xFF;
      } else {
        if (outPos < output.length) {
          output[outPos] = compressed[pos];
        }
        outPos++;
        pos++;
      }
    }
    return outPos;
  }
}
