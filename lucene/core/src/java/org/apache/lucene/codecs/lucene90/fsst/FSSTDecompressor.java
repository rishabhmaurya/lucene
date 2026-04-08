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
   * Decompresses {@code compressed[off..off+len)} into {@code output}. Caller must ensure output
   * has at least 7 bytes of slack beyond the actual decompressed size.
   *
   * @return the number of decompressed bytes written to output
   */
  public int decompress(byte[] compressed, int off, int len, byte[] output) {
    final byte[] symbolLen = table.len;
    final long[] symbol = table.decodeLong;
    int pos = off, end = off + len, outPos = 0;
    int outSafe = output.length - 8;
    while (pos < end) {
      int code = compressed[pos++] & 0xFF;
      if (code != 0xFF) {
        if (outPos <= outSafe) {
          BitUtil.VH_LE_LONG.set(output, outPos, symbol[code]);
        } else {
          long v = symbol[code];
          int l = symbolLen[code] & 0xFF;
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
}
