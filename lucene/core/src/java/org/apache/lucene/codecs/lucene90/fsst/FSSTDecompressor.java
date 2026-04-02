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
   * Decompresses {@code compressed[off..off+len)} into {@code output}.
   *
   * @return the number of decompressed bytes written to output
   */
  public int decompress(byte[] compressed, int off, int len, byte[] output) {
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      int code = compressed[pos++] & 0xFF;
      if (code != FSSTSymbolTable.ESCAPE) {
        outPos += table.symbolBytes(code, output, outPos);
      } else {
        output[outPos++] = compressed[pos++];
      }
    }
    return outPos;
  }
}
