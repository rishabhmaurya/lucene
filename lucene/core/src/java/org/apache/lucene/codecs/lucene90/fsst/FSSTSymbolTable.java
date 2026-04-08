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
 * FSST (Fast Static Symbol Table) symbol table. Maps 255 codes (0-254) to symbols of 1-8 bytes.
 * Code 255 (0xFF) is reserved as the ESCAPE code for unmatched bytes.
 *
 * <p>Binary format: 255 length bytes followed by 255 × 8 symbol bytes (2295 bytes total). Each
 * symbol is stored in the first {@code len[i]} bytes of its 8-byte slot.
 *
 * @see <a href="https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf">FSST: Fast Random Access String
 *     Compression (VLDB 2020)</a>
 */
public final class FSSTSymbolTable {

  /** The escape code — indicates the next byte is a literal (uncompressed) byte. */
  public static final int ESCAPE = 0xFF;

  /** Maximum number of symbols in the table (codes 0-254). */
  public static final int MAX_SYMBOLS = 255;

  /** Serialized size in bytes: 255 lengths + 255 × 8 symbol bytes. */
  public static final int SERIALIZED_SIZE = MAX_SYMBOLS + MAX_SYMBOLS * 8;

  /** Symbol lengths — public for inlined decompression in hot paths. */
  public final byte[] len = new byte[MAX_SYMBOLS];

  private final byte[] symbols = new byte[MAX_SYMBOLS * 8];

  /** Pre-decoded symbols as longs for fast decompression. Public for inlined hot paths. */
  public final long[] decodeLong = new long[MAX_SYMBOLS];

  private FSSTSymbolTable() {}

  /** Load a symbol table from a byte array (at least {@link #SERIALIZED_SIZE} bytes). */
  public static FSSTSymbolTable load(byte[] data) {
    if (data.length < SERIALIZED_SIZE) {
      throw new IllegalArgumentException(
          "Expected at least " + SERIALIZED_SIZE + " bytes, got " + data.length);
    }
    FSSTSymbolTable t = new FSSTSymbolTable();
    System.arraycopy(data, 0, t.len, 0, MAX_SYMBOLS);
    System.arraycopy(data, MAX_SYMBOLS, t.symbols, 0, MAX_SYMBOLS * 8);
    // Pre-decode symbols as little-endian longs for fast decompression
    for (int i = 0; i < MAX_SYMBOLS; i++) {
      long v = 0;
      int base = i * 8;
      int l = t.len[i] & 0xFF;
      for (int j = 0; j < l; j++) {
        v |= (long) (t.symbols[base + j] & 0xFF) << (j * 8);
      }
      t.decodeLong[i] = v;
    }
    return t;
  }

  /** Serialize this symbol table to a byte array. */
  public byte[] toBytes() {
    byte[] out = new byte[SERIALIZED_SIZE];
    System.arraycopy(len, 0, out, 0, MAX_SYMBOLS);
    System.arraycopy(symbols, 0, out, MAX_SYMBOLS, MAX_SYMBOLS * 8);
    return out;
  }

  /** Returns the length of the symbol for the given code (0 if unused). */
  public int symbolLength(int code) {
    return len[code] & 0xFF;
  }

  /**
   * Copies the symbol bytes for the given code into {@code dest} at {@code destOff}.
   *
   * @return the symbol length
   */
  public int symbolBytes(int code, byte[] dest, int destOff) {
    int l = len[code] & 0xFF;
    System.arraycopy(symbols, code * 8, dest, destOff, l);
    return l;
  }

  /** Returns the first byte of the symbol for the given code. */
  byte firstByte(int code) {
    return symbols[code * 8];
  }
}
