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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * FSST symbol table: up to 255 symbols of 1–8 bytes each. Binary format: 255 length bytes followed
 * by 255 × 8 symbol bytes (2295 bytes total).
 */
public final class FSSTSymbolTable {

  public static final int ESCAPE = 0xFF;
  public static final int MAX_SYMBOLS = 255;
  private static final int SERIALIZED_SIZE = MAX_SYMBOLS + MAX_SYMBOLS * 8;

  private final byte[] len = new byte[MAX_SYMBOLS];
  private final byte[] symbols = new byte[MAX_SYMBOLS * 8];

  private FSSTSymbolTable() {}

  public static FSSTSymbolTable load(Path path) throws IOException {
    return load(Files.readAllBytes(path));
  }

  public static FSSTSymbolTable load(byte[] data) {
    if (data.length < SERIALIZED_SIZE) {
      throw new IllegalArgumentException("Expected at least " + SERIALIZED_SIZE + " bytes");
    }
    FSSTSymbolTable t = new FSSTSymbolTable();
    System.arraycopy(data, 0, t.len, 0, MAX_SYMBOLS);
    System.arraycopy(data, MAX_SYMBOLS, t.symbols, 0, MAX_SYMBOLS * 8);
    return t;
  }

  public byte[] toBytes() {
    byte[] out = new byte[SERIALIZED_SIZE];
    System.arraycopy(len, 0, out, 0, MAX_SYMBOLS);
    System.arraycopy(symbols, 0, out, MAX_SYMBOLS, MAX_SYMBOLS * 8);
    return out;
  }

  public int symbolLength(int code) {
    return len[code] & 0xFF;
  }

  /**
   * Copies the symbol bytes for the given code into dest at destOff. Returns the symbol length.
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

  /** Returns the symbol byte at position j for the given code. */
  byte symbolByteAt(int code, int j) {
    return symbols[code * 8 + j];
  }
}
