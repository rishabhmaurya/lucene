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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for FSST symbol table, compressor, decompressor, and builder. */
public class TestFSST extends LuceneTestCase {

  /** Basic round-trip: compress then decompress should return original. */
  public void testRoundTrip() {
    // Identity table: code N -> byte N
    byte[] tableData = new byte[FSSTSymbolTable.SERIALIZED_SIZE];
    for (int i = 0; i < 255; i++) {
      tableData[i] = 1; // length 1
      tableData[255 + i * 8] = (byte) i; // symbol = byte i
    }
    FSSTSymbolTable table = FSSTSymbolTable.load(tableData);
    FSSTCompressor comp = new FSSTCompressor(table);
    FSSTDecompressor decomp = new FSSTDecompressor(table);

    String[] inputs = {"hello", "world", "http://example.com/page/1", "", "a"};
    byte[] compBuf = new byte[4096];
    byte[] decBuf = new byte[4096];

    for (String input : inputs) {
      if (input.isEmpty()) continue;
      byte[] raw = input.getBytes(StandardCharsets.UTF_8);
      int compLen = comp.compress(raw, 0, raw.length, compBuf);
      int decLen = decomp.decompress(compBuf, 0, compLen, decBuf);
      assertEquals(input, new String(decBuf, 0, decLen, StandardCharsets.UTF_8));
    }
  }

  /** Symbol table builder produces a table that compresses and decompresses correctly. */
  public void testBuilderRoundTrip() {
    List<BytesRef> terms = new ArrayList<>();
    String[] urls = {
      "http://example.com/page/1",
      "http://example.com/page/2",
      "http://example.com/page/3",
      "http://other.ru/catalog/item/100",
      "http://other.ru/catalog/item/200",
      "http://shop.example.com/product/shoes",
      "http://shop.example.com/product/shirt",
    };
    for (String url : urls) {
      terms.add(new BytesRef(url));
    }

    FSSTSymbolTable table = FSSTSymbolTableBuilder.build(terms);
    assertNotNull(table);

    FSSTCompressor comp = new FSSTCompressor(table);
    FSSTDecompressor decomp = new FSSTDecompressor(table);

    byte[] compBuf = new byte[4096];
    byte[] decBuf = new byte[4096];

    for (String url : urls) {
      byte[] raw = url.getBytes(StandardCharsets.UTF_8);
      int compLen = comp.compress(raw, 0, raw.length, compBuf);
      assertTrue("Compressed should not be empty", compLen > 0);
      int decLen = decomp.decompress(compBuf, 0, compLen, decBuf);
      assertEquals(url, new String(decBuf, 0, decLen, StandardCharsets.UTF_8));
    }
  }

  /** Builder should produce symbols that actually compress repetitive data. */
  public void testBuilderCompresses() {
    List<BytesRef> terms = new ArrayList<>();
    // Lots of repetitive URL-like terms
    for (int i = 0; i < 1000; i++) {
      terms.add(new BytesRef("http://example.com/page/" + i));
    }

    FSSTSymbolTable table = FSSTSymbolTableBuilder.build(terms);
    FSSTCompressor comp = new FSSTCompressor(table);

    byte[] compBuf = new byte[4096];
    long rawTotal = 0, compTotal = 0;
    for (BytesRef term : terms) {
      rawTotal += term.length;
      compTotal += comp.compress(term.bytes, term.offset, term.length, compBuf);
    }

    double ratio = (double) compTotal / rawTotal;
    assertTrue("Expected compression ratio below 0.9, got " + ratio, ratio < 0.9);
  }

  /** Symbol table serialization round-trip. */
  public void testSymbolTableSerialization() {
    List<BytesRef> terms = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      terms.add(new BytesRef("http://test.com/" + i));
    }

    FSSTSymbolTable original = FSSTSymbolTableBuilder.build(terms);
    byte[] serialized = original.toBytes();
    assertEquals(FSSTSymbolTable.SERIALIZED_SIZE, serialized.length);

    FSSTSymbolTable restored = FSSTSymbolTable.load(serialized);

    // Verify same compression output
    FSSTCompressor comp1 = new FSSTCompressor(original);
    FSSTCompressor comp2 = new FSSTCompressor(restored);
    byte[] buf1 = new byte[4096], buf2 = new byte[4096];

    byte[] input = "http://test.com/42".getBytes(StandardCharsets.UTF_8);
    int len1 = comp1.compress(input, 0, input.length, buf1);
    int len2 = comp2.compress(input, 0, input.length, buf2);
    assertEquals(len1, len2);
    for (int i = 0; i < len1; i++) {
      assertEquals("Byte mismatch at " + i, buf1[i], buf2[i]);
    }
  }

  /** Builder with empty input should not crash. */
  public void testBuilderEmpty() {
    FSSTSymbolTable table = FSSTSymbolTableBuilder.build(List.of());
    assertNotNull(table);
  }

  /** Builder via TermSupplier interface. */
  public void testBuilderWithTermSupplier() {
    String[] terms = {"alpha", "beta", "gamma", "delta", "alpha", "beta"};
    int[] idx = {0};
    FSSTSymbolTable table =
        FSSTSymbolTableBuilder.build(
            () -> idx[0] < terms.length ? new BytesRef(terms[idx[0]++]) : null, terms.length);
    assertNotNull(table);

    // Verify round-trip
    FSSTCompressor comp = new FSSTCompressor(table);
    FSSTDecompressor decomp = new FSSTDecompressor(table);
    byte[] compBuf = new byte[256], decBuf = new byte[256];
    for (String t : terms) {
      byte[] raw = t.getBytes(StandardCharsets.UTF_8);
      int cl = comp.compress(raw, 0, raw.length, compBuf);
      int dl = decomp.decompress(compBuf, 0, cl, decBuf);
      assertEquals(t, new String(decBuf, 0, dl, StandardCharsets.UTF_8));
    }
  }
}
