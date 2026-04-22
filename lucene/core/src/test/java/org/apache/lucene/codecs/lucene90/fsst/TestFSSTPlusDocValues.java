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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.TermsDictMode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for FSST+ (prefix-chunked FSST) doc values integration. */
public class TestFSSTPlusDocValues extends LuceneTestCase {

  private static Lucene104Codec fsstPlusCodec() {
    Lucene90DocValuesFormat dvFormat =
        new Lucene90DocValuesFormat(4096, TermsDictMode.FSST_PLUS);
    return new Lucene104Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return dvFormat;
      }
    };
  }

  /** Basic round-trip: write and read back sorted doc values. */
  public void testBasicSortedDocValues() throws Exception {
    Path tempDir = createTempDir();
    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (String val : new String[] {"apple", "banana", "cherry", "date", "elderberry"}) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("fruit", new BytesRef(val)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("fruit");
        assertEquals(5, dv.getValueCount());
        assertEquals("apple", dv.lookupOrd(0).utf8ToString());
        assertEquals("banana", dv.lookupOrd(1).utf8ToString());
        assertEquals("cherry", dv.lookupOrd(2).utf8ToString());
        assertEquals("date", dv.lookupOrd(3).utf8ToString());
        assertEquals("elderberry", dv.lookupOrd(4).utf8ToString());
      }
    }
  }

  /** URLs with shared prefixes — the primary use case for FSST+. */
  public void testURLsWithSharedPrefixes() throws Exception {
    Path tempDir = createTempDir();
    List<String> urls = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      urls.add("http://example.com/products/item" + i);
    }
    for (int i = 0; i < 100; i++) {
      urls.add("http://example.com/categories/cat" + i);
    }
    for (int i = 0; i < 50; i++) {
      urls.add("http://other-site.org/page/" + i);
    }
    Collections.sort(urls);
    List<String> uniqueUrls = new ArrayList<>(new java.util.TreeSet<>(urls));

    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (String url : urls) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(url)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(uniqueUrls.size(), dv.getValueCount());
        for (int i = 0; i < uniqueUrls.size(); i++) {
          assertEquals("Mismatch at ord " + i, uniqueUrls.get(i), dv.lookupOrd(i).utf8ToString());
        }
      }
    }
  }

  /** SortedSet doc values with multiple values per doc. */
  public void testSortedSetDocValues() throws Exception {
    Path tempDir = createTempDir();
    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("http://a.com/1")));
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("http://a.com/2")));
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("http://b.com/1")));
        w.addDocument(doc);
        doc = new Document();
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("http://a.com/1")));
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("http://c.com/x")));
        w.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedSetDocValues dv =
            reader.leaves().get(0).reader().getSortedSetDocValues("tags");
        assertEquals(4, dv.getValueCount());
        assertEquals("http://a.com/1", dv.lookupOrd(0).utf8ToString());
        assertEquals("http://a.com/2", dv.lookupOrd(1).utf8ToString());
        assertEquals("http://b.com/1", dv.lookupOrd(2).utf8ToString());
        assertEquals("http://c.com/x", dv.lookupOrd(3).utf8ToString());
      }
    }
  }

  /** Single term — edge case with one-element block. */
  public void testSingleTerm() throws Exception {
    Path tempDir = createTempDir();
    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        Document doc = new Document();
        doc.add(new SortedDocValuesField("f", new BytesRef("only-one")));
        w.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("f");
        assertEquals(1, dv.getValueCount());
        assertEquals("only-one", dv.lookupOrd(0).utf8ToString());
      }
    }
  }

  /** Empty string value. */
  public void testEmptyString() throws Exception {
    Path tempDir = createTempDir();
    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        Document doc = new Document();
        doc.add(new SortedDocValuesField("f", new BytesRef("")));
        w.addDocument(doc);
        doc = new Document();
        doc.add(new SortedDocValuesField("f", new BytesRef("notempty")));
        w.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("f");
        assertEquals(2, dv.getValueCount());
        assertEquals("", dv.lookupOrd(0).utf8ToString());
        assertEquals("notempty", dv.lookupOrd(1).utf8ToString());
      }
    }
  }

  /** More than 128 terms — tests multi-block handling. */
  public void testMultipleBlocks() throws Exception {
    Path tempDir = createTempDir();
    int numTerms = 500;
    List<String> terms = new ArrayList<>();
    for (int i = 0; i < numTerms; i++) {
      terms.add(String.format("http://example.com/page/%05d", i));
    }

    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (String t : terms) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(t)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(numTerms, dv.getValueCount());
        // Check all terms
        for (int i = 0; i < numTerms; i++) {
          assertEquals("Mismatch at ord " + i, terms.get(i), dv.lookupOrd(i).utf8ToString());
        }
        // Random access
        assertEquals(terms.get(0), dv.lookupOrd(0).utf8ToString());
        assertEquals(terms.get(127), dv.lookupOrd(127).utf8ToString());
        assertEquals(terms.get(128), dv.lookupOrd(128).utf8ToString());
        assertEquals(terms.get(numTerms - 1), dv.lookupOrd(numTerms - 1).utf8ToString());
      }
    }
  }

  /** Diverse data with no shared prefixes — FSST+ should still work correctly. */
  public void testNoPrefixSharing() throws Exception {
    Path tempDir = createTempDir();
    String[] terms = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"};

    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (String t : terms) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("word", new BytesRef(t)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("word");
        Arrays.sort(terms);
        assertEquals(terms.length, dv.getValueCount());
        for (int i = 0; i < terms.length; i++) {
          assertEquals(terms[i], dv.lookupOrd(i).utf8ToString());
        }
      }
    }
  }

  /** Binary/non-ASCII data. */
  public void testBinaryData() throws Exception {
    Path tempDir = createTempDir();
    byte[][] values = {
      {0x00, 0x01, 0x02},
      {0x00, 0x01, 0x03},
      {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD},
    };

    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (byte[] v : values) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("bin", new BytesRef(v)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("bin");
        assertEquals(3, dv.getValueCount());
        // Sorted order: 0x000102, 0x000103, 0xFFFEFD
        assertArrayEquals(values[0], toBytes(dv.lookupOrd(0)));
        assertArrayEquals(values[1], toBytes(dv.lookupOrd(1)));
        assertArrayEquals(values[2], toBytes(dv.lookupOrd(2)));
      }
    }
  }

  /** Exactly 128 terms — one full block. */
  public void testExactlyOneBlock() throws Exception {
    Path tempDir = createTempDir();
    List<String> terms = new ArrayList<>();
    for (int i = 0; i < 128; i++) {
      terms.add(String.format("prefix_%04d_suffix", i));
    }

    try (MMapDirectory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(fsstPlusCodec());
      conf.setMergePolicy(NoMergePolicy.INSTANCE);
      conf.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        for (String t : terms) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("f", new BytesRef(t)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("f");
        assertEquals(128, dv.getValueCount());
        for (int i = 0; i < 128; i++) {
          assertEquals(terms.get(i), dv.lookupOrd(i).utf8ToString());
        }
      }
    }
  }

  /** Test the DP prefix chunker directly. */
  public void testPrefixChunkerDP() {
    FSSTPlusPrefixChunker chunker = new FSSTPlusPrefixChunker();

    // 4 URLs with shared prefix
    byte[] buf = concat(
        "http://example.com/a".getBytes(),
        "http://example.com/b".getBytes(),
        "http://example.com/c".getBytes(),
        "http://other.org/x".getBytes());
    int[] offsets = {0, 20, 40, 60, 78};

    int numChunks = chunker.solve(buf, offsets, 4, 0.67f);
    assertTrue("Expected at least 1 chunk", numChunks >= 1);
    // First chunk should have prefix >= 4 bytes (http://example.com/)
    if (numChunks >= 2) {
      assertTrue("First chunk prefix should be long", chunker.chunkPrefixLen(0) >= 4);
    }
  }

  /** Test chunker with single term. */
  public void testPrefixChunkerSingleTerm() {
    FSSTPlusPrefixChunker chunker = new FSSTPlusPrefixChunker();
    byte[] buf = "hello".getBytes();
    int[] offsets = {0, 5};
    assertEquals(1, chunker.solve(buf, offsets, 1, 0.67f));
    assertEquals(0, chunker.chunkPrefixLen(0));
  }

  /** Test chunker with empty input. */
  public void testPrefixChunkerEmpty() {
    FSSTPlusPrefixChunker chunker = new FSSTPlusPrefixChunker();
    assertEquals(0, chunker.solve(new byte[0], new int[] {0}, 0, 0.67f));
  }

  public void testMergeWithLongTerms() throws Exception {
    // Test that merge works with terms longer than 1024 bytes (exercises dynamic buffer growth)
    Path dir = createTempDir();
    Lucene90DocValuesFormat dvFormat =
        new Lucene90DocValuesFormat(4096, TermsDictMode.FSST_PLUS);
    org.apache.lucene.store.Directory directory = newFSDirectory(dir);
    org.apache.lucene.index.IndexWriterConfig config =
        new org.apache.lucene.index.IndexWriterConfig();
    config.setCodec(
        new Lucene104Codec() {
          @Override
          public DocValuesFormat getDocValuesFormatForField(String field) {
            return dvFormat;
          }
        });
    config.setMergePolicy(new org.apache.lucene.index.LogDocMergePolicy());
    org.apache.lucene.index.IndexWriter writer =
        new org.apache.lucene.index.IndexWriter(directory, config);

    // Create long terms with shared prefixes > 1024 bytes
    String longPrefix = "http://www.example.com/very/deep/path/".repeat(30); // ~1140 chars
    List<String> allTerms = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      allTerms.add(longPrefix + String.format("suffix-%05d", i));
    }
    Collections.sort(allTerms);

    // Write in two segments with non-overlapping terms to force merge
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("field", new org.apache.lucene.util.BytesRef(allTerms.get(i))));
      writer.addDocument(doc);
    }
    writer.flush();
    for (int i = 100; i < 200; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("field", new org.apache.lucene.util.BytesRef(allTerms.get(i))));
      writer.addDocument(doc);
    }
    writer.flush();

    // Force merge — this exercises termsEnum() during OrdinalMap build
    writer.forceMerge(1);
    writer.close();

    // Verify all terms readable
    org.apache.lucene.index.DirectoryReader reader =
        org.apache.lucene.index.DirectoryReader.open(directory);
    org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
    org.apache.lucene.index.SortedDocValues sdv = leaf.getSortedDocValues("field");
    assertEquals(200, sdv.getValueCount());
    for (int i = 0; i < 200; i++) {
      assertEquals(allTerms.get(i), sdv.lookupOrd(i).utf8ToString());
    }
    reader.close();
    directory.close();
  }

  private static byte[] toBytes(BytesRef ref) {
    return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
  }

  private static byte[] concat(byte[]... arrays) {
    int total = 0;
    for (byte[] a : arrays) total += a.length;
    byte[] result = new byte[total];
    int pos = 0;
    for (byte[] a : arrays) {
      System.arraycopy(a, 0, result, pos, a.length);
      pos += a.length;
    }
    return result;
  }

  public void testLargeBlockWithLongSuffixes() throws Exception {
    // Exercises: (1) suffix offsets > 255 (endianness), (2) decompressor near buffer boundary,
    // (3) merge with termsEnum() on large terms
    Path tempDir = createTempDir();
    Lucene90DocValuesFormat dvFormat = new Lucene90DocValuesFormat(4096, TermsDictMode.FSST_PLUS);
    org.apache.lucene.store.Directory directory = newFSDirectory(tempDir);
    org.apache.lucene.index.IndexWriterConfig config = new org.apache.lucene.index.IndexWriterConfig();
    config.setCodec(new Lucene104Codec() {
      @Override public DocValuesFormat getDocValuesFormatForField(String f) { return dvFormat; }
    });
    config.setMergePolicy(new org.apache.lucene.index.LogDocMergePolicy());
    org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(directory, config);

    // 256 terms with shared prefix + long random suffixes across 2 segments
    // Suffix offsets will exceed 255 bytes, triggering endianness bugs if present
    String prefix = "http://example.com/";
    java.util.Random rng = new java.util.Random(42);
    List<String> allTerms = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      StringBuilder sb = new StringBuilder(prefix);
      for (int j = 0; j < 600; j++) sb.append((char)('a' + rng.nextInt(26)));
      allTerms.add(sb.toString());
    }
    Collections.sort(allTerms);

    // Write 2 segments to force merge
    for (int i = 0; i < 128; i++) {
      org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
      doc.add(new SortedSetDocValuesField("field", new org.apache.lucene.util.BytesRef(allTerms.get(i))));
      writer.addDocument(doc);
    }
    writer.flush();
    for (int i = 128; i < 256; i++) {
      org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
      doc.add(new SortedSetDocValuesField("field", new org.apache.lucene.util.BytesRef(allTerms.get(i))));
      writer.addDocument(doc);
    }
    writer.flush();

    // Force merge exercises termsEnum() → lookupOrd() → decompressor near buffer boundary
    writer.forceMerge(1);
    writer.close();

    org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(directory);
    org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
    org.apache.lucene.index.SortedSetDocValues sdv = leaf.getSortedSetDocValues("field");
    assertEquals(256, sdv.getValueCount());
    for (int i = 0; i < 256; i++) {
      assertEquals(allTerms.get(i), sdv.lookupOrd(i).utf8ToString());
    }
    reader.close();
    directory.close();
  }

  public void testMergeWithLargeBlocks() throws Exception {
    // Exercises suffix offset overflow: 128 terms * ~3000 bytes each = suffix area > 65535 bytes
    // This would crash with 16-bit suffix offsets
    Path tempDir = createTempDir();
    Lucene90DocValuesFormat dvFormat = new Lucene90DocValuesFormat(4096, TermsDictMode.FSST_PLUS);
    org.apache.lucene.store.Directory directory = newFSDirectory(tempDir);
    org.apache.lucene.index.IndexWriterConfig config = new org.apache.lucene.index.IndexWriterConfig();
    config.setCodec(new Lucene104Codec() {
      @Override public DocValuesFormat getDocValuesFormatForField(String f) { return dvFormat; }
    });
    config.setMaxBufferedDocs(200);
    org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(directory, config);

    java.util.Random rng = new java.util.Random(42);
    String[] prefixes = {"http://www.example.com/", "https://cdn.example.org/"};
    List<String> allTerms = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      StringBuilder sb = new StringBuilder(prefixes[rng.nextInt(prefixes.length)]);
      int len = rng.nextInt(100) < 10 ? 2000 + rng.nextInt(1500) : 50 + rng.nextInt(200);
      for (int j = 0; j < len; j++) sb.append((char)('a' + rng.nextInt(26)));
      allTerms.add(sb.toString());
    }
    Collections.sort(allTerms);
    // Deduplicate
    List<String> unique = new ArrayList<>();
    for (int i = 0; i < allTerms.size(); i++) {
      if (i == 0 || !allTerms.get(i).equals(allTerms.get(i - 1))) unique.add(allTerms.get(i));
    }

    for (String t : unique) {
      org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
      doc.add(new SortedSetDocValuesField("field", new org.apache.lucene.util.BytesRef(t)));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(directory);
    org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
    org.apache.lucene.index.SortedSetDocValues sdv = leaf.getSortedSetDocValues("field");
    assertEquals(unique.size(), sdv.getValueCount());
    for (int i = 0; i < unique.size(); i++) {
      assertEquals(unique.get(i), sdv.lookupOrd(i).utf8ToString());
    }
    reader.close();
    directory.close();
  }

  public void testConcurrentBuild() throws Exception {
    // Verify thread-safety: multiple threads building symbol tables concurrently
    int numThreads = 8;
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger();
    Thread[] threads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      final int tid = t;
      threads[t] = new Thread(() -> {
        try {
          latch.await();
          List<org.apache.lucene.util.BytesRef> samples = new ArrayList<>();
          for (int i = 0; i < 1000; i++) {
            samples.add(new org.apache.lucene.util.BytesRef("http://thread" + tid + ".example.com/path/" + i));
          }
          FSSTSymbolTable table = FSSTSymbolTableBuilder.build(samples);
          // Verify table works
          FSSTCompressor comp = new FSSTCompressor(table);
          byte[] out = new byte[256];
          int len = comp.compress("http://thread0.example.com/path/42".getBytes(), 0, 34, out);
          if (len <= 0 || len > 34) errors.incrementAndGet();
        } catch (Exception e) {
          errors.incrementAndGet();
        }
      });
      threads[t].start();
    }
    latch.countDown();
    for (Thread th : threads) th.join();
    assertEquals(0, errors.get());
  }
}

