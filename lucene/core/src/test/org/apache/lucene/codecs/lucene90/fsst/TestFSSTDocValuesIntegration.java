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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Integration test: write index with FSST doc values, read back, verify lookupOrd and
 * lookupCompressedOrd work correctly.
 */
public class TestFSSTDocValuesIntegration extends LuceneTestCase {

  static final String[] URLS = {
    "http://example.com/page/1",
    "http://example.com/page/2",
    "http://example.com/page/3",
    "http://other.ru/catalog/item/100",
    "http://other.ru/catalog/item/200",
    "http://shop.example.com/product/shoes",
    "http://shop.example.com/product/shirt",
    "http://news.example.com/article/2024/01",
    "http://news.example.com/article/2024/02",
    "http://video.yandex.ru/search?text=music",
  };

  /** Write SORTED field with FSST, read back, verify all terms. */
  public void testSortedDocValuesRoundTrip() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        for (String url : URLS) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(url)));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      // Read and verify
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        SortedDocValues dv = leaf.getSortedDocValues("url");
        assertNotNull(dv);
        assertEquals(URLS.length, dv.getValueCount());

        // Collect sorted terms
        List<String> sortedTerms = new ArrayList<>();
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          sortedTerms.add(dv.lookupOrd(ord).utf8ToString());
        }

        // Verify all terms are present and sorted
        List<String> expected = new ArrayList<>(List.of(URLS));
        expected.sort(String::compareTo);
        assertEquals(expected, sortedTerms);

        // Verify doc -> ord -> term round-trip
        for (int docId = 0; docId < URLS.length; docId++) {
          assertTrue(dv.advanceExact(docId));
          int ord = dv.ordValue();
          String term = dv.lookupOrd(ord).utf8ToString();
          assertTrue("Term should be a valid URL: " + term, term.startsWith("http://"));
        }

        // Verify lookupTerm
        for (String url : URLS) {
          int ord = dv.lookupTerm(new BytesRef(url));
          assertTrue("Should find term: " + url, ord >= 0);
          assertEquals(url, dv.lookupOrd(ord).utf8ToString());
        }

        // Verify FSSTCompressedAccess
        if (dv instanceof FSSTCompressedAccess fsst) {
          assertTrue("Should have compressed access", fsst.hasCompressedAccess());
          byte[] decompBuf = new byte[4096];
          for (int ord = 0; ord < dv.getValueCount(); ord++) {
            BytesRef compressed = fsst.lookupCompressedOrd(ord);
            assertNotNull(compressed);
            assertTrue(compressed.length > 0);
            int decLen = fsst.decompress(compressed, decompBuf);
            String decompressed = new String(decompBuf, 0, decLen, StandardCharsets.UTF_8);
            assertEquals(sortedTerms.get(ord), decompressed);
          }
        }
      }
    }
  }

  /** Write SORTED_SET field, read back, verify. */
  public void testSortedSetDocValuesRoundTrip() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        for (int i = 0; i < URLS.length; i++) {
          Document doc = new Document();
          doc.add(new SortedSetDocValuesField("urls", new BytesRef(URLS[i])));
          // Add a second value for some docs
          if (i % 2 == 0 && i + 1 < URLS.length) {
            doc.add(new SortedSetDocValuesField("urls", new BytesRef(URLS[i + 1])));
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        SortedSetDocValues dv = leaf.getSortedSetDocValues("urls");
        assertNotNull(dv);
        assertEquals(URLS.length, dv.getValueCount());

        // Verify all ordinals resolve to valid terms
        for (long ord = 0; ord < dv.getValueCount(); ord++) {
          BytesRef term = dv.lookupOrd(ord);
          assertNotNull(term);
          assertTrue(term.utf8ToString().startsWith("http://"));
        }
      }
    }
  }

  /** Verify FSST survives segment merge — two segments merged into one. */
  public void testMerge() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      IndexWriterConfig conf =
          new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        // Segment 1
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(URLS[i])));
          writer.addDocument(doc);
        }
        writer.flush();
        // Segment 2
        for (int i = 5; i < URLS.length; i++) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(URLS[i])));
          writer.addDocument(doc);
        }
        writer.flush();
      }

      // Verify 2 segments
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertTrue("Should have 2 segments", reader.leaves().size() >= 2);
      }

      // Now merge
      conf = new IndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        writer.forceMerge(1);
      }

      // Verify merged segment
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(URLS.length, dv.getValueCount());
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          assertTrue(dv.lookupOrd(ord).utf8ToString().startsWith("http://"));
        }
      }
    }
  }

  /** Larger dataset — 1000 terms. */
  public void testLargerDataset() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec);
      List<String> allTerms = new ArrayList<>();
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        for (int i = 0; i < 1000; i++) {
          String url = "http://example.com/page/" + i + "/detail?id=" + (i * 7);
          allTerms.add(url);
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(url)));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(1000, dv.getValueCount());

        // Verify all terms readable
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          String term = dv.lookupOrd(ord).utf8ToString();
          assertTrue(term.startsWith("http://example.com/page/"));
        }

        // Verify lookupTerm for random terms
        for (int i = 0; i < 100; i++) {
          String url = allTerms.get(i * 10);
          int ord = dv.lookupTerm(new BytesRef(url));
          assertTrue("Should find: " + url, ord >= 0);
          assertEquals(url, dv.lookupOrd(ord).utf8ToString());
        }

        // Verify FSSTCompressedAccess round-trip
        if (dv instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
          byte[] buf = new byte[4096];
          for (int ord = 0; ord < dv.getValueCount(); ord++) {
            BytesRef compressed = fsst.lookupCompressedOrd(ord);
            int len = fsst.decompress(compressed, buf);
            assertEquals(
                dv.lookupOrd(ord).utf8ToString(), new String(buf, 0, len, StandardCharsets.UTF_8));
          }
        }
      }
    }
  }

  /** Verify FSST works with compound file format (default Lucene behavior). */
  public void testCompoundFile() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      // Explicitly enable compound file (the default)
      IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(true);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        for (String url : URLS) {
          Document doc = new Document();
          doc.add(new SortedDocValuesField("url", new BytesRef(url)));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(URLS.length, dv.getValueCount());
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          String term = dv.lookupOrd(ord).utf8ToString();
          assertTrue("Term should be a URL: " + term, term.startsWith("http://"));
        }
        // Verify compressed access works through compound file
        if (dv instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
          byte[] buf = new byte[4096];
          for (int ord = 0; ord < dv.getValueCount(); ord++) {
            BytesRef compressed = fsst.lookupCompressedOrd(ord);
            int len = fsst.decompress(compressed, buf);
            assertEquals(
                dv.lookupOrd(ord).utf8ToString(), new String(buf, 0, len, StandardCharsets.UTF_8));
          }
        }
      }
    }
  }

  /** Multi-segment merge with many unique terms — verifies no data corruption during merge. */
  public void testLargeMerge() throws IOException {
    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      // Use NoMergePolicy to create multiple segments, then merge explicitly
      IndexWriterConfig conf =
          new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
      int docsPerSegment = 5000;
      int numSegments = 3;
      Set<String> allTerms = new TreeSet<>();
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        for (int seg = 0; seg < numSegments; seg++) {
          for (int i = 0; i < docsPerSegment; i++) {
            int id = seg * docsPerSegment + i;
            String term = "http://example" + (id % 200) + ".com/path/" + id + "/item?q=" + (id * 3);
            allTerms.add(term);
            Document doc = new Document();
            doc.add(new SortedDocValuesField("url", new BytesRef(term)));
            writer.addDocument(doc);
          }
          writer.flush();
        }
      }

      // Verify multiple segments exist
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertTrue("Should have multiple segments", reader.leaves().size() >= numSegments);
      }

      // Merge into one segment
      conf = new IndexWriterConfig().setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, conf)) {
        writer.forceMerge(1);
      }

      // Verify merged result
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("url");
        assertEquals(allTerms.size(), dv.getValueCount());

        // Verify all terms in sorted order
        Iterator<String> expected = allTerms.iterator();
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          assertEquals("Mismatch at ord " + ord, expected.next(), dv.lookupOrd(ord).utf8ToString());
        }
      }
    }
  }

  public void testSeekCeilAndLookupTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(fsstCodec());
    String[] terms = {
      "http://a.com/1", "http://a.com/2", "http://b.com/1",
      "http://c.com/1", "http://d.com/1", "http://z.com/1"
    };
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (String t : terms) {
        Document doc = new Document();
        doc.add(new SortedDocValuesField("url", new BytesRef(t)));
        w.addDocument(doc);
      }
    }
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      LeafReader leaf = reader.leaves().get(0).reader();
      SortedDocValues dv = leaf.getSortedDocValues("url");
      // lookupTerm — exact match
      assertTrue(dv.lookupTerm(new BytesRef("http://a.com/1")) >= 0);
      assertTrue(dv.lookupTerm(new BytesRef("http://z.com/1")) >= 0);
      // lookupTerm — not found
      int result = dv.lookupTerm(new BytesRef("http://a.com/0"));
      assertTrue(result < 0);
      // seekCeil via termsEnum
      TermsEnum te = dv.termsEnum();
      assertEquals(TermsEnum.SeekStatus.FOUND, te.seekCeil(new BytesRef("http://b.com/1")));
      assertEquals("http://b.com/1", te.term().utf8ToString());
      assertEquals(TermsEnum.SeekStatus.NOT_FOUND, te.seekCeil(new BytesRef("http://b.com/0")));
      assertEquals("http://b.com/1", te.term().utf8ToString());
      assertEquals(TermsEnum.SeekStatus.END, te.seekCeil(new BytesRef("zzz")));
    }
    dir.close();
  }

  public void testSequentialNext() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(fsstCodec());
    TreeSet<String> expected = new TreeSet<>();
    for (int i = 0; i < 200; i++) {
      expected.add("http://example.com/page/" + i + "/detail?id=" + i);
    }
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (String t : expected) {
        Document doc = new Document();
        doc.add(new SortedDocValuesField("url", new BytesRef(t)));
        w.addDocument(doc);
      }
    }
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      LeafReader leaf = reader.leaves().get(0).reader();
      SortedDocValues dv = leaf.getSortedDocValues("url");
      // Iterate via next() and verify order matches sorted set
      TermsEnum te = dv.termsEnum();
      Iterator<String> it = expected.iterator();
      BytesRef term;
      while ((term = te.next()) != null) {
        assertTrue(it.hasNext());
        assertEquals(it.next(), term.utf8ToString());
      }
      assertFalse(it.hasNext());
    }
    dir.close();
  }

  /**
   * Stress test with diverse term patterns: short, long, single-byte, multi-byte UTF-8, repeated
   * prefixes, unique terms, empty-ish terms, and high cardinality. Tests all access paths:
   * lookupOrd, next, seekCeil, lookupTerm, compressed access. Runs across: no-merge, force-merge,
   * compound file on, compound file off.
   */
  public void testDiverseDataAllConfigurations() throws IOException {
    // Build diverse term set
    TreeSet<String> termSet = new TreeSet<>();
    // Short terms (1-3 chars)
    termSet.add("a");
    termSet.add("ab");
    termSet.add("z");
    termSet.add("zz");
    // Single byte that would be escape in FSST
    for (int b = 0; b < 256; b += 37) {
      termSet.add("x" + (char) ('A' + (b % 26)) + b);
    }
    // Long shared prefix (stress prefix coding)
    for (int i = 0; i < 100; i++) {
      termSet.add("http://very-long-shared-prefix.example.com/path/to/resource/" + i);
    }
    // Varying lengths (1 to 200 bytes)
    for (int len = 1; len <= 200; len += 13) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < len; j++) sb.append((char) ('a' + (j % 26)));
      termSet.add(sb.toString());
    }
    // Multi-byte UTF-8 (2, 3, 4 byte chars)
    termSet.add("café");
    termSet.add("日本語テスト");
    termSet.add("Ελληνικά");
    termSet.add("emoji\uD83D\uDE00test");
    termSet.add("mixed_αβγ_123");
    // Duplicate-heavy: many docs mapping to few terms
    String[] heavyTerms = {"popular_term_A", "popular_term_B", "popular_term_C"};
    // Numeric-like strings
    for (int i = 0; i < 50; i++) {
      termSet.add(String.format(java.util.Locale.ROOT, "%010d", i * 7919));
    }
    // Terms that differ only in last byte
    for (int i = 0; i < 20; i++) {
      termSet.add("identical_prefix_" + (char) ('a' + i));
    }

    List<String> sortedTerms = new ArrayList<>(termSet);
    int uniqueCount = sortedTerms.size();

    // Test 4 configurations
    boolean[][] configs = {
      {false, false}, // no merge, no compound
      {false, true}, // no merge, compound
      {true, false}, // force merge, no compound
      {true, true}, // force merge, compound
    };

    for (boolean[] config : configs) {
      boolean doMerge = config[0];
      boolean useCompound = config[1];

      try (Directory dir = newDirectory()) {
        Lucene104Codec codec = fsstCodec();

        // Index: split docs across segments if merging, else single segment
        if (doMerge) {
          IndexWriterConfig iwc =
              new IndexWriterConfig()
                  .setCodec(codec)
                  .setMergePolicy(NoMergePolicy.INSTANCE)
                  .setUseCompoundFile(useCompound);
          try (IndexWriter w = new IndexWriter(dir, iwc)) {
            int half = sortedTerms.size() / 2;
            // Segment 1: first half + heavy terms
            for (int i = 0; i < half; i++) {
              Document doc = new Document();
              doc.add(new SortedDocValuesField("val", new BytesRef(sortedTerms.get(i))));
              w.addDocument(doc);
            }
            for (String ht : heavyTerms) {
              Document doc = new Document();
              doc.add(new SortedDocValuesField("val", new BytesRef(ht)));
              w.addDocument(doc);
            }
            w.flush();
            // Segment 2: second half + heavy terms (overlap)
            for (int i = half; i < sortedTerms.size(); i++) {
              Document doc = new Document();
              doc.add(new SortedDocValuesField("val", new BytesRef(sortedTerms.get(i))));
              w.addDocument(doc);
            }
            for (String ht : heavyTerms) {
              Document doc = new Document();
              doc.add(new SortedDocValuesField("val", new BytesRef(ht)));
              w.addDocument(doc);
            }
            w.flush();
          }
          // Merge
          IndexWriterConfig mergeConf =
              new IndexWriterConfig().setCodec(codec).setUseCompoundFile(useCompound);
          try (IndexWriter w = new IndexWriter(dir, mergeConf)) {
            w.forceMerge(1);
          }
        } else {
          IndexWriterConfig iwc =
              new IndexWriterConfig().setCodec(codec).setUseCompoundFile(useCompound);
          try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (String t : sortedTerms) {
              Document doc = new Document();
              doc.add(new SortedDocValuesField("val", new BytesRef(t)));
              w.addDocument(doc);
            }
            // Add duplicates
            for (int i = 0; i < 50; i++) {
              Document doc = new Document();
              doc.add(
                  new SortedDocValuesField("val", new BytesRef(heavyTerms[i % heavyTerms.length])));
              w.addDocument(doc);
            }
          }
        }

        // Verify
        String label = "merge=" + doMerge + " compound=" + useCompound;
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          assertEquals(label + " should have 1 segment", 1, reader.leaves().size());
          SortedDocValues dv = reader.leaves().get(0).reader().getSortedDocValues("val");
          assertNotNull(label, dv);

          // Add heavy terms to expected set
          for (String ht : heavyTerms) termSet.add(ht);
          List<String> expected = new ArrayList<>(termSet);
          assertEquals(label + " value count", expected.size(), dv.getValueCount());

          // 1. lookupOrd: verify all ordinals
          for (int ord = 0; ord < dv.getValueCount(); ord++) {
            assertEquals(
                label + " lookupOrd(" + ord + ")",
                expected.get(ord),
                dv.lookupOrd(ord).utf8ToString());
          }

          // 2. Random access lookupOrd (non-sequential)
          int[] randomOrds = {
            0,
            dv.getValueCount() - 1,
            dv.getValueCount() / 2,
            1,
            dv.getValueCount() / 3,
            dv.getValueCount() - 2
          };
          for (int ord : randomOrds) {
            assertEquals(
                label + " random lookupOrd(" + ord + ")",
                expected.get(ord),
                dv.lookupOrd(ord).utf8ToString());
          }

          // 3. next() sequential iteration
          TermsEnum te = dv.termsEnum();
          int count = 0;
          BytesRef term;
          while ((term = te.next()) != null) {
            assertEquals(label + " next() ord=" + count, expected.get(count), term.utf8ToString());
            count++;
          }
          assertEquals(label + " next() count", expected.size(), count);

          // 4. seekCeil
          te = dv.termsEnum();
          // Exact match
          assertEquals(
              label + " seekCeil exact",
              TermsEnum.SeekStatus.FOUND,
              te.seekCeil(new BytesRef(expected.get(0))));
          assertEquals(expected.get(0), te.term().utf8ToString());
          // Not found — should land past last term
          assertEquals(
              label + " seekCeil past end",
              TermsEnum.SeekStatus.END,
              te.seekCeil(new BytesRef("\uffff\uffff\uffff")));
          // seekCeil on term before first
          assertEquals(
              label + " seekCeil before first",
              TermsEnum.SeekStatus.NOT_FOUND,
              te.seekCeil(new BytesRef("")));

          // 5. lookupTerm
          for (int i = 0; i < Math.min(20, expected.size()); i++) {
            assertTrue(
                label + " lookupTerm(" + expected.get(i) + ")",
                dv.lookupTerm(new BytesRef(expected.get(i))) >= 0);
          }
          assertTrue(
              label + " lookupTerm missing",
              dv.lookupTerm(new BytesRef("NONEXISTENT_TERM_XYZ")) < 0);

          // 6. Compressed access
          if (dv instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
            byte[] buf = new byte[4096];
            for (int ord = 0; ord < dv.getValueCount(); ord++) {
              BytesRef compressed = fsst.lookupCompressedOrd(ord);
              assertNotNull(label + " compressed ord=" + ord, compressed);
              int len = fsst.decompress(compressed, buf);
              assertEquals(
                  label + " decompress ord=" + ord,
                  expected.get(ord),
                  new String(buf, 0, len, StandardCharsets.UTF_8));
            }
          }
        }
      }
    }
  }

  /** Test SortedSet with diverse multi-valued data across merge and compound configs. */
  public void testSortedSetDiverse() throws IOException {
    TreeSet<String> allTerms = new TreeSet<>();
    // Mix of patterns
    for (int i = 0; i < 200; i++) {
      allTerms.add("category_" + (i % 30));
      allTerms.add("tag:" + String.format(java.util.Locale.ROOT, "%05d", i));
      if (i % 10 == 0) allTerms.add("long_prefix_shared_across_many_terms_" + i);
    }
    List<String> termList = new ArrayList<>(allTerms);

    try (Directory dir = newDirectory()) {
      Lucene104Codec codec = fsstCodec();
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        // Each doc gets 1-5 values
        for (int d = 0; d < 500; d++) {
          Document doc = new Document();
          int numVals = 1 + (d % 5);
          for (int v = 0; v < numVals; v++) {
            String t = termList.get((d * 3 + v) % termList.size());
            doc.add(new SortedSetDocValuesField("tags", new BytesRef(t)));
          }
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SortedSetDocValues dv = reader.leaves().get(0).reader().getSortedSetDocValues("tags");
        assertNotNull(dv);
        assertEquals(allTerms.size(), dv.getValueCount());
        // Verify sorted order
        List<String> expected = new ArrayList<>(allTerms);
        for (int ord = 0; ord < dv.getValueCount(); ord++) {
          assertEquals(
              "sortedset lookupOrd(" + ord + ")",
              expected.get(ord),
              dv.lookupOrd(ord).utf8ToString());
        }
        // next() iteration
        TermsEnum te = dv.termsEnum();
        int count = 0;
        BytesRef term;
        while ((term = te.next()) != null) {
          assertEquals(expected.get(count), term.utf8ToString());
          count++;
        }
        assertEquals(expected.size(), count);
      }
    }
  }

  private static Lucene104Codec fsstCodec() {
    Lucene90DocValuesFormat fsstDvFormat =
        new Lucene90DocValuesFormat(4096, Lucene90DocValuesFormat.TermsDictMode.FSST);
    return new Lucene104Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return fsstDvFormat;
      }
    };
  }
}
