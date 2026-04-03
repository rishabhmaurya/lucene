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
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
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
                dv.lookupOrd(ord).utf8ToString(),
                new String(buf, 0, len, StandardCharsets.UTF_8));
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
                dv.lookupOrd(ord).utf8ToString(),
                new String(buf, 0, len, StandardCharsets.UTF_8));
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
