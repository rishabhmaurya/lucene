/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package org.apache.lucene.codecs.lucene90.fsst;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for FSST-based term dictionary encoding. */
public class TestFSSTDocValues extends LuceneTestCase {

  // Sample URLs similar to clickbench data
  static final String[] SAMPLE_URLS = {
    "http://example.com/page/1",
    "http://example.com/page/2",
    "http://example.com/page/3",
    "http://example.com/search?q=test",
    "http://example.com/search?q=hello",
    "http://example.com/search?q=world",
    "http://other.ru/catalog/item/100",
    "http://other.ru/catalog/item/200",
    "http://other.ru/catalog/item/300",
    "http://shop.example.com/product/shoes",
    "http://shop.example.com/product/shirt",
    "http://shop.example.com/product/pants",
    "http://news.example.com/article/2024/01",
    "http://news.example.com/article/2024/02",
    "http://news.example.com/article/2024/03",
    "http://video.yandex.ru/search?text=music",
    "http://video.yandex.ru/search?text=movie",
    "http://video.yandex.ru/search?text=sport",
    "http://mail.ru/inbox/message/12345",
    "http://mail.ru/inbox/message/67890",
  };

  private Path symbolTablePath;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    symbolTablePath = buildSymbolTable();
    System.setProperty(Lucene90DocValuesFormat.FSST_SYMBOL_TABLE_PATH_PROP,
        symbolTablePath.toString());
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty(Lucene90DocValuesFormat.FSST_SYMBOL_TABLE_PATH_PROP);
    Files.deleteIfExists(symbolTablePath);
    super.tearDown();
  }

  /**
   * Build a minimal but correct symbol table. Every byte gets a 1-byte symbol (identity mapping)
   * so compression is lossless. No multi-byte symbols — purely for correctness testing.
   */
  private Path buildSymbolTable() throws IOException {
    byte[] tableBytes = new byte[FSSTSymbolTable.MAX_SYMBOLS + FSSTSymbolTable.MAX_SYMBOLS * 8];
    for (int i = 0; i < FSSTSymbolTable.MAX_SYMBOLS; i++) {
      tableBytes[i] = 1; // length = 1
      tableBytes[FSSTSymbolTable.MAX_SYMBOLS + i * 8] = (byte) i; // symbol = the byte itself
    }
    Path path = createTempFile("fsst-table", ".bin");
    Files.write(path, tableBytes);
    return path;
  }

  /** Test basic FSST compress/decompress round-trip. */
  public void testCompressDecompressRoundTrip() throws Exception {
    FSSTSymbolTable table = FSSTSymbolTable.load(symbolTablePath);
    FSSTCompressor compressor = new FSSTCompressor(table);
    FSSTDecompressor decompressor = new FSSTDecompressor(table);

    for (String url : SAMPLE_URLS) {
      byte[] input = url.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] compressed = new byte[input.length * 2];
      int compLen = compressor.compress(input, 0, input.length, compressed);

      // With identity table, compressed length equals input length (no multi-byte symbols)
      assertEquals("Identity table should produce same length", input.length, compLen);

      byte[] decompressed = new byte[input.length + 64];
      int decLen = decompressor.decompress(compressed, 0, compLen, decompressed);

      assertEquals("Decompressed length mismatch for: " + url, input.length, decLen);
      for (int i = 0; i < input.length; i++) {
        assertEquals("Byte mismatch at position " + i + " for: " + url,
            input[i], decompressed[i]);
      }
    }
  }

  /** Test FSST with SortedDocValues — write and read back via lookupOrd. */
  public void testSortedDocValuesFSST() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, conf);

    for (int i = 0; i < SAMPLE_URLS.length; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("url", new BytesRef(SAMPLE_URLS[i])));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leaf = reader.leaves().get(0).reader();
    SortedDocValues dv = leaf.getSortedDocValues("url");
    assertNotNull(dv);

    // Verify all terms can be looked up by ordinal
    int valueCount = dv.getValueCount();
    assertTrue("Expected multiple unique values", valueCount > 1);

    // Collect all terms via lookupOrd
    List<String> terms = new ArrayList<>();
    for (int ord = 0; ord < valueCount; ord++) {
      BytesRef term = dv.lookupOrd(ord);
      terms.add(term.utf8ToString());
    }

    // Terms should be sorted
    for (int i = 1; i < terms.size(); i++) {
      assertTrue("Terms not sorted at index " + i,
          terms.get(i - 1).compareTo(terms.get(i)) < 0);
    }

    // All sample URLs should be present
    for (String url : SAMPLE_URLS) {
      assertTrue("Missing URL: " + url, terms.contains(url));
    }

    // Verify doc -> ord -> term round-trip
    for (int docId = 0; docId < SAMPLE_URLS.length; docId++) {
      assertTrue(dv.advanceExact(docId));
      int ord = dv.ordValue();
      BytesRef term = dv.lookupOrd(ord);
      // The term should be one of our sample URLs
      assertTrue("Unexpected term: " + term.utf8ToString(),
          terms.contains(term.utf8ToString()));
    }

    reader.close();
    dir.close();
  }

  /** Test compressed access API — covered by TestFSSTSidecar, skip here without sidecar. */
  public void testCompressedAccessAPI() throws Exception {
    // Compressed access requires sidecar files — see TestFSSTSidecar for full test
  }

  /** Test SortedSetDocValues with FSST — multi-valued field. */
  public void testSortedSetDocValuesFSST() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, conf);

    // Each doc gets 2-3 URLs
    for (int i = 0; i < SAMPLE_URLS.length; i++) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesField("urls", new BytesRef(SAMPLE_URLS[i])));
      doc.add(new SortedSetDocValuesField("urls",
          new BytesRef(SAMPLE_URLS[(i + 1) % SAMPLE_URLS.length])));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leaf = reader.leaves().get(0).reader();
    SortedSetDocValues dv = leaf.getSortedSetDocValues("urls");
    assertNotNull(dv);

    long valueCount = dv.getValueCount();
    assertTrue("Expected multiple unique values", valueCount > 1);

    // Verify all ordinals resolve correctly
    for (long ord = 0; ord < valueCount; ord++) {
      BytesRef term = dv.lookupOrd(ord);
      assertNotNull(term);
      assertTrue("Empty term at ord " + ord, term.length > 0);
    }

    // Verify compressed access on SortedSetDocValues (may not be available if wrapped)
    if (dv instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
      byte[] decompBuf = new byte[1024];
      for (long ord = 0; ord < valueCount; ord++) {
        BytesRef compressed = fsst.lookupCompressedOrd(ord);
        int decLen = fsst.decompress(compressed, decompBuf);
        BytesRef expected = dv.lookupOrd(ord);
        assertEquals(expected.length, decLen);
      }
    }

    reader.close();
    dir.close();
  }

  /**
   * Test that the same symbol table works across multiple segments (simulating shared table across
   * shards). Write two segments, verify both can be read with the same symbol table.
   */
  public void testSharedSymbolTableAcrossSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig();
    conf.setMaxBufferedDocs(10); // force multiple segments
    IndexWriter writer = new IndexWriter(dir, conf);

    // Write docs that will span multiple segments
    for (int i = 0; i < SAMPLE_URLS.length; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("url", new BytesRef(SAMPLE_URLS[i])));
      writer.addDocument(doc);
      if (i == SAMPLE_URLS.length / 2) {
        writer.flush(); // force a segment boundary
      }
    }
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    assertTrue("Expected multiple segments", reader.leaves().size() >= 2);

    // Verify each segment works independently with the shared symbol table
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      SortedDocValues dv = leaf.getSortedDocValues("url");
      if (dv == null) continue;

      int valueCount = dv.getValueCount();
      for (int ord = 0; ord < valueCount; ord++) {
        BytesRef term = dv.lookupOrd(ord);
        assertNotNull(term);
        assertTrue(term.length > 0);
      }

      // Compressed access works per segment
      if (dv instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
        byte[] buf = new byte[1024];
        for (int ord = 0; ord < valueCount; ord++) {
          BytesRef compressed = fsst.lookupCompressedOrd(ord);
          int decLen = fsst.decompress(compressed, buf);
          BytesRef expected = dv.lookupOrd(ord);
          assertEquals(expected.length, decLen);
        }
      }
    }

    reader.close();
    dir.close();
  }

  /** Test seekCeil works correctly with FSST encoding. */
  public void testSeekCeil() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, conf);

    for (String url : SAMPLE_URLS) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("url", new BytesRef(url)));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leaf = reader.leaves().get(0).reader();
    SortedDocValues dv = leaf.getSortedDocValues("url");

    // lookupTerm for exact match
    int ord = dv.lookupTerm(new BytesRef("http://example.com/page/1"));
    assertTrue("Should find exact term", ord >= 0);
    assertEquals("http://example.com/page/1", dv.lookupOrd(ord).utf8ToString());

    // lookupTerm for non-existent term (should return insertion point)
    int missing = dv.lookupTerm(new BytesRef("http://example.com/page/1a"));
    assertTrue("Should return negative for missing term", missing < 0);

    reader.close();
    dir.close();
  }
}
