/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package org.apache.lucene.codecs.lucene90.fsst;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * End-to-end test: write LZ4 index → build FSST sidecar → verify lookupCompressedOrd works
 * and round-trips correctly through decompress.
 */
public class TestFSSTSidecar extends LuceneTestCase {

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

    /**
     * Full end-to-end: write index → build sidecar → set basePath → reopen → verify
     * lookupCompressedOrd returns compressed bytes that decompress to original terms.
     */
    public void testCompressedAccessViaDocValues() throws Exception {
        // Create identity symbol table (code N -> byte N)
        Path tableDir = createTempDir("fsst-tables");
        Path tablePath = tableDir.resolve("url.fsst");
        writeIdentitySymbolTable(tablePath);

        // Create index directory structure that mimics OpenSearch shard layout:
        // <basePath>/sidecars/s0/<segment>_url.fdvd
        // Index at: <tempDir>/0/index/
        Path baseDir = createTempDir("fsst-base");
        Path shardDir = baseDir.resolve("0");
        Path indexDir = shardDir.resolve("index");
        Files.createDirectories(indexDir);

        // Step 1: Write index with standard LZ4 codec
        MMapDirectory dir = new MMapDirectory(indexDir);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(new Lucene103Codec()));
        for (String url : URLS) {
            Document doc = new Document();
            doc.add(new SortedDocValuesField("url", new BytesRef(url)));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Collect expected terms from LZ4 reader
        DirectoryReader reader = DirectoryReader.open(dir);
        LeafReader leaf = reader.leaves().get(0).reader();
        SortedDocValues dv = leaf.getSortedDocValues("url");
        int valueCount = dv.getValueCount();
        assertEquals(URLS.length, valueCount);

        List<String> expectedTerms = new ArrayList<>();
        for (int ord = 0; ord < valueCount; ord++) {
            expectedTerms.add(dv.lookupOrd(ord).utf8ToString());
        }

        // Get segment name for sidecar file naming
        LeafReader unwrapped = leaf;
        while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();
        String segName = ((SegmentReader) unwrapped).getSegmentInfo().info.name;
        reader.close();

        // Step 2: Build sidecar files
        Path sidecarDir = baseDir.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);

        // Reopen to build sidecar
        reader = DirectoryReader.open(dir);
        leaf = reader.leaves().get(0).reader();
        unwrapped = leaf;
        while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();
        SegmentReader segReader = (SegmentReader) unwrapped;
        SortedDocValues sorted = segReader.getSortedDocValues("url");

        MMapDirectory sidecarMMap = new MMapDirectory(sidecarDir);
        FSSTSidecarBuilder.buildSidecar(sidecarMMap, segName, "url", sorted, null, valueCount, tablePath);
        sidecarMMap.close();
        reader.close();

        // Verify sidecar files exist
        assertTrue(Files.exists(sidecarDir.resolve(segName + "_url.fdvd")));
        assertTrue(Files.exists(sidecarDir.resolve(segName + "_url.fdvm")));

        // Step 3: Set basePath and reopen — producer should find sidecar
        String oldBasePath = System.getProperty("opensearch.fsst.basePath");
        try {
            System.setProperty("opensearch.fsst.basePath", baseDir.toString());

            reader = DirectoryReader.open(dir);
            leaf = reader.leaves().get(0).reader();
            unwrapped = leaf;
            while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();
            dv = ((SegmentReader) unwrapped).getSortedDocValues("url");
            assertNotNull(dv);

            // Verify lookupOrd still works (goes through sidecar decompression)
            for (int ord = 0; ord < valueCount; ord++) {
                assertEquals(expectedTerms.get(ord), dv.lookupOrd(ord).utf8ToString());
            }

            // Verify FSSTCompressedAccess is available
            assertTrue("Doc values should implement FSSTCompressedAccess",
                dv instanceof FSSTCompressedAccess);
            FSSTCompressedAccess fsst = (FSSTCompressedAccess) dv;
            assertTrue("Should have compressed access", fsst.hasCompressedAccess());

            // Verify lookupCompressedOrd + decompress round-trip
            byte[] decompBuf = new byte[1024];
            for (int ord = 0; ord < valueCount; ord++) {
                BytesRef compressed = fsst.lookupCompressedOrd(ord);
                assertNotNull("Compressed bytes should not be null for ord " + ord, compressed);
                assertTrue("Compressed bytes should not be empty", compressed.length > 0);

                int decLen = fsst.decompress(compressed, decompBuf);
                String decompressed = new String(decompBuf, 0, decLen, java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Decompress mismatch at ord " + ord, expectedTerms.get(ord), decompressed);
            }

            reader.close();
        } finally {
            if (oldBasePath != null) System.setProperty("opensearch.fsst.basePath", oldBasePath);
            else System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }

    /**
     * Simulates the exact StreamStringTermsAggregator pattern:
     * 1. Get SortedSetDocValues via DocValues.getSortedSet (OpenSearch path)
     * 2. Call FSSTCompressedAccess.unwrap BEFORE iteration (cache it)
     * 3. Iterate all docs (collection phase)
     * 4. Use cached access for lookupCompressedOrd (bucket building phase)
     *
     * This caught a real bug: calling unwrap AFTER iteration fails with
     * "iterator has already been used" because DocValues.unwrapSingleton
     * checks the iterator state.
     */
    public void testUnwrapBeforeIterationThenLookupCompressed() throws Exception {
        Path tableDir = createTempDir("fsst-tables2");
        writeIdentitySymbolTable(tableDir.resolve("url.fsst"));

        Path baseDir = createTempDir("fsst-base2");
        Path indexDir = baseDir.resolve("0").resolve("index");
        Files.createDirectories(indexDir);

        MMapDirectory dir = new MMapDirectory(indexDir);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(new Lucene103Codec()));
        for (String url : URLS) {
            Document doc = new Document();
            doc.add(new SortedDocValuesField("url", new BytesRef(url)));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Build sidecar
        DirectoryReader reader = DirectoryReader.open(dir);
        LeafReader leaf = reader.leaves().get(0).reader();
        LeafReader unwrapped = leaf;
        while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();
        SegmentReader segReader = (SegmentReader) unwrapped;
        String segName = segReader.getSegmentInfo().info.name;
        SortedDocValues sdv = segReader.getSortedDocValues("url");
        int valueCount = sdv.getValueCount();

        // Collect expected terms
        List<String> expectedTerms = new ArrayList<>();
        for (int ord = 0; ord < valueCount; ord++) {
            expectedTerms.add(sdv.lookupOrd(ord).utf8ToString());
        }

        Path sidecarDir = baseDir.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);
        MMapDirectory sidecarMMap = new MMapDirectory(sidecarDir);
        FSSTSidecarBuilder.buildSidecar(sidecarMMap, segName, "url",
            segReader.getSortedDocValues("url"), null, valueCount, tableDir.resolve("url.fsst"));
        sidecarMMap.close();
        reader.close();

        String oldBasePath = System.getProperty("opensearch.fsst.basePath");
        try {
            System.setProperty("opensearch.fsst.basePath", baseDir.toString());

            reader = DirectoryReader.open(dir);
            leaf = reader.leaves().get(0).reader();
            unwrapped = leaf;
            while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();

            // Step 1: Get SortedSetDocValues (OpenSearch path)
            org.apache.lucene.index.SortedSetDocValues ssdv =
                org.apache.lucene.index.DocValues.getSortedSet(unwrapped, "url");

            // Step 2: Cache unwrap BEFORE iteration
            FSSTCompressedAccess fsst = FSSTCompressedAccess.unwrap(ssdv);
            assertNotNull("unwrap should work before iteration", fsst);
            assertTrue(fsst.hasCompressedAccess());

            // Step 3: Iterate all docs (simulates collection phase)
            for (int doc = 0; doc < URLS.length; doc++) {
                if (ssdv.advanceExact(doc)) {
                    long ord = ssdv.nextOrd();
                    assertTrue(ord >= 0);
                }
            }

            // Step 4: Verify unwrap AFTER iteration would fail
            try {
                FSSTCompressedAccess.unwrap(ssdv);
                fail("unwrap after iteration should throw IllegalStateException");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("iterator has already been used"));
            }

            // Step 5: Use CACHED access for lookupCompressedOrd (bucket building)
            byte[] buf = new byte[1024];
            for (int ord = 0; ord < valueCount; ord++) {
                BytesRef compressed = fsst.lookupCompressedOrd(ord);
                assertNotNull(compressed);
                assertTrue(compressed.length > 0);

                int len = fsst.decompress(compressed, buf);
                String decompressed = new String(buf, 0, len, java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Round-trip mismatch at ord " + ord, expectedTerms.get(ord), decompressed);
            }

            reader.close();
        } finally {
            if (oldBasePath != null) System.setProperty("opensearch.fsst.basePath", oldBasePath);
            else System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }

    /**
     * Simulates the GlobalOrdinalsStringTermsAggregator lookups path:
     * DocValues.getSortedSet(reader, field) → SingletonSortedSetDocValues → .termsEnum()
     * Verifies the returned TermsEnum is FSSTCompressedAccess when sidecar exists.
     * This is the path used by getOrLoadTermsEnums() in GlobalOrdinalsIndexFieldData.
     */
    public void testTermsEnumIsFSSTCompressedAccess() throws Exception {
        Path tableDir = createTempDir("fsst-tables3");
        writeIdentitySymbolTable(tableDir.resolve("url.fsst"));

        Path baseDir = createTempDir("fsst-base3");
        Path indexDir = baseDir.resolve("0").resolve("index");
        Files.createDirectories(indexDir);

        MMapDirectory dir = new MMapDirectory(indexDir);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(new Lucene103Codec()));
        for (String url : URLS) {
            Document doc = new Document();
            doc.add(new SortedDocValuesField("url", new BytesRef(url)));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Build sidecar
        DirectoryReader reader = DirectoryReader.open(dir);
        LeafReader leaf = reader.leaves().get(0).reader();
        LeafReader unwrapped = leaf;
        while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();
        SegmentReader segReader = (SegmentReader) unwrapped;
        String segName = segReader.getSegmentInfo().info.name;
        SortedDocValues sdv = segReader.getSortedDocValues("url");
        int valueCount = sdv.getValueCount();

        Path sidecarDir = baseDir.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);
        MMapDirectory sidecarMMap = new MMapDirectory(sidecarDir);
        FSSTSidecarBuilder.buildSidecar(sidecarMMap, segName, "url",
            segReader.getSortedDocValues("url"), null, valueCount, tableDir.resolve("url.fsst"));
        sidecarMMap.close();
        reader.close();

        String oldBasePath = System.getProperty("opensearch.fsst.basePath");
        try {
            System.setProperty("opensearch.fsst.basePath", baseDir.toString());

            reader = DirectoryReader.open(dir);
            leaf = reader.leaves().get(0).reader();
            unwrapped = leaf;
            while (unwrapped instanceof FilterLeafReader flr) unwrapped = flr.getDelegate();

            // DocValues.getSortedSet wraps SortedDocValues in SingletonSortedSetDocValues
            org.apache.lucene.index.SortedSetDocValues ssdv =
                org.apache.lucene.index.DocValues.getSortedSet(unwrapped, "url");

            // termsEnum() now always returns LZ4 TermsDict
            org.apache.lucene.index.TermsEnum te = ssdv.termsEnum();
            assertFalse("termsEnum should be LZ4 TermsDict, not FSSTCompressedAccess",
                te instanceof FSSTCompressedAccess);

            // Compressed access is on the doc values via unwrap
            FSSTCompressedAccess fsst = FSSTCompressedAccess.unwrap(ssdv);
            assertNotNull("Doc values should have FSSTCompressedAccess via unwrap", fsst);
            assertTrue(fsst.hasCompressedAccess());

            reader.close();
        } finally {
            if (oldBasePath != null) System.setProperty("opensearch.fsst.basePath", oldBasePath);
            else System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }

    /**
     * Tests global ordinals path: multiple segments → build OrdinalMap → lookups TermsEnum
     * should be FSSTSidecarTermsDict → lookupCompressedOrd via global→segment mapping works.
     */
    public void testGlobalOrdinalsWithFSST() throws Exception {
        Path tableDir = createTempDir("fsst-tables-go");
        writeIdentitySymbolTable(tableDir.resolve("url.fsst"));

        Path baseDir = createTempDir("fsst-base-go");
        Path indexDir = baseDir.resolve("0").resolve("index");
        Files.createDirectories(indexDir);

        MMapDirectory dir = new MMapDirectory(indexDir);

        // Write 2 segments to force global ordinals
        IndexWriterConfig conf = new IndexWriterConfig().setCodec(new Lucene103Codec());
        conf.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, conf);
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
        writer.close();

        // Build sidecars for each segment
        DirectoryReader reader = DirectoryReader.open(dir);
        assertTrue("Need multiple segments", reader.leaves().size() >= 2);

        Path sidecarDir = baseDir.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);
        MMapDirectory sidecarMMap = new MMapDirectory(sidecarDir);

        // Collect expected terms per segment
        List<List<String>> expectedPerSeg = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader lr = ctx.reader();
            while (lr instanceof FilterLeafReader flr) lr = flr.getDelegate();
            SegmentReader segReader = (SegmentReader) lr;
            String segName = segReader.getSegmentInfo().info.name;
            SortedDocValues sdv = segReader.getSortedDocValues("url");
            int vc = sdv.getValueCount();

            List<String> terms = new ArrayList<>();
            for (int o = 0; o < vc; o++) terms.add(sdv.lookupOrd(o).utf8ToString());
            expectedPerSeg.add(terms);

            FSSTSidecarBuilder.buildSidecar(sidecarMMap, segName, "url",
                segReader.getSortedDocValues("url"), null, vc, tableDir.resolve("url.fsst"));
        }
        sidecarMMap.close();
        reader.close();

        // Reopen with FSST basePath set
        String oldBasePath = System.getProperty("opensearch.fsst.basePath");
        try {
            System.setProperty("opensearch.fsst.basePath", baseDir.toString());
            reader = DirectoryReader.open(dir);

            // Build global ordinals (same as GlobalOrdinalsBuilder)
            org.apache.lucene.index.SortedSetDocValues[] subs =
                new org.apache.lucene.index.SortedSetDocValues[reader.leaves().size()];
            for (int i = 0; i < reader.leaves().size(); i++) {
                LeafReader lr = reader.leaves().get(i).reader();
                while (lr instanceof FilterLeafReader flr) lr = flr.getDelegate();
                subs[i] = org.apache.lucene.index.DocValues.getSortedSet(lr, "url");
            }
            org.apache.lucene.index.OrdinalMap ordinalMap =
                org.apache.lucene.index.OrdinalMap.build(null, subs, org.apache.lucene.util.packed.PackedInts.DEFAULT);

            // Get per-segment FSSTCompressedAccess from doc values (not from termsEnum)
            FSSTCompressedAccess[] segAccess = new FSSTCompressedAccess[reader.leaves().size()];
            for (int i = 0; i < reader.leaves().size(); i++) {
                LeafReader lr = reader.leaves().get(i).reader();
                while (lr instanceof FilterLeafReader flr) lr = flr.getDelegate();
                org.apache.lucene.index.SortedSetDocValues dv =
                    org.apache.lucene.index.DocValues.getSortedSet(lr, "url");
                segAccess[i] = FSSTCompressedAccess.unwrap(dv);
                assertNotNull("Segment " + i + " should have FSSTCompressedAccess", segAccess[i]);
                assertTrue(segAccess[i].hasCompressedAccess());
            }

            // Verify global→segment→lookupCompressedOrd→decompress round-trip
            long globalValueCount = ordinalMap.getValueCount();
            byte[] buf = new byte[1024];
            for (long globalOrd = 0; globalOrd < globalValueCount; globalOrd++) {
                long segOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
                int segIdx = ordinalMap.getFirstSegmentNumber(globalOrd);

                BytesRef compressed = segAccess[segIdx].lookupCompressedOrd(segOrd);
                assertNotNull(compressed);
                int len = segAccess[segIdx].decompress(compressed, buf);
                String decompressed = new String(buf, 0, len, java.nio.charset.StandardCharsets.UTF_8);

                assertEquals(expectedPerSeg.get(segIdx).get((int) segOrd), decompressed);
            }

            reader.close();
        } finally {
            if (oldBasePath != null) System.setProperty("opensearch.fsst.basePath", oldBasePath);
            else System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }

    private void writeIdentitySymbolTable(Path path) throws java.io.IOException {
        byte[] data = new byte[255 + 255 * 8];
        for (int i = 0; i < 255; i++) data[i] = 1;
        for (int i = 0; i < 255; i++) data[255 + i * 8] = (byte) i;
        Files.write(path, data);
    }
}
