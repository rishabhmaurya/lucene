package org.apache.lucene.codecs.lucene90.fsst;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Debug test for external sidecar directory with per-shard subdirs. */
public class TestFSSTSidecarExternal extends LuceneTestCase {

    public void testExternalSidecarLookup() throws Exception {
        // Setup: symbol table
        Path fsstBase = createTempDir("fsst-base");
        byte[] tableBytes = new byte[FSSTSymbolTable.MAX_SYMBOLS + FSSTSymbolTable.MAX_SYMBOLS * 8];
        for (int i = 0; i < FSSTSymbolTable.MAX_SYMBOLS; i++) {
            tableBytes[i] = 1;
            tableBytes[FSSTSymbolTable.MAX_SYMBOLS + i * 8] = (byte) i;
        }
        Files.write(fsstBase.resolve("url.fsst"), tableBytes);

        // Create index in a path that mimics OpenSearch: .../indices/uuid/0/index
        Path dataRoot = createTempDir("data");
        Path shardPath = dataRoot.resolve("indices").resolve("test-uuid").resolve("0").resolve("index");
        Files.createDirectories(shardPath);

        Directory dir = new MMapDirectory(shardPath);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        String[] urls = {"http://a.com/1", "http://b.com/2", "http://c.com/3"};
        for (String url : urls) {
            Document doc = new Document();
            doc.add(new SortedDocValuesField("url", new BytesRef(url)));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Build sidecar in external dir: fsstBase/sidecars/s0/
        Path sidecarDir = fsstBase.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);
        FSSTSidecarBuilder.main(new String[]{
            shardPath.toString(), fsstBase.toString(), sidecarDir.toString(), "url"
        });

        // Verify sidecar files exist
        assertTrue("fdvd should exist", Files.list(sidecarDir).anyMatch(p -> p.toString().endsWith(".fdvd")));

        // Set system property
        System.setProperty("opensearch.fsst.basePath", fsstBase.toString());
        try {
            // Reopen and check
            DirectoryReader reader = DirectoryReader.open(dir);
            LeafReader leaf = reader.leaves().get(0).reader();

            // Log the reader chain
            LeafReader r = leaf;
            StringBuilder chain = new StringBuilder(r.getClass().getSimpleName());
            while (r instanceof FilterLeafReader flr) {
                r = flr.getDelegate();
                chain.append(" -> ").append(r.getClass().getSimpleName());
            }
            System.out.println("Reader chain: " + chain);
            System.out.println("Final reader is CodecReader: " + (r instanceof CodecReader));

            // Get doc values directly from CodecReader
            if (r instanceof CodecReader cr) {
                SortedDocValues sdv = cr.getSortedDocValues("url");
                System.out.println("SortedDocValues type: " + (sdv != null ? sdv.getClass().getName() : "null"));
                System.out.println("Is FSSTCompressedAccess: " + (sdv instanceof FSSTCompressedAccess));
                if (sdv instanceof FSSTCompressedAccess fsst) {
                    System.out.println("hasCompressedAccess: " + fsst.hasCompressedAccess());
                    if (fsst.hasCompressedAccess()) {
                        // Test round-trip
                        for (int ord = 0; ord < sdv.getValueCount(); ord++) {
                            BytesRef compressed = fsst.lookupCompressedOrd(ord);
                            byte[] buf = new byte[256];
                            int len = fsst.decompress(compressed, buf);
                            BytesRef expected = sdv.lookupOrd(ord);
                            assertEquals(expected.utf8ToString(), new String(buf, 0, len));
                        }
                        System.out.println("FSST compressed access WORKS!");
                    }
                }
            }

            // Also test via FSSTAccessUtil
            FSSTCompressedAccess access = FSSTAccessUtil.getCompressedAccess(
                reader.leaves().get(0), "url");
            System.out.println("FSSTAccessUtil result: " + (access != null ? "AVAILABLE" : "NOT_AVAILABLE"));

            reader.close();
        } finally {
            System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }
}
