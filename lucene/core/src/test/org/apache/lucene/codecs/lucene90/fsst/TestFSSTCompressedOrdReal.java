package org.apache.lucene.codecs.lucene90.fsst;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Test lookupCompressedOrd with real symbol table on SortedSetDocValues (keyword field). */
public class TestFSSTCompressedOrdReal extends LuceneTestCase {

    static final String[] URLS = {
        "http://example.com/page/1",
        "http://example.com/page/2",
        "http://other.ru/catalog/item/100",
        "http://other.ru/catalog/item/200",
        "http://video.yandex.ru/search?text=music",
        "http://shop.example.com/product/shoes",
        "http://news.example.com/article/2024/01",
        "http://kinopoisk.ru",
        "http://smeshariki.ru/region",
        "http://liver.ru/belgorod/page/1006",
    };

    public void testLookupCompressedOrdWithSortedSet() throws Exception {
        Path tablePath = Path.of("/tmp/real_url_table.fsst");
        if (!Files.exists(tablePath)) {
            System.out.println("Skipping: /tmp/real_url_table.fsst not found");
            return;
        }

        // Setup
        Path fsstBase = createTempDir("fsst-base");
        Files.copy(tablePath, fsstBase.resolve("url.fsst"));

        Path dataRoot = createTempDir("data");
        Path shardPath = dataRoot.resolve("indices").resolve("uuid").resolve("0").resolve("index");
        Files.createDirectories(shardPath);

        // Index as SortedSetDocValues (keyword field behavior)
        MMapDirectory dir = new MMapDirectory(shardPath);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        for (String url : URLS) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("url", new BytesRef(url)));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Build sidecar
        Path sidecarDir = fsstBase.resolve("sidecars").resolve("s0");
        Files.createDirectories(sidecarDir);
        FSSTSidecarBuilder.main(new String[]{
            shardPath.toString(), fsstBase.toString(), sidecarDir.toString(), "url"
        });

        // Set system property
        System.setProperty("opensearch.fsst.basePath", fsstBase.toString());
        try {
            DirectoryReader reader = DirectoryReader.open(dir);
            LeafReader leaf = reader.leaves().get(0).reader();

            // Get SortedSetDocValues (like keyword field)
            SortedSetDocValues ssdv = leaf.getSortedSetDocValues("url");
            assertNotNull("Should have SortedSetDocValues", ssdv);
            long valueCount = ssdv.getValueCount();
            System.out.println("ValueCount: " + valueCount);

            // Get compressed access via FSSTAccessUtil
            FSSTCompressedAccess access = FSSTAccessUtil.getCompressedAccess(
                reader.leaves().get(0), "url");
            System.out.println("FSSTAccess: " + (access != null ? "AVAILABLE" : "NOT_AVAILABLE"));

            if (access != null && access.hasCompressedAccess()) {
                byte[] decompBuf = new byte[4096];
                FSSTDecompressor decompressor = new FSSTDecompressor(FSSTSymbolTable.load(tablePath));

                for (long ord = 0; ord < valueCount; ord++) {
                    // Get compressed bytes
                    BytesRef compressed = access.lookupCompressedOrd(ord);
                    assertNotNull("Compressed should not be null for ord " + ord, compressed);

                    // Decompress
                    int decLen = decompressor.decompress(
                        compressed.bytes, compressed.offset, compressed.length, decompBuf);
                    String decompressed = new String(decompBuf, 0, decLen, java.nio.charset.StandardCharsets.UTF_8);

                    // Compare with regular lookupOrd
                    BytesRef expected = ssdv.lookupOrd(ord);
                    String expectedStr = expected.utf8ToString();

                    System.out.println("ord=" + ord + " expected=\"" + expectedStr
                        + "\" compressed=" + compressed.length + " decompressed=\"" + decompressed + "\"");
                    assertEquals("Mismatch at ord " + ord, expectedStr, decompressed);
                }
                System.out.println("ALL ORDS PASSED!");
            }

            reader.close();
        } finally {
            System.clearProperty("opensearch.fsst.basePath");
        }
        dir.close();
    }
}
