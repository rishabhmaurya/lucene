package org.apache.lucene.codecs.lucene90.fsst;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Test FSST compress/decompress with real symbol table from C tool. */
public class TestFSSTRealTable extends LuceneTestCase {

    static final String[] TEST_URLS = {
        "http://example.com/page/1",
        "http://other.ru/catalog/item/100",
        "http://video.yandex.ru/search?text=music",
        "http://shop.example.com/product/shoes",
        "http://news.example.com/article/2024/01",
        "http://mail.ru/inbox/message/12345",
        "http://liver.ru/belgorod/page/1006",
        "http://kinopoisk.ru",
        "http://smeshariki.ru/region",
    };

    public void testRoundTripWithRealTable() throws Exception {
        Path tablePath = Path.of("/tmp/real_url_table.fsst");
        if (!Files.exists(tablePath)) {
            System.out.println("Skipping: /tmp/real_url_table.fsst not found");
            return;
        }

        FSSTSymbolTable table = FSSTSymbolTable.load(tablePath);
        FSSTCompressor compressor = new FSSTCompressor(table);
        FSSTDecompressor decompressor = new FSSTDecompressor(table);

        // Print some symbols for debugging
        int symCount = 0;
        for (int i = 0; i < FSSTSymbolTable.MAX_SYMBOLS; i++) {
            if (table.symbolLength(i) > 0) symCount++;
        }
        System.out.println("Symbols loaded: " + symCount);
        // Print first 10 multi-byte symbols
        int printed = 0;
        for (int i = 0; i < FSSTSymbolTable.MAX_SYMBOLS && printed < 10; i++) {
            int len = table.symbolLength(i);
            if (len > 1) {
                byte[] sym = new byte[len];
                table.symbolBytes(i, sym, 0);
                System.out.println("  code " + i + " (len=" + len + "): \""
                    + new String(sym, 0, len, java.nio.charset.StandardCharsets.UTF_8) + "\"");
                printed++;
            }
        }

        byte[] compressBuf = new byte[4096];
        byte[] decompressBuf = new byte[4096];

        for (String url : TEST_URLS) {
            byte[] input = url.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            // Compress
            int compLen = compressor.compress(input, 0, input.length, compressBuf);
            assertTrue("Compressed length should be > 0 for: " + url, compLen > 0);

            // Decompress
            int decLen = decompressor.decompress(compressBuf, 0, compLen, decompressBuf);

            // Verify
            String result = new String(decompressBuf, 0, decLen, java.nio.charset.StandardCharsets.UTF_8);
            System.out.println("URL: " + url);
            System.out.println("  raw=" + input.length + " compressed=" + compLen + " decompressed=" + decLen);
            if (!url.equals(result)) {
                System.out.println("  MISMATCH! got: " + result);
                // Print hex of first few bytes
                StringBuilder sb = new StringBuilder("  compressed hex: ");
                for (int i = 0; i < Math.min(compLen, 20); i++) {
                    sb.append(String.format("%02x ", compressBuf[i] & 0xFF));
                }
                System.out.println(sb);
            }
            assertEquals("Round-trip failed for: " + url, url, result);
        }
    }
}
