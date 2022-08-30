package org.apache.lucene.backward_codecs.lucene93Ext;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene93.Lucene93Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TSIntPoint;
import org.apache.lucene.document.TSPointQuery;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePointsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDSummaryWriter;
import org.apache.lucene.util.bkd.BKDWithSummaryReader;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

;import static org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;

public class TestLucene93ExtPointFormat extends BasePointsFormatTestCase {

    @Override
    protected Codec getCodec() {
        int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
        double maxMBSortInHeap = 3.0 + (3 * random().nextDouble());
        return new Lucene93ExtCodec(new Lucene93Codec(), maxPointsInLeafNode, maxMBSortInHeap);
    }

    public void testTSPointCount() throws IOException {
        Instant start = Instant.now();

        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        // Avoid mockRandomMP since it may cause non-optimal merges that make the
        // number of points per leaf hard to predict
        // while (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
        //    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        // }
        MergePolicy mergePolicy = NoMergePolicy.INSTANCE;
        iwc.setMergePolicy(mergePolicy);
        IndexWriter w = new IndexWriter(dir, iwc);

        final int numDocs = 100000;
        int low = random().nextInt(numDocs/100);
        int high = random().nextInt(low, numDocs - 1);
        int exp = 0;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            int measurement = random().nextInt(3);
            doc.add(new TSIntPoint("tsid1", "tsid1_cpu", i, measurement));
            if (i >= low && i <= high) {
                exp += measurement;
            }
            w.addDocument(doc);
        }


        w.flush();
        final IndexReader r = DirectoryReader.open(w);
        if (w.isOpen())
            w.close();
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        start = Instant.now();
        System.out.println("Indexing took: " + timeElapsed);
        // w.forceMerge(1);
        int tt = 100;
        while(tt-->0) {

            byte[] lowerPoint = new byte[8];
            byte[] upperPoint = new byte[8];

            NumericUtils.longToSortableBytes(low, lowerPoint, 0);
            NumericUtils.longToSortableBytes(high, upperPoint, 0);
            int actualCount = 0;
            int segments = 0, matches = 0;
            int diskAccess = 0;
            for (LeafReaderContext readerContext : r.leaves()) {
                final LeafReader lr = readerContext.reader();
                PointValues points = lr.getPointValues("tsid1");
                FieldInfo fieldInfo = lr.getFieldInfos().fieldInfo("tsid1");
                BKDSummaryWriter.SummaryMergeFunction<?> mergeFunction = fieldInfo.getMergeFunction();

                TSPointQuery tsPointQuery = new TSPointQuery("tsid1", lowerPoint, upperPoint);
                byte[] res = tsPointQuery.getSummary((BKDWithSummaryReader.BKDSummaryTree) points.getPointTree(), mergeFunction);
                if (res != null) {
                    actualCount += (Integer) mergeFunction.unpackBytes(res);
                }
                // lr.close();
                segments++;
                diskAccess += tsPointQuery.diskAccess[0];
            }
            assertEquals(exp, actualCount);

            // System.out.println("Matching docs count:" + actualCount + " | Segments:" + segments + " | DiskAccess: "
            //        + diskAccess);
        }
        r.close();
        finish = Instant.now();
        timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Search took: " + timeElapsed);
        //r.close();
        dir.close();
    }

    public void testPointCount() throws IOException {
        Instant start = Instant.now();

        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        // Avoid mockRandomMP since it may cause non-optimal merges that make the
        // number of points per leaf hard to predict
        // while (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
        //    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        // }
        MergePolicy mergePolicy = NoMergePolicy.INSTANCE;
        iwc.setMergePolicy(mergePolicy);
        IndexWriter w = new IndexWriter(dir, iwc);
        final int numDocs = 100000;
        int low = random().nextInt(numDocs/100);
        int high = random().nextInt(low, numDocs - 1);
        int exp = 0;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            int measurement = random().nextInt(3);
            doc.add(new TextField("tsid", "tsid1", Field.Store.NO));
            // doc.add(new LongPoint("timestamp", i));
            doc.add(new SortedNumericDocValuesField("timestamp", i));
            doc.add(new NumericDocValuesField("measurement", measurement));
            if (i >= low && i <= high) {
                exp += measurement;
            }
            w.addDocument(doc);
        }
        w.flush();
        final IndexReader reader = DirectoryReader.open(w);
        final IndexSearcher searcher = newSearcher(reader, false);
        if (w.isOpen())
            w.close();
        int tt = 100;
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        start = Instant.now();
        System.out.println("Indexing took: " + timeElapsed);
        while(tt-->0) {
            byte[] lowerPoint = new byte[8];
            byte[] upperPoint = new byte[8];

            NumericUtils.longToSortableBytes(low, lowerPoint, 0);
            NumericUtils.longToSortableBytes(high, upperPoint, 0);
            // final Query q = LongPoint.newRangeQuery("timestamp", low, high);
            final Query q = SortedNumericDocValuesField.newSlowRangeQuery("timestamp", low, high);

            final long[] actualRes = {0};
            final int[] match = {0};
            final int[] segments = {0};
            searcher.search(q, new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    final NumericDocValues values = context.reader().getNumericDocValues("measurement");
                    segments[0]++;
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) throws IOException {

                        }

                        @Override
                        public void collect(int doc) throws IOException {
                            match[0]++;
                            values.advance(doc);

                            actualRes[0] += values.longValue();
                        }
                    };
                }

                @Override
                public ScoreMode scoreMode() {
                    return COMPLETE_NO_SCORES;
                }
            });
            assertEquals(exp, actualRes[0]);

            // System.out.println("Matching docs count:" + match[0] + " | Segments:" + segments[0] + " | DiskAccess: "
            //         + match[0]);

        }
        reader.close();
        finish = Instant.now();
        timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Search took: " + timeElapsed);

        dir.close();

    }
}
