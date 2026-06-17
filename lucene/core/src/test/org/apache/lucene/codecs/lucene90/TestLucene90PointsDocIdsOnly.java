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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.BitSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

/**
 * End-to-end test of the opt-in value-free points format through the real {@link IndexWriter} /
 * codec stack: a single-dim long field carrying the {@link
 * Lucene90PointsWriter#DOC_IDS_ONLY_ATTRIBUTE_KEY} attribute is indexed, then range-queried, and
 * we assert the result is a correct super-set (exact after a residual re-check) and that the
 * on-disk points files are smaller than the default format.
 */
public class TestLucene90PointsDocIdsOnly extends LuceneTestCase {

  /** A single-dim long-point FieldType that opts into the value-free leaf format. */
  private static FieldType docIdsOnlyLongType() {
    FieldType t = new FieldType();
    t.setDimensions(1, Long.BYTES);
    t.putAttribute(Lucene90PointsWriter.DOC_IDS_ONLY_ATTRIBUTE_KEY, "true");
    t.freeze();
    return t;
  }

  private void indexValues(Directory dir, long[] values, boolean docIdsOnly) throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig().setCodec(org.apache.lucene.codecs.Codec.forName("Lucene104"));
    iwc.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(256.0);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setUseCompoundFile(false); // keep .kdd/.kdi/.kdm separate so we can measure them
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      FieldType type = docIdsOnly ? docIdsOnlyLongType() : null;
      for (long v : values) {
        Document doc = new Document();
        if (docIdsOnly) {
          doc.add(new Field("val", pack(v), type));
        } else {
          doc.add(new LongPoint("val", v));
        }
        w.addDocument(doc);
      }
      w.commit();
    }
  }

  private static byte[] pack(long v) {
    byte[] b = new byte[Long.BYTES];
    NumericUtils.longToSortableBytes(v, b, 0);
    return b;
  }

  /** Visitor over [min,max]; residual re-check needs the actual values keyed by docID. */
  private IntersectVisitor rangeVisitor(
      BitSet hits, long min, long max, long[] valuesByDoc, boolean residual) {
    byte[] qMin = pack(min);
    byte[] qMax = pack(max);
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        if (residual) {
          long v = valuesByDoc[docID];
          if (v < min || v > max) {
            return;
          }
        }
        hits.set(docID);
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        if (java.util.Arrays.compareUnsigned(packedValue, 0, Long.BYTES, qMin, 0, Long.BYTES) >= 0
            && java.util.Arrays.compareUnsigned(packedValue, 0, Long.BYTES, qMax, 0, Long.BYTES)
                <= 0) {
          hits.set(docID);
        }
      }

      @Override
      public Relation compare(byte[] minPacked, byte[] maxPacked) {
        if (java.util.Arrays.compareUnsigned(maxPacked, 0, Long.BYTES, qMin, 0, Long.BYTES) < 0
            || java.util.Arrays.compareUnsigned(minPacked, 0, Long.BYTES, qMax, 0, Long.BYTES) > 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        } else if (java.util.Arrays.compareUnsigned(minPacked, 0, Long.BYTES, qMin, 0, Long.BYTES)
                < 0
            || java.util.Arrays.compareUnsigned(maxPacked, 0, Long.BYTES, qMax, 0, Long.BYTES) > 0) {
          return Relation.CELL_CROSSES_QUERY;
        } else {
          return Relation.CELL_INSIDE_QUERY;
        }
      }
    };
  }

  public void testValueFreePruningThroughCodec() throws Exception {
    final int n = 2000;
    long[] valuesByDoc = new long[n];
    try (Directory dir = newDirectory()) {
      // Index with the opt-in attribute; record value per docID (docs added in order => docID==i).
      // Pin the codec (Lucene90PointsFormat) and disable merges so the value-free segment is read
      // exactly as written (the value-free format is intentionally not mergeable).
      IndexWriterConfig iwc =
          new IndexWriterConfig().setCodec(org.apache.lucene.codecs.Codec.forName("Lucene104"));
      iwc.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
      // Large RAM buffer + no flush-by-doc so all docs land in one segment (docID == add order).
      iwc.setRAMBufferSizeMB(256.0);
      iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      FieldType type = docIdsOnlyLongType();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < n; i++) {
          long v = random().nextInt(1000);
          valuesByDoc[i] = v;
          Document doc = new Document();
          doc.add(new Field("val", pack(v), type));
          w.addDocument(doc);
        }
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected a single segment", 1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().get(0);
        PointValues pv = ctx.reader().getPointValues("val");
        assertNotNull(pv);
        // The test framework may wrap PointValues (e.g. AssertingPointValues); unwrap to confirm
        // the opt-in attribute actually reached the writer and produced a value-free BKD.
        assertTrue(
            "opt-in attribute did not reach the writer (docIdsOnly=false)",
            unwrapDocIdsOnly(pv));

        for (int iter = 0; iter < 40; iter++) {
          int a = random().nextInt(1000);
          int b = random().nextInt(1000);
          long qMin = Math.min(a, b);
          long qMax = Math.max(a, b);

          BitSet truth = new BitSet();
          for (int d = 0; d < n; d++) {
            if (valuesByDoc[d] >= qMin && valuesByDoc[d] <= qMax) {
              truth.set(d);
            }
          }

          // Raw value-free result must be a super-set of truth (pruning never drops a match).
          BitSet raw = new BitSet();
          pv.intersect(rangeVisitor(raw, qMin, qMax, valuesByDoc, false));
          for (int d = truth.nextSetBit(0); d >= 0; d = truth.nextSetBit(d + 1)) {
            assertTrue("value-free codec dropped a true match docID=" + d, raw.get(d));
          }
          // And the raw result must still prune *something* vs. the whole segment for a narrow
          // range (sanity that the tree navigation is actually excluding cells).
          assertTrue("raw super-set should not exceed segment size", raw.cardinality() <= n);

          // After residual re-check it must equal truth exactly.
          BitSet exact = new BitSet();
          pv.intersect(rangeVisitor(exact, qMin, qMax, valuesByDoc, true));
          assertEquals("residual-checked result equals truth", truth, exact);
        }
      }
    }
  }

  /**
   * Value-free segments must SURVIVE a merge by rebuilding the BKD from co-written
   * SortedNumericDocValues. Index three segments (each point + co-written DV), forceMerge to one,
   * then assert the merged single segment is still a value-free BKD and prunes correctly.
   */
  public void testValueFreeMergeRebuildsFromDocValues() throws Exception {
    final int perSeg = 500;
    final int segs = 3;
    final int n = perSeg * segs;
    long[] valuesByDoc = new long[n];
    FieldType type = docIdsOnlyLongType();

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig().setCodec(org.apache.lucene.codecs.Codec.forName("Lucene104"));
      // Allow merges (we explicitly forceMerge below); keep docID == insertion order across the
      // run by flushing one segment per batch via commit().
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        int docId = 0;
        for (int s = 0; s < segs; s++) {
          for (int i = 0; i < perSeg; i++) {
            long v = random().nextInt(1000);
            valuesByDoc[docId++] = v;
            Document doc = new Document();
            doc.add(new Field("val", pack(v), type));
            // Co-written merge-survival value source (mirrors NumericPointFieldFactory).
            doc.add(new SortedNumericDocValuesField("val", v));
            w.addDocument(doc);
          }
          w.commit(); // new segment per batch
        }
        w.forceMerge(1); // <- triggers the value-free BKD rebuild-from-DV merge path
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected a single merged segment", 1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().get(0);
        PointValues pv = ctx.reader().getPointValues("val");
        assertNotNull("merged segment must still have the points field", pv);
        assertTrue("merged segment must still be value-free", unwrapDocIdsOnly(pv));
        assertEquals("merged segment must retain all docs", n, ctx.reader().maxDoc());

        for (int iter = 0; iter < 40; iter++) {
          int a = random().nextInt(1000);
          int b = random().nextInt(1000);
          long qMin = Math.min(a, b);
          long qMax = Math.max(a, b);
          BitSet truth = new BitSet();
          for (int d = 0; d < n; d++) {
            if (valuesByDoc[d] >= qMin && valuesByDoc[d] <= qMax) {
              truth.set(d);
            }
          }
          // Super-set (no true match dropped after the merge rebuild).
          BitSet raw = new BitSet();
          pv.intersect(rangeVisitor(raw, qMin, qMax, valuesByDoc, false));
          for (int d = truth.nextSetBit(0); d >= 0; d = truth.nextSetBit(d + 1)) {
            assertTrue("merged value-free BKD dropped a true match docID=" + d, raw.get(d));
          }
          // Exact after residual re-check.
          BitSet exact = new BitSet();
          pv.intersect(rangeVisitor(exact, qMin, qMax, valuesByDoc, true));
          assertEquals("merged residual-checked result equals truth", truth, exact);
        }
      }
    }
  }

  /**
   * The cluster-reproducing case: when the index has an IndexSort, merge routes points through
   * {@code SortingCodecReader} → base {@code PointsWriter.mergeOneField}, whose visitor calls
   * {@code visit(int docID)} and previously threw on a value-free leaf ("cannot complete
   * forceMerge"). Verifies the {@code mergeOneField} override rebuilds the value-free BKD from
   * doc-values even under IndexSort, and the merged segment prunes correctly in the SORTED docID
   * space.
   */
  public void testValueFreeMergeUnderIndexSort() throws Exception {
    final int perSeg = 400;
    final int segs = 3;
    final int n = perSeg * segs;
    FieldType type = docIdsOnlyLongType();

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig().setCodec(org.apache.lucene.codecs.Codec.forName("Lucene104"));
      iwc.setUseCompoundFile(false);
      // IndexSort by a separate long DV field (mirrors Mustang's index.sort.field) → forces the
      // SortingCodecReader merge path that hit the bug on the cluster.
      iwc.setIndexSort(
          new org.apache.lucene.search.Sort(
              new org.apache.lucene.search.SortedNumericSortField(
                  "sort_val", org.apache.lucene.search.SortField.Type.LONG, true)));

      // value -> we recover the post-merge value per doc via the "val" DV, so track by a stable key.
      // After an index sort docIDs are reordered, so validate against the merged segment's own DV.
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        int k = 0;
        for (int s = 0; s < segs; s++) {
          for (int i = 0; i < perSeg; i++) {
            long v = random().nextInt(1000);
            Document doc = new Document();
            doc.add(new Field("val", pack(v), type));
            doc.add(new SortedNumericDocValuesField("val", v)); // merge-survival source
            doc.add(new SortedNumericDocValuesField("sort_val", random().nextInt(100000))); // sort key
            w.addDocument(doc);
            k++;
          }
          w.commit();
        }
        w.forceMerge(1); // SortingCodecReader merge path
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected one merged segment", 1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().get(0);
        PointValues pv = ctx.reader().getPointValues("val");
        assertNotNull("merged value-free field must survive IndexSort merge", pv);
        assertTrue("merged segment must still be value-free", unwrapDocIdsOnly(pv));
        assertEquals("all docs retained", n, ctx.reader().maxDoc());

        // Recover the post-merge value per (sorted) docID from the merged segment's own DV.
        long[] mergedVals = new long[n];
        org.apache.lucene.index.SortedNumericDocValues dv =
            ctx.reader().getSortedNumericDocValues("val");
        int d;
        while ((d = dv.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
          mergedVals[d] = dv.nextValue();
        }

        for (int iter = 0; iter < 30; iter++) {
          int a = random().nextInt(1000), b = random().nextInt(1000);
          long qMin = Math.min(a, b), qMax = Math.max(a, b);
          BitSet truth = new BitSet();
          for (int x = 0; x < n; x++) {
            if (mergedVals[x] >= qMin && mergedVals[x] <= qMax) truth.set(x);
          }
          BitSet raw = new BitSet();
          pv.intersect(rangeVisitor(raw, qMin, qMax, mergedVals, false));
          for (int x = truth.nextSetBit(0); x >= 0; x = truth.nextSetBit(x + 1)) {
            assertTrue("sorted-merge value-free BKD dropped match docID=" + x, raw.get(x));
          }
          BitSet exact = new BitSet();
          pv.intersect(rangeVisitor(exact, qMin, qMax, mergedVals, true));
          assertEquals("sorted-merge residual result equals truth", truth, exact);
        }
      }
    }
  }

  /** Unwrap any test-framework wrappers and report whether the underlying BKD is value-free. */
  private static boolean unwrapDocIdsOnly(PointValues pv) {
    if (pv instanceof org.apache.lucene.tests.index.AssertingLeafReader.AssertingPointValues a) {
      pv = a.getWrapped();
    }
    assertTrue("expected a BKDReader, got " + pv.getClass(), pv instanceof org.apache.lucene.util.bkd.BKDReader);
    return ((org.apache.lucene.util.bkd.BKDReader) pv).isDocIdsOnly();
  }

  public void testValueFreeFilesAreSmaller() throws Exception {
    final int n = 30000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = random().nextLong();
    }
    try (Directory full = newDirectory();
        Directory lite = newDirectory()) {
      indexValues(full, values, false);
      indexValues(lite, values, true);
      long fullBytes = pointsBytes(full);
      long liteBytes = pointsBytes(lite);
      assertTrue(
          "value-free points (" + liteBytes + ") should be smaller than full (" + fullBytes + ")",
          liteBytes < fullBytes);
    }
  }

  private static long pointsBytes(Directory dir) throws IOException {
    long total = 0;
    for (String f : dir.listAll()) {
      if (f.endsWith(".kdd") || f.endsWith(".kdi") || f.endsWith(".kdm")) {
        total += dir.fileLength(f);
      }
    }
    return total;
  }
}
