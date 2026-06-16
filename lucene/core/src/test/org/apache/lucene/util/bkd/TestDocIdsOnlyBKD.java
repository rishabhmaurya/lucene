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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.NumericUtils;

/**
 * Tests for the opt-in "doc-ids only" (value-free) BKD leaf format: leaves store only matching
 * doc-ids (no packed values), so a range query is answered as a conservative super-set that the
 * caller re-checks exactly. See {@link BKDWriter#VERSION_DOC_IDS_ONLY_LEAVES}.
 */
public class TestDocIdsOnlyBKD extends LuceneTestCase {

  /** Build a single-dim int BKD over docID->value, optionally value-free, and return the index FP. */
  private long writeInts(Directory dir, BKDConfig config, int[] values, boolean docIdsOnly)
      throws IOException {
    int n = values.length;
    BKDWriter w =
        new BKDWriter(n, dir, "tmp", config, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP, n, docIdsOnly);
    byte[] scratch = new byte[4];
    for (int docID = 0; docID < n; docID++) {
      NumericUtils.intToSortableBytes(values[docID], scratch, 0);
      w.add(scratch, docID);
    }
    long indexFP;
    try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
      IORunnable finalizer = w.finish(out, out, out);
      indexFP = out.getFilePointer();
      finalizer.run();
    }
    return indexFP;
  }

  /**
   * Range visitor that records hits. When {@code residual} is true it re-checks every doc-id
   * against the actual value (mirroring how a value-free index's caller would re-filter), so the
   * result becomes exact even though the index only delivered a super-set.
   */
  private IntersectVisitor rangeVisitor(
      BitSet hits, int queryMin, int queryMax, int[] values, boolean residual) {
    byte[] qMin = new byte[4];
    byte[] qMax = new byte[4];
    NumericUtils.intToSortableBytes(queryMin, qMin, 0);
    NumericUtils.intToSortableBytes(queryMax, qMax, 0);
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        if (residual) {
          int v = values[docID];
          if (v < queryMin || v > queryMax) {
            return;
          }
        }
        hits.set(docID);
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        if (Arrays.compareUnsigned(packedValue, 0, 4, qMin, 0, 4) >= 0
            && Arrays.compareUnsigned(packedValue, 0, 4, qMax, 0, 4) <= 0) {
          hits.set(docID);
        }
      }

      @Override
      public Relation compare(byte[] minPacked, byte[] maxPacked) {
        if (Arrays.compareUnsigned(maxPacked, 0, 4, qMin, 0, 4) < 0
            || Arrays.compareUnsigned(minPacked, 0, 4, qMax, 0, 4) > 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        } else if (Arrays.compareUnsigned(minPacked, 0, 4, qMin, 0, 4) < 0
            || Arrays.compareUnsigned(maxPacked, 0, 4, qMax, 0, 4) > 0) {
          return Relation.CELL_CROSSES_QUERY;
        } else {
          return Relation.CELL_INSIDE_QUERY;
        }
      }
    };
  }

  /**
   * The core invariant: a value-free range query returns a SUPER-SET of the true matches (never
   * misses one), and after a residual re-check the result is EXACTLY the true matches.
   */
  public void testDocIdsOnlyIsSupersetAndExactAfterResidual() throws Exception {
    final int n = 1000;
    final BKDConfig config = new BKDConfig(1, 1, 4, 16);
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = random().nextInt(500); // duplicates on purpose
    }
    try (Directory dir = newDirectory()) {
      long fp = writeInts(dir, config, values, true);
      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(fp);
        BKDReader r = new BKDReader(in, in, in);
        assertTrue("reader should report docIdsOnly", r.docIdsOnly);

        for (int iter = 0; iter < 50; iter++) {
          int a = random().nextInt(500);
          int b = random().nextInt(500);
          int qMin = Math.min(a, b);
          int qMax = Math.max(a, b);

          BitSet truth = new BitSet();
          for (int d = 0; d < n; d++) {
            if (values[d] >= qMin && values[d] <= qMax) {
              truth.set(d);
            }
          }

          // Raw (no residual): must be a super-set of truth.
          BitSet raw = new BitSet();
          r.intersect(rangeVisitor(raw, qMin, qMax, values, false));
          for (int d = truth.nextSetBit(0); d >= 0; d = truth.nextSetBit(d + 1)) {
            assertTrue("value-free missed a true match docID=" + d, raw.get(d));
          }

          // With residual re-check: must be exactly truth.
          BitSet exact = new BitSet();
          r.intersect(rangeVisitor(exact, qMin, qMax, values, true));
          assertEquals("residual-checked result must equal truth", truth, exact);
        }
      }
    }
  }

  /** The default (docIdsOnly=false) path must remain exact and unchanged. */
  public void testDefaultPathStillExact() throws Exception {
    final int n = 1000;
    final BKDConfig config = new BKDConfig(1, 1, 4, 16);
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = random().nextInt(500);
    }
    try (Directory dir = newDirectory()) {
      long fp = writeInts(dir, config, values, false);
      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(fp);
        BKDReader r = new BKDReader(in, in, in);
        assertFalse("default reader must not be docIdsOnly", r.docIdsOnly);
        for (int iter = 0; iter < 20; iter++) {
          int a = random().nextInt(500);
          int b = random().nextInt(500);
          int qMin = Math.min(a, b);
          int qMax = Math.max(a, b);
          BitSet truth = new BitSet();
          for (int d = 0; d < n; d++) {
            if (values[d] >= qMin && values[d] <= qMax) {
              truth.set(d);
            }
          }
          BitSet got = new BitSet();
          r.intersect(rangeVisitor(got, qMin, qMax, values, false));
          assertEquals("default path must be exact (no residual)", truth, got);
        }
      }
    }
  }

  /** Value-free leaves should be materially smaller on disk than full leaves. */
  public void testDocIdsOnlyIsSmallerOnDisk() throws Exception {
    final int n = 20000;
    final BKDConfig config = new BKDConfig(1, 1, 4, 512);
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = random().nextInt();
    }
    try (Directory dirFull = newDirectory();
        Directory dirLite = newDirectory()) {
      writeInts(dirFull, config, values, false);
      writeInts(dirLite, config, values, true);
      long full = dirFull.fileLength("bkd");
      long lite = dirLite.fileLength("bkd");
      assertTrue(
          "value-free index (" + lite + ") should be smaller than full (" + full + ")",
          lite < full);
    }
  }

  /** Sole-value and all-same-value edge cases still produce a correct super-set. */
  public void testEdgeCasesSingleAndConstant() throws Exception {
    final BKDConfig config = new BKDConfig(1, 1, 4, 8);
    // all docs share one value
    int[] constant = new int[100];
    Arrays.fill(constant, 7);
    try (Directory dir = newDirectory()) {
      long fp = writeInts(dir, config, constant, true);
      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(fp);
        BKDReader r = new BKDReader(in, in, in);
        // query that includes the value -> all 100 docs
        BitSet hits = new BitSet();
        r.intersect(rangeVisitor(hits, 7, 7, constant, true));
        assertEquals(100, hits.cardinality());
        // query that excludes it -> none
        BitSet none = new BitSet();
        r.intersect(rangeVisitor(none, 8, 100, constant, true));
        assertEquals(0, none.cardinality());
      }
    }
  }

  /** docIdsOnly must be rejected for multi-dimensional configs. */
  public void testDocIdsOnlyRejectsMultiDim() throws Exception {
    BKDConfig config = new BKDConfig(2, 2, 4, 16);
    try (Directory dir = newDirectory()) {
      expectThrows(
          IllegalArgumentException.class,
          () ->
              new BKDWriter(
                  10, dir, "tmp", config, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP, 10, true));
    }
  }

  /** Larger randomized sweep across configs/leaf sizes; the super-set invariant must always hold. */
  public void testRandomizedSuperset() throws Exception {
    int iters = atLeast(3);
    for (int it = 0; it < iters; it++) {
      int n = TestUtil.nextInt(random(), 50, 5000);
      int maxPointsInLeaf = TestUtil.nextInt(random(), 2, 256);
      BKDConfig config = new BKDConfig(1, 1, 4, maxPointsInLeaf);
      int span = TestUtil.nextInt(random(), 2, 10000);
      int[] values = new int[n];
      for (int i = 0; i < n; i++) {
        values[i] = random().nextInt(span);
      }
      try (Directory dir = newDirectory()) {
        long fp = writeInts(dir, config, values, true);
        try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
          in.seek(fp);
          BKDReader r = new BKDReader(in, in, in);
          for (int q = 0; q < 10; q++) {
            int a = random().nextInt(span);
            int b = random().nextInt(span);
            int qMin = Math.min(a, b);
            int qMax = Math.max(a, b);
            BitSet truth = new BitSet();
            for (int d = 0; d < n; d++) {
              if (values[d] >= qMin && values[d] <= qMax) {
                truth.set(d);
              }
            }
            BitSet raw = new BitSet();
            r.intersect(rangeVisitor(raw, qMin, qMax, values, false));
            for (int d = truth.nextSetBit(0); d >= 0; d = truth.nextSetBit(d + 1)) {
              assertTrue("missed match n=" + n + " leaf=" + maxPointsInLeaf, raw.get(d));
            }
            BitSet exact = new BitSet();
            r.intersect(rangeVisitor(exact, qMin, qMax, values, true));
            assertEquals(truth, exact);
          }
        }
      }
    }
  }
}
