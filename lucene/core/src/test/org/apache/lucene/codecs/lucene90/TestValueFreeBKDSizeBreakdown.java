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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

/**
 * Quantifies the on-disk storage of the value-free numeric range index, broken down into the
 * three contributors so we can answer "how much is BKD vs how much is doc-values":
 *
 * <ul>
 *   <li><b>FULL_POINT</b> — a standard {@code LongPoint} (values stored in the BKD leaves). Baseline.
 *   <li><b>VFREE_BKD_ONLY</b> — value-free BKD, NO doc-values. The pure pruning-index cost.
 *   <li><b>VFREE_BKD_PLUS_DV</b> — value-free BKD + co-written SortedNumericDocValues (the
 *       merge-survival config actually shipped). BKD-leaves + DV columns.
 * </ul>
 *
 * Reports bytes per points-extension (.kdd/.kdi/.kdm) and doc-values (.dvd/.dvm) so the BKD
 * contribution and the DV contribution are separately visible. Single segment (no merge) for a
 * clean comparison.
 */
public class TestValueFreeBKDSizeBreakdown extends LuceneTestCase {

  private static final String FIELD = "v";

  private static byte[] pack(long v) {
    byte[] b = new byte[Long.BYTES];
    NumericUtils.longToSortableBytes(v, b, 0);
    return b;
  }

  private static FieldType valueFreeType() {
    FieldType t = new FieldType();
    t.setDimensions(1, Long.BYTES);
    t.putAttribute(Lucene90PointsWriter.DOC_IDS_ONLY_ATTRIBUTE_KEY, "true");
    t.freeze();
    return t;
  }

  private enum Mode {
    FULL_POINT,
    VFREE_BKD_ONLY,
    VFREE_BKD_PLUS_DV
  }

  private long[] buildAndMeasure(Directory dir, Mode mode, long[] values) throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig().setCodec(org.apache.lucene.codecs.Codec.forName("Lucene104"));
    iwc.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(512.0);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setUseCompoundFile(false);
    FieldType vfType = (mode != Mode.FULL_POINT) ? valueFreeType() : null;
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (long v : values) {
        Document doc = new Document();
        if (mode == Mode.FULL_POINT) {
          doc.add(new LongPoint(FIELD, v));
        } else {
          doc.add(new Field(FIELD, pack(v), vfType));
          if (mode == Mode.VFREE_BKD_PLUS_DV) {
            doc.add(new SortedNumericDocValuesField(FIELD, v));
          }
        }
        w.addDocument(doc);
      }
      w.commit();
    }
    long bkd = 0, dv = 0, other = 0;
    for (String f : dir.listAll()) {
      long len = dir.fileLength(f);
      if (f.endsWith(".kdd") || f.endsWith(".kdi") || f.endsWith(".kdm")) {
        bkd += len;
      } else if (f.endsWith(".dvd") || f.endsWith(".dvm")) {
        dv += len;
      } else {
        other += len;
      }
    }
    return new long[] {bkd, dv, other};
  }

  public void testSizeBreakdown() throws Exception {
    final int n = 1_000_000; // 1M docs for a representative per-doc amortized size
    long[] values = new long[n];
    // Mixed distribution: a scattered timestamp-like long (the EventTime motivating case).
    long base = 1_372_795_000_000L;
    for (int i = 0; i < n; i++) {
      values[i] = base + (long) (random().nextDouble() * 2_332_993_000L);
    }

    long[] full, vfree, vfreeDv;
    try (Directory d = FSDirectory.open(createTempDir("full"))) {
      full = buildAndMeasure(d, Mode.FULL_POINT, values);
    }
    try (Directory d = FSDirectory.open(createTempDir("vfree"))) {
      vfree = buildAndMeasure(d, Mode.VFREE_BKD_ONLY, values);
    }
    try (Directory d = FSDirectory.open(createTempDir("vfreeDv"))) {
      vfreeDv = buildAndMeasure(d, Mode.VFREE_BKD_PLUS_DV, values);
    }

    double mb = 1024.0 * 1024.0;
    StringBuilder sb = new StringBuilder("\n=== VALUE-FREE BKD SIZE BREAKDOWN (n=" + n + " longs) ===\n");
    sb.append(String.format(java.util.Locale.ROOT, "%-22s %12s %12s %12s%n", "mode", "BKD(MB)", "DV(MB)", "BKD B/doc"));
    sb.append(row("FULL_POINT (baseline)", full, n, mb));
    sb.append(row("VFREE_BKD_ONLY", vfree, n, mb));
    sb.append(row("VFREE_BKD_PLUS_DV", vfreeDv, n, mb));
    sb.append("\n");
    sb.append(String.format(java.util.Locale.ROOT, "BKD saving vs full point: %.1f%% smaller leaves%n",
        100.0 * (full[0] - vfree[0]) / full[0]));
    sb.append(String.format(java.util.Locale.ROOT, "DV co-write adds: %.2f MB (%.2f B/doc)%n",
        vfreeDv[1] / mb, (double) vfreeDv[1] / n));
    sb.append(String.format(java.util.Locale.ROOT, "Total value-free+DV vs full point: %.2f MB vs %.2f MB%n",
        (vfreeDv[0] + vfreeDv[1]) / mb, (full[0] + full[1]) / mb));
    System.out.println(sb);

    // Assertions: value-free BKD leaves are smaller than full-point; DV is the dominant add-back.
    assertTrue("value-free BKD should be smaller than full point BKD", vfree[0] < full[0]);
    assertTrue("DV co-write should add measurable doc-values bytes", vfreeDv[1] > 0);
    assertEquals("BKD-only must write zero doc-values", 0, vfree[1]);
  }

  private static String row(String name, long[] sizes, int n, double mb) {
    return String.format(
        java.util.Locale.ROOT,
        "%-22s %12.2f %12.2f %12.2f%n",
        name,
        sizes[0] / mb,
        sizes[1] / mb,
        (double) sizes[0] / n);
  }
}
