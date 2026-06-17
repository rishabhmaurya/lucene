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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDWriter;

/** Writes dimensional values */
public class Lucene90PointsWriter extends PointsWriter {

  /**
   * Opt-in per-field attribute. When a single-dimension field's {@link FieldInfo} carries this
   * attribute set to {@code "true"}, its BKD is written in the value-free ("doc-ids only") leaf
   * format (see {@link org.apache.lucene.util.bkd.BKDWriter#VERSION_DOC_IDS_ONLY_LEAVES}): leaf
   * blocks store only matching doc-ids and range queries return a conservative super-set the
   * caller must re-check. Absent or any other value => the standard full-value format (byte
   * identical to before). Set it via {@code FieldType.putAttribute(...)} at index time.
   */
  public static final String DOC_IDS_ONLY_ATTRIBUTE_KEY = "bkdDocIdsOnly";

  /** Outputs used to write the BKD tree data files. */
  protected final IndexOutput metaOut, indexOut, dataOut;

  final SegmentWriteState writeState;
  final int maxPointsInLeafNode;
  final double maxMBSortInHeap;
  final int version;
  private boolean finished;

  /** Full constructor */
  public Lucene90PointsWriter(
      SegmentWriteState writeState, int maxPointsInLeafNode, double maxMBSortInHeap, int version)
      throws IOException {
    assert writeState.fieldInfos.hasPointValues();
    this.writeState = writeState;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxMBSortInHeap = maxMBSortInHeap;
    this.version = version;
    String dataFileName =
        IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Lucene90PointsFormat.DATA_EXTENSION);
    dataOut = writeState.directory.createOutput(dataFileName, writeState.context);
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(
          dataOut,
          Lucene90PointsFormat.DATA_CODEC_NAME,
          Lucene90PointsFormat.VERSION_CURRENT,
          writeState.segmentInfo.getId(),
          writeState.segmentSuffix);

      String metaFileName =
          IndexFileNames.segmentFileName(
              writeState.segmentInfo.name,
              writeState.segmentSuffix,
              Lucene90PointsFormat.META_EXTENSION);
      metaOut = writeState.directory.createOutput(metaFileName, writeState.context);
      CodecUtil.writeIndexHeader(
          metaOut,
          Lucene90PointsFormat.META_CODEC_NAME,
          Lucene90PointsFormat.VERSION_CURRENT,
          writeState.segmentInfo.getId(),
          writeState.segmentSuffix);

      String indexFileName =
          IndexFileNames.segmentFileName(
              writeState.segmentInfo.name,
              writeState.segmentSuffix,
              Lucene90PointsFormat.INDEX_EXTENSION);
      indexOut = writeState.directory.createOutput(indexFileName, writeState.context);
      CodecUtil.writeIndexHeader(
          indexOut,
          Lucene90PointsFormat.INDEX_CODEC_NAME,
          Lucene90PointsFormat.VERSION_CURRENT,
          writeState.segmentInfo.getId(),
          writeState.segmentSuffix);

      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  public Lucene90PointsWriter(
      SegmentWriteState writeState, int maxPointsInLeafNode, double maxMBSortInHeap)
      throws IOException {
    this(writeState, maxPointsInLeafNode, maxMBSortInHeap, Lucene90PointsFormat.VERSION_CURRENT);
  }

  /**
   * Uses the defaults values for {@code maxPointsInLeafNode} (512) and {@code maxMBSortInHeap}
   * (16.0)
   */
  public Lucene90PointsWriter(SegmentWriteState writeState) throws IOException {
    this(
        writeState,
        BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
        BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
        Lucene90PointsFormat.VERSION_CURRENT);
  }

  /** Constructor that takes a version. This is used for testing with older versions. */
  Lucene90PointsWriter(SegmentWriteState writeState, int version) throws IOException {
    this(
        writeState,
        BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
        BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
        version);
  }

  @Override
  public void writeField(FieldInfo fieldInfo, PointsReader reader) throws IOException {

    PointValues.PointTree values = reader.getValues(fieldInfo.name).getPointTree();

    BKDConfig config =
        new BKDConfig(
            fieldInfo.getPointDimensionCount(),
            fieldInfo.getPointIndexDimensionCount(),
            fieldInfo.getPointNumBytes(),
            maxPointsInLeafNode);

    boolean docIdsOnly = isDocIdsOnly(fieldInfo);
    // A value-free field needs the BKD version that understands the doc-ids-only flag, regardless
    // of the segment's points-format version. Per-field BKD versioning is safe: each field's BKD
    // writes its own header, and BKDReader reads it back per field.
    int bkdVersion =
        docIdsOnly
            ? BKDWriter.VERSION_DOC_IDS_ONLY_LEAVES
            : Lucene90PointsFormat.bkdVersion(version);

    try (BKDWriter writer =
        new BKDWriter(
            writeState.segmentInfo.maxDoc(),
            writeState.directory,
            writeState.segmentInfo.name,
            config,
            maxMBSortInHeap,
            values.size(),
            bkdVersion,
            docIdsOnly)) {

      if (values instanceof MutablePointTree) {
        IORunnable finalizer =
            writer.writeField(
                metaOut, indexOut, dataOut, fieldInfo.name, (MutablePointTree) values);
        if (finalizer != null) {
          metaOut.writeInt(fieldInfo.number);
          finalizer.run();
        }
        return;
      }

      values.visitDocValues(
          new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              throw new IllegalStateException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              writer.add(packedValue, docID);
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }
          });

      // We could have 0 points on merge since all docs with dimensional fields may be deleted:
      IORunnable finalizer = writer.finish(metaOut, indexOut, dataOut);
      if (finalizer != null) {
        metaOut.writeInt(fieldInfo.number);
        finalizer.run();
      }
    }
  }

  /**
   * Whether this field opted in to the value-free leaf format. Only single-dimension fields may
   * opt in (the BKD doc-ids-only mode is 1-D only); a multi-dim field carrying the attribute is
   * ignored here and falls back to the standard format.
   */
  static boolean isDocIdsOnly(FieldInfo fieldInfo) {
    return fieldInfo.getPointDimensionCount() == 1
        && Boolean.parseBoolean(fieldInfo.getAttribute(DOC_IDS_ONLY_ATTRIBUTE_KEY));
  }

  /**
   * Intercepts the per-field merge so a value-free ("doc-ids only") field is rebuilt from its
   * co-written SortedNumericDocValues rather than from its (value-less) points. This is the entry
   * the IndexSort / {@code SortingCodecReader} path uses (base {@code PointsWriter.merge} →
   * {@code mergeOneField}); without it the sorting merge visitor calls {@code visit(int docID)} on
   * a value-free leaf and the standard guard throws "this writer hit an unrecoverable error".
   */
  @Override
  protected void mergeOneField(MergeState mergeState, FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getPointDimensionCount() != 0 && isDocIdsOnly(fieldInfo)) {
      mergeDocIdsOnlyFromDocValues(fieldInfo, mergeState);
      return;
    }
    super.mergeOneField(mergeState, fieldInfo);
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    /*
     * If indexSort is activated and some of the leaves are not sorted the next test will catch that
     * and the non-optimized merge will run. If the readers are all sorted then it's safe to perform
     * a bulk merge of the points.
     */
    for (PointsReader reader : mergeState.pointsReaders) {
      if (reader instanceof Lucene90PointsReader == false) {
        // We can only bulk merge when all to-be-merged segments use our format. The base
        // PointsWriter.merge dispatches per field to mergeOneField (overridden above), which
        // handles value-free fields via doc-values rebuild — so this path is value-free-safe.
        super.merge(mergeState);
        return;
      }
    }
    for (PointsReader reader : mergeState.pointsReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }

    // A value-free ("doc-ids only") field stores no per-point values in its leaves, so the normal
    // merge paths (bulk BKDWriter.merge / base re-index, both of which read source point values)
    // cannot reconstruct its BKD. Instead we rebuild it from the field's SortedNumericDocValues,
    // which are co-written at index time precisely as the merge-survival value source and merge
    // natively. Handle those fields here and skip them in the normal point-merge loop below.
    java.util.Set<String> docIdsOnlyHandled = new java.util.HashSet<>();
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.getPointDimensionCount() != 0 && isDocIdsOnly(fieldInfo)) {
        if (fieldInfo.getPointDimensionCount() != 1 || fieldInfo.getPointNumBytes() != Long.BYTES) {
          throw new IllegalStateException(
              "Value-free (doc-ids only) BKD merge supports only single-dimension 8-byte points; "
                  + "field \""
                  + fieldInfo.name
                  + "\" has dims="
                  + fieldInfo.getPointDimensionCount()
                  + " bytesPerDim="
                  + fieldInfo.getPointNumBytes());
        }
        mergeDocIdsOnlyFromDocValues(fieldInfo, mergeState);
        docIdsOnlyHandled.add(fieldInfo.name);
      }
    }

    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (docIdsOnlyHandled.contains(fieldInfo.name)) {
        continue; // already rebuilt from doc-values above
      }
      if (fieldInfo.getPointDimensionCount() != 0) {
        if (fieldInfo.getPointDimensionCount() == 1) {

          // Worst case total maximum size (if none of the points are deleted):
          long totMaxSize = 0;
          for (int i = 0; i < mergeState.pointsReaders.length; i++) {
            PointsReader reader = mergeState.pointsReaders[i];
            if (reader != null) {
              FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
              FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
              if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
                PointValues values = reader.getValues(fieldInfo.name);
                if (values != null) {
                  totMaxSize += values.size();
                }
              }
            }
          }

          BKDConfig config =
              new BKDConfig(
                  fieldInfo.getPointDimensionCount(),
                  fieldInfo.getPointIndexDimensionCount(),
                  fieldInfo.getPointNumBytes(),
                  maxPointsInLeafNode);

          // System.out.println("MERGE: field=" + fieldInfo.name);
          // Optimize the 1D case to use BKDWriter.merge, which does a single merge sort of the
          // already sorted incoming segments, instead of trying to sort all points again as if
          // we were simply reindexing them:
          try (BKDWriter writer =
              new BKDWriter(
                  writeState.segmentInfo.maxDoc(),
                  writeState.directory,
                  writeState.segmentInfo.name,
                  config,
                  maxMBSortInHeap,
                  totMaxSize,
                  Lucene90PointsFormat.bkdVersion(version))) {
            List<PointValues> pointValues = new ArrayList<>();
            List<MergeState.DocMap> docMaps = new ArrayList<>();
            for (int i = 0; i < mergeState.pointsReaders.length; i++) {
              PointsReader reader = mergeState.pointsReaders[i];

              if (reader != null) {

                // we confirmed this up above
                assert reader instanceof Lucene90PointsReader;
                Lucene90PointsReader reader90 = (Lucene90PointsReader) reader;

                // NOTE: we cannot just use the merged fieldInfo.number (instead of resolving to
                // this
                // reader's FieldInfo as we do below) because field numbers can easily be different
                // when addIndexes(Directory...) copies over segments from another index:

                FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
                FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
                if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
                  PointValues aPointValues = reader90.getValues(readerFieldInfo.name);
                  if (aPointValues != null) {
                    pointValues.add(aPointValues);
                    docMaps.add(mergeState.docMaps[i]);
                  }
                }
              }
            }

            IORunnable finalizer = writer.merge(metaOut, indexOut, dataOut, docMaps, pointValues);
            if (finalizer != null) {
              metaOut.writeInt(fieldInfo.number);
              finalizer.run();
            }
          }
        } else {
          mergeOneField(mergeState, fieldInfo);
        }
      }
    }

    finish();
  }

  /**
   * Rebuilds a value-free ("doc-ids only") BKD for the merged segment from the field's
   * {@link org.apache.lucene.index.SortedNumericDocValues} across all source segments. The
   * value-free leaves themselves carry no values, so they cannot be merged directly; the
   * co-written doc-values (see the indexer's numeric field factory) are the merge-survival value
   * source. Each source doc's value is read in merged-docID order (applying the per-segment doc
   * map and live docs), fed to a fresh {@code docIdsOnly} {@link BKDWriter}, and the resulting BKD
   * replaces the field's points in the merged segment.
   */
  private void mergeDocIdsOnlyFromDocValues(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    BKDConfig config =
        new BKDConfig(
            fieldInfo.getPointDimensionCount(),
            fieldInfo.getPointIndexDimensionCount(),
            fieldInfo.getPointNumBytes(),
            maxPointsInLeafNode);

    int maxDoc = writeState.segmentInfo.maxDoc();
    // Collect (mergedDocID, value) for every live doc that has the field, then add in docID order
    // (BKDWriter.add for 1-D requires ascending value? No — add() buffers and sorts; but the
    // OneDimensionBKDWriter via writeField needs sorted input. We use the buffering add() path
    // which sorts internally, so insertion order is free.)
    try (BKDWriter writer =
        new BKDWriter(
            maxDoc,
            writeState.directory,
            writeState.segmentInfo.name,
            config,
            maxMBSortInHeap,
            (long) maxDoc,
            BKDWriter.VERSION_DOC_IDS_ONLY_LEAVES,
            true)) {

      byte[] scratch = new byte[Long.BYTES];
      boolean any = false;
      for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
        org.apache.lucene.codecs.DocValuesProducer dvp = mergeState.docValuesProducers[i];
        if (dvp == null) {
          continue;
        }
        FieldInfos readerFieldInfos = mergeState.fieldInfos[i];
        FieldInfo readerFieldInfo = readerFieldInfos.fieldInfo(fieldInfo.name);
        if (readerFieldInfo == null
            || readerFieldInfo.getDocValuesType() != org.apache.lucene.index.DocValuesType.SORTED_NUMERIC) {
          throw new IllegalStateException(
              "Value-free BKD field \""
                  + fieldInfo.name
                  + "\" requires co-written SORTED_NUMERIC doc-values to merge, but source segment "
                  + i
                  + " has docValuesType="
                  + (readerFieldInfo == null ? "<absent>" : readerFieldInfo.getDocValuesType()));
        }
        org.apache.lucene.index.SortedNumericDocValues dv = dvp.getSortedNumeric(readerFieldInfo);
        MergeState.DocMap docMap = mergeState.docMaps[i];
        int docID;
        while ((docID = dv.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
          int newDocID = docMap.get(docID);
          if (newDocID == -1) {
            continue; // deleted in the merged view
          }
          // A value-free numeric point is single-valued; take the first value.
          long value = dv.nextValue();
          org.apache.lucene.util.NumericUtils.longToSortableBytes(value, scratch, 0);
          writer.add(scratch, newDocID);
          any = true;
        }
      }

      if (any == false) {
        return; // no live docs with this field; nothing to write
      }
      IORunnable finalizer = writer.finish(metaOut, indexOut, dataOut);
      if (finalizer != null) {
        metaOut.writeInt(fieldInfo.number);
        finalizer.run();
      }
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    metaOut.writeInt(-1);
    CodecUtil.writeFooter(indexOut);
    CodecUtil.writeFooter(dataOut);
    metaOut.writeLong(indexOut.getFilePointer());
    metaOut.writeLong(dataOut.getFilePointer());
    CodecUtil.writeFooter(metaOut);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(metaOut, indexOut, dataOut);
  }
}
