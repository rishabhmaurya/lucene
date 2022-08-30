package org.apache.lucene.backward_codecs.lucene93Ext;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutableSummaryPointTree;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDSummaryWriter;
import org.apache.lucene.util.bkd.BKDWriter;

import java.io.IOException;

public class Lucene93ExtPointsWriter extends Lucene90PointsWriter {
    protected IndexOutput summaryIndex = null, summaryData = null;
    SegmentWriteState writeState;

    public Lucene93ExtPointsWriter(SegmentWriteState writeState, int maxPointsInLeafNode, double maxMBSortInHeap) throws IOException {
        super(writeState, maxPointsInLeafNode, maxMBSortInHeap);
        this.writeState = writeState;
    }

    public Lucene93ExtPointsWriter(SegmentWriteState writeState) throws IOException {
        super(writeState);
    }

    @Override
    public BKDWriter getBKDWriter(SegmentWriteState writeState, PointValues.PointTree values, BKDConfig config, FieldInfo fieldInfo) {
        if(values instanceof MutableSummaryPointTree && fieldInfo.hasPointsSummary()) {
            return new BKDSummaryWriter(
                    writeState.segmentInfo.maxDoc(),
                    writeState.directory,
                    writeState.segmentInfo.name,
                    config,
                    maxMBSortInHeap,
                    values.size());
        } else {
            return super.getBKDWriter(writeState, values, config, fieldInfo);
        }
    }

    @Override
    public Runnable writeField(IndexOutput metaOut,
                               IndexOutput indexOut,
                               IndexOutput dataOut,
                               FieldInfo fieldInfo,
                               PointValues.PointTree pointTree,
                               BKDWriter writer) throws IOException {
        if(pointTree instanceof MutableSummaryPointTree && fieldInfo.hasPointsSummary()) {
            initSummaryFiles();
            return ((BKDSummaryWriter) writer).writeField(metaOut, indexOut, dataOut, summaryIndex, summaryData,
                    fieldInfo.getName(), (MutableSummaryPointTree) pointTree, fieldInfo.getMergeFunction());
        } else {
            return super.writeField(metaOut, indexOut, dataOut, fieldInfo, pointTree, writer);
        }
    }

    private void initSummaryFiles() throws IOException {
        if (summaryIndex == null) {
            String summaryIndexFileName =
                    IndexFileNames.segmentFileName(
                            writeState.segmentInfo.name,
                            writeState.segmentSuffix,
                            Lucene93ExtCodec.SUMMARY_INDEX_EXTENSION);
            summaryIndex = writeState.directory.createOutput(summaryIndexFileName, writeState.context);
            CodecUtil.writeIndexHeader(
                    summaryIndex,
                    Lucene93ExtCodec.SUMMARY_INDEX_CODEC_NAME,
                    Lucene90PointsFormat.VERSION_CURRENT,
                    writeState.segmentInfo.getId(),
                    writeState.segmentSuffix);
        }

        if (summaryData == null) {
            String summaryDataFileName =
                    IndexFileNames.segmentFileName(
                            writeState.segmentInfo.name,
                            writeState.segmentSuffix,
                            Lucene93ExtCodec.SUMMARY_DATA_EXTENSION);
            summaryData = writeState.directory.createOutput(summaryDataFileName, writeState.context);
            CodecUtil.writeIndexHeader(
                    summaryData,
                    Lucene93ExtCodec.SUMMARY_DATA_CODEC_NAME,
                    Lucene90PointsFormat.VERSION_CURRENT,
                    writeState.segmentInfo.getId(),
                    writeState.segmentSuffix);
        }
    }

    @Override
    public Runnable writerFinish(BKDWriter writer, IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut)
            throws IOException {
        throw new UnsupportedOperationException("Merge on BKDTrees is not yet supported yet");
    }

    @Override
    public void finish() throws IOException {
        if (summaryIndex != null) {
            CodecUtil.writeFooter(summaryIndex);
        }
        if (summaryData != null) {
            CodecUtil.writeFooter(summaryData);
        }
        super.finish();
    }

    public void writeAdditionalMetadata(IndexOutput metaOut) throws IOException {
        super.writeAdditionalMetadata(metaOut);
        if (summaryIndex != null) {
            metaOut.writeLong(summaryIndex.getFilePointer());
        }
        if (summaryData != null) {
            metaOut.writeLong(summaryData.getFilePointer());
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (summaryIndex != null) {
            IOUtils.close(summaryIndex);
        }
        if (summaryData != null) {
            IOUtils.close(summaryData);
        }
    }
}
