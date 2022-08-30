package org.apache.lucene.backward_codecs.lucene93Ext;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDReader;
import org.apache.lucene.util.bkd.BKDWithSummaryReader;

import java.io.IOException;

public class Lucene93ExtPointsReader extends Lucene90PointsReader {
    /**
     * Sole constructor
     *
     * @param readState
     */
    IndexInput summaryIndexIn, summaryDataIn;
    long summaryIndexLength = -1, summaryDataLength = -1;
    public Lucene93ExtPointsReader(SegmentReadState readState) throws IOException {
        super(readState);
    }

    @Override
    public void init(SegmentReadState readState) throws IOException {
        summaryIndexIn = getSummaryIndexIn(readState);
        summaryDataIn = getSummaryDataIn(readState);
    }
    private IndexInput getSummaryIndexIn(SegmentReadState readState) throws IOException {
        boolean success = false;
        IndexInput summaryIndexIn = null;
        for (FieldInfo fieldInfo : readState.fieldInfos) {
            if (fieldInfo.hasPointsSummary()) {
                try {
                    String summaryIndexName =
                            IndexFileNames.segmentFileName(
                                    readState.segmentInfo.name,
                                    readState.segmentSuffix,
                                    Lucene93ExtCodec.SUMMARY_INDEX_EXTENSION);
                    summaryIndexIn = readState.directory.openInput(summaryIndexName, readState.context);
                    CodecUtil.checkIndexHeader(
                            summaryIndexIn,
                            Lucene93ExtCodec.SUMMARY_INDEX_CODEC_NAME,
                            Lucene90PointsFormat.VERSION_START,
                            Lucene90PointsFormat.VERSION_CURRENT,
                            readState.segmentInfo.getId(),
                            readState.segmentSuffix);
                    CodecUtil.retrieveChecksum(summaryIndexIn);
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(this);
                    }
                }
                break;
            }
        }
        return summaryIndexIn;
    }

    private IndexInput getSummaryDataIn(SegmentReadState readState) throws IOException {
        boolean success = false;
        IndexInput summaryDataIn = null;
        for (FieldInfo fieldInfo : readState.fieldInfos) {
            if (fieldInfo.hasPointsSummary()) {
                try {
                    String summaryDataName =
                            IndexFileNames.segmentFileName(
                                    readState.segmentInfo.name,
                                    readState.segmentSuffix,
                                    Lucene93ExtCodec.SUMMARY_DATA_EXTENSION);
                    summaryDataIn = readState.directory.openInput(summaryDataName, readState.context);
                    CodecUtil.checkIndexHeader(
                            summaryDataIn,
                            Lucene93ExtCodec.SUMMARY_DATA_CODEC_NAME,
                            Lucene90PointsFormat.VERSION_START,
                            Lucene90PointsFormat.VERSION_CURRENT,
                            readState.segmentInfo.getId(),
                            readState.segmentSuffix);
                    CodecUtil.retrieveChecksum(summaryDataIn);
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(this);
                    }
                }
                break;
            }
        }
        return summaryDataIn;
    }

    @Override
    public BKDReader getBKDReader(SegmentReadState readState, int fieldNumber, IndexInput metaIn, IndexInput indexIn, IndexInput dataIn)
            throws IOException {
        if (readState.fieldInfos.fieldInfo(fieldNumber).hasPointsSummary()) {
            return new BKDWithSummaryReader(metaIn, indexIn, dataIn, summaryIndexIn, summaryDataIn);
        } else {
            return super.getBKDReader(readState, fieldNumber, metaIn, indexIn, dataIn);
        }
    }

    @Override
    public void readAdditionalMetadata(IndexInput metaIn) throws IOException {
        super.readAdditionalMetadata(metaIn);
        if (summaryIndexIn != null) {
            summaryIndexLength = metaIn.readLong();
            summaryDataLength = metaIn.readLong();
        }
    }

    @Override
    public void retrieveAdditionalChecksum() throws IOException {
        super.retrieveAdditionalChecksum();
        if (summaryIndexIn != null) {
            CodecUtil.retrieveChecksum(summaryIndexIn, summaryIndexLength);
        }
        if (summaryDataIn != null) {
            CodecUtil.retrieveChecksum(summaryDataIn, summaryDataLength);
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        super.checkIntegrity();
        if (summaryIndexIn != null) {
            CodecUtil.checksumEntireFile(summaryIndexIn);
        }
        if (summaryDataIn != null) {
            CodecUtil.checksumEntireFile(summaryDataIn);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(summaryIndexIn, summaryDataIn);
        super.close();
    }
}
