package org.apache.lucene.backward_codecs.lucene93Ext;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene93.Lucene93Codec;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDWriter;

import java.io.IOException;

public class Lucene93ExtCodec extends FilterCodec {

    public static final String SUMMARY_INDEX_EXTENSION = "kdsi";

    public static final String SUMMARY_DATA_EXTENSION = "kdsd";
    public static final String SUMMARY_DATA_CODEC_NAME = "Lucene93PointsFormatSummaryData";
    public static final String SUMMARY_INDEX_CODEC_NAME = "Lucene93PointsFormatSummaryIndex";

    final int maxPointsInLeafNode;
    final double maxMBSortHeap;

    public Lucene93ExtCodec() {
        this(new Lucene93Codec(), BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
                BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
    }
    public Lucene93ExtCodec(Codec defaultCodec, int maxPointsInLeafNode, double maxMBSortHeap) {
        super("Lucene93ExtCodec", defaultCodec);
        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.maxMBSortHeap = maxMBSortHeap;
    }

    @Override
    public PointsFormat pointsFormat() {
        return new PointsFormat() {
            @Override
            public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
                return new Lucene93ExtPointsWriter(writeState, maxPointsInLeafNode,  maxMBSortHeap);
            }

            @Override
            public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
                return new Lucene93ExtPointsReader(readState);
            }
        };
    }
}
