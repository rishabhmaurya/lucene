package org.apache.lucene.document;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDSummaryWriter;

public class TSIntPoint extends LongPoint {

    String timeSeries;
    int measurement;
    long timestamp;

    public TSIntPoint(String name, String timeSeries, long timestamp, int measurement) {
        super(name, getType(1, true), timestamp);
        this.timeSeries = timeSeries;
        this.measurement = measurement;
    }
    static FieldType getType(int numDims, boolean summary) {
        assert numDims == 1;
        FieldType type = new FieldType();
        type.setDimensions(numDims, Long.BYTES);
        if (summary) {
            type.putAttribute("summary", "true");
        }
        type.freeze();
        return type;
    }
    public String getTimSeries() {
        return timeSeries;
    }

    public int getMeasurement() {
        return measurement;
    }

    public BytesRef getPackedMetrics() {
        byte [] b = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(measurement, b, 0);
        return new BytesRef(b);
    }

    public long getTimeStamp() {
        return timestamp;
    }

    public BKDSummaryWriter.SummaryMergeFunction<?> getMergeFunction() {
        return new BKDSummaryWriter.SummaryMergeFunction<Integer>() {

            @Override
            public int getSummarySize() {
                return Integer.BYTES;
            }

            @Override
            public void merge(byte[] a, byte[] b, byte[] c) {
                packBytes(unpackBytes(a) + unpackBytes(b), c);
            }

            @Override
            public Integer unpackBytes(byte[] val) {
                return NumericUtils.sortableBytesToInt(val, 0);
            }

            @Override
            public void packBytes(Integer val, byte[] res) {
                NumericUtils.intToSortableBytes(val, res, 0);
            }
        };
    }

}
