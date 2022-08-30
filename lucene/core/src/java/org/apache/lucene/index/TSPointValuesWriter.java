package org.apache.lucene.index;

import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.codecs.MutableSummaryPointTree;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.bkd.BKDSummaryWriter;

import java.io.IOException;

public class TSPointValuesWriter extends PointValuesWriter {

    private final PagedBytes summaryBytes;
    private final DataOutput summaryBytesOut;

    private final int packedSummaryLength;

    private final BKDSummaryWriter.SummaryMergeFunction<?> mergeFunction;
    public TSPointValuesWriter(Counter bytesUsed, FieldInfo fieldInfo) {
        super(bytesUsed, fieldInfo);
        this.summaryBytes = new PagedBytes(12);
        this.summaryBytesOut = summaryBytes.getDataOutput();
        mergeFunction = fieldInfo.getMergeFunction();
        packedSummaryLength = mergeFunction.getSummarySize();
    }

    public void addPackedValue(int docID, BytesRef timestamp, BytesRef summaryData) throws IOException {
        addPackedValue(docID, timestamp);
        final long bytesRamBytesUsedBefore = summaryBytes.ramBytesUsed();
        summaryBytesOut.writeBytes(summaryData.bytes, summaryData.offset, summaryData.length);
        iwBytesUsed.addAndGet(summaryBytes.ramBytesUsed() - bytesRamBytesUsedBefore);
    }

    @Override
    public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer)
            throws IOException {
        final PagedBytes.Reader bytesReader = bytes.freeze(false);
        final PagedBytes.Reader summaryBytesReader = summaryBytes.freeze(false);

        MutablePointTree points =
                new MutableSummaryPointTree() {
                    final int[] ords = new int[numPoints];
                    int[] temp;

                    {
                        for (int i = 0; i < numPoints; ++i) {
                            ords[i] = i;
                        }
                    }

                    @Override
                    public long size() {
                        return numPoints;
                    }

                    @Override
                    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
                        final BytesRef scratch = new BytesRef();
                        final byte[] packedValue = new byte[packedBytesLength];

                        final BytesRef summaryScratch = new BytesRef();
                        final byte[] packedSummaryValue = new byte[packedSummaryLength];

                        for (int i = 0; i < numPoints; i++) {
                            getValue(i, scratch);
                            assert scratch.length == packedValue.length;
                            System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
                            //visitor.visit(getDocID(i), packedValue);

                            final int offset =  packedSummaryLength * ords[i];
                            summaryBytesReader.fillSlice(summaryScratch, offset, packedSummaryLength);
                            System.arraycopy(summaryScratch.bytes, summaryScratch.offset, packedSummaryValue, 0,
                                    packedSummaryLength);
                            visitor.visit(getDocID(i), packedValue, packedSummaryValue);
                        }
                    }

                    @Override
                    public void swap(int i, int j) {
                        int tmp = ords[i];
                        ords[i] = ords[j];
                        ords[j] = tmp;
                    }

                    @Override
                    public int getDocID(int i) {
                        return docIDs[ords[i]];
                    }

                    @Override
                    public void getValue(int i, BytesRef packedValue) {
                        final long offset = (long) packedBytesLength * ords[i];
                        bytesReader.fillSlice(packedValue, offset, packedBytesLength);
                    }

                    @Override
                    public byte getByteAt(int i, int k) {
                        final long offset = (long) packedBytesLength * ords[i] + k;
                        return bytesReader.getByte(offset);
                    }

                    @Override
                    public void save(int i, int j) {
                        if (temp == null) {
                            temp = new int[ords.length];
                        }
                        temp[j] = ords[i];
                    }

                    @Override
                    public void restore(int i, int j) {
                        if (temp != null) {
                            System.arraycopy(temp, i, ords, i, j - i);
                        }
                    }
                };

        final PointValues.PointTree values;
        if (sortMap == null) {
            values = points;
        } else {
            values = new MutableSortingPointValues(points, sortMap);
        }
        PointsReader reader =
                new PointsReader() {
                    @Override
                    public PointValues getValues(String fieldName) {
                        if (fieldName.equals(fieldInfo.name) == false) {
                            throw new IllegalArgumentException("fieldName must be the same");
                        }
                        return new PointValues() {
                            @Override
                            public PointTree getPointTree() throws IOException {
                                return values;
                            }

                            @Override
                            public byte[] getMinPackedValue() throws IOException {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public byte[] getMaxPackedValue() throws IOException {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public int getNumDimensions() throws IOException {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public int getNumIndexDimensions() throws IOException {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public int getBytesPerDimension() throws IOException {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public long size() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public int getDocCount() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override
                    public void checkIntegrity() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
        writer.writeField(fieldInfo, reader);
    }
}
