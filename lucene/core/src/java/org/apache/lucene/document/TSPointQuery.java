package org.apache.lucene.document;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.bkd.BKDSummaryWriter;
import org.apache.lucene.util.bkd.BKDWithSummaryReader;

import java.io.IOException;


public class TSPointQuery extends PointRangeQuery {

    public int[] diskAccess = {0};
    /**
     * Expert: create a multidimensional range query for point values.
     *
     * @param tsid      field name. must not be {@code null}.
     * @param lowerPoint lower portion of the range (inclusive).
     * @param upperPoint upper portion of the range (inclusive).
     * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length !=
     *                                  upperValue.length}
     */
    public TSPointQuery(String tsid, byte[] lowerPoint, byte[] upperPoint) {
        super(tsid, lowerPoint, upperPoint, 1);
    }

    @Override
    protected String toString(int dimension, byte[] value) {
        return null;
    }

    public byte[] getSummary(BKDWithSummaryReader.BKDSummaryTree pointTree,
                             BKDSummaryWriter.SummaryMergeFunction<?> mergeFunction) throws IOException {
        final byte[][] summary = {null};
        pointTree.visitSummary(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {

            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                // TODO - it will be when leaf node is reached crossing the query, we need to fetch the leaf node block
                // to read the packed value and decide whether to include point or not, read value of metric and
                // aggregate
            }

            @Override
            public void visit(int docID, byte[] packedValue, byte[] packedSummary) throws IOException {
                if (compare(packedValue, packedValue) == PointValues.Relation.CELL_INSIDE_QUERY) {
                    diskAccess[0]++;
                    if (summary[0] == null) {
                        summary[0] = new byte[mergeFunction.getSummarySize()];
                        System.arraycopy(packedSummary, 0, summary[0], 0, mergeFunction.getSummarySize());
                    } else {
                        mergeFunction.merge(summary[0], packedSummary, summary[0]);
                    }
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                boolean crosses;
                ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(minPackedValue.length);

                if (comparator.compare(minPackedValue, 0, upperPoint, 0) > 0
                        || comparator.compare(maxPackedValue, 0, lowerPoint, 0) < 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }

                crosses = comparator.compare(minPackedValue, 0, lowerPoint, 0) < 0
                        || comparator.compare(maxPackedValue, 0, upperPoint, 0) > 0;

                if (crosses) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            }

            @Override
            public void visit(byte[] packedSummary, int firstDocID, int lastDocID) throws IOException {
                diskAccess[0]++;
                if (summary[0] == null) {
                    summary[0] = new byte[mergeFunction.getSummarySize()];
                    System.arraycopy(packedSummary, 0, summary[0], 0, mergeFunction.getSummarySize());
                } else {
                    mergeFunction.merge(summary[0], packedSummary, summary[0]);
                }            }
        }, 0);
        return summary[0];
    }
}
