package org.apache.lucene.util.bkd;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class BKDWithSummaryReader extends BKDReader {

    IndexInput summaryIndexIn;
    IndexInput summaryDataIn;
    IndexInput packedSummary, packedSummaryData;

    int summaryBytesLength;
    long packedSummaryLength, packedSummaryDataLength;
    long summaryIndexFP, summaryDataFP;
    /**
     * Caller must pre-seek the provided {@link IndexInput} to the index location that {@link
     * BKDWriter#finish} returned. BKD tree is always stored off-heap.
     *
     * @param metaIn
     * @param indexIn
     * @param dataIn
     */
    public BKDWithSummaryReader(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn,
                                IndexInput summaryIndexIn, IndexInput summaryDataIn) throws IOException {
        super(metaIn, indexIn, dataIn);
        this.summaryIndexIn = summaryIndexIn;
        this.summaryDataIn = summaryDataIn;
        this.summaryBytesLength = metaIn.readVInt();
        this.packedSummaryLength = metaIn.readLong();
        this.packedSummaryDataLength = metaIn.readLong();

        this.summaryIndexFP = metaIn.readLong();
        this.summaryDataFP = metaIn.readLong();
        summaryIndexIn.seek(summaryIndexFP);
        summaryDataIn.seek(summaryDataFP); // TODO fetch index from metadata
        this.packedSummary = summaryIndexIn.slice("packedSummaryIndex", summaryIndexFP,
                packedSummaryLength);
        this.packedSummaryData = summaryDataIn.slice("packedSummaryData", summaryDataFP,
                packedSummaryDataLength);

    }

    public class BKDSummaryTree extends BKDPointTree {
        IndexInput summaryNodes;
        IndexInput summaryData;

        public BKDSummaryTree(IndexInput innerNodes, IndexInput leafNodes, IndexInput summaryNodes,
                              IndexInput summaryDataIn, BKDConfig config, int numLeaves, int version,
                       long pointCount, byte[] minPackedValue, byte[] maxPackedValue, boolean isTreeBalanced)
                throws IOException {
            super(innerNodes, leafNodes, config, numLeaves, version, pointCount, minPackedValue, maxPackedValue,
                    isTreeBalanced);
            this.summaryNodes = summaryNodes;
            this.summaryData = summaryDataIn;
        }

        public long visitSummary(IntersectVisitor visitor, long summaryBytesOffset) throws IOException {
            Relation r = visitor.compare(this.getMinPackedValue(), this.getMaxPackedValue());
            switch (r) {
                case CELL_OUTSIDE_QUERY:
                    // This cell is fully outside the query shape: return 0 as the count of its nodes
                    return summaryBytesOffset + summarySizeUnderCurrentNode();
                case CELL_INSIDE_QUERY:
                    // This cell is fully inside the query shape: return the size of the entire node as the
                    // count
                    long summarySizeUnderCurrentNode = summarySizeUnderCurrentNode();
                    summaryNodes.seek(Math.max(0, summaryBytesOffset + summarySizeUnderCurrentNode - summaryBytesLength));
                    byte [] summaryBytes = new byte[summaryBytesLength];
                    summaryNodes.readBytes(summaryBytes, 0, summaryBytesLength);
                    visitor.visit(summaryBytes, -1, -1);
                    return summaryBytesOffset + summarySizeUnderCurrentNode;
                case CELL_CROSSES_QUERY:
                    /*
                    The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                    through and do full counting.
                    */
                    if (!this.moveToChild()) {
                        long offset = (long) currNodeLeafOrderOffset() *config.maxPointsInLeafNode*summaryBytesLength;
                        final int[] i = {0};
                        final byte[][] scratch = {new byte[summaryBytesLength]};

                        this.visitDocValues(new IntersectVisitor() {
                            @Override
                            public void visit(int docID) throws IOException {

                            }

                            @Override
                            public void visit(int docID, byte[] packedValue) throws IOException {
                                summaryData.seek(offset + (long) i[0] *summaryBytesLength);
                                summaryData.readBytes(scratch[0], 0, summaryBytesLength);
                                visitor.visit(i[0], packedValue, scratch[0]);
                                i[0]++;
                            }

                            @Override
                            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                                return visitor.compare(minPackedValue, maxPackedValue);
                            }
                        });
                        return summaryBytesOffset + summaryBytesLength;
                    } else {
                        // in-place traversal
                        long leftBytesOffset = visitSummary(visitor, summaryBytesOffset);
                        this.moveToSibling();
                        long rightBytesOffset = visitSummary(visitor, leftBytesOffset);
                        this.moveToParent();
                        return rightBytesOffset + summaryBytesLength;
                    }

                default:
                    throw new IllegalArgumentException("Unreachable code");
            }
        }

        private int currNodeLeafOrderOffset() {
            // leaves = 8
            int totalNodes = numLeaves*2-1; //numNodes = leaves*2 -1 = 15

            int firstOrderedLeaf = totalNodes - leafNodeOffset + 1; // startingLeaf = numNodes - leaves + 1 = 8
            int msb = 31 - Integer.numberOfLeadingZeros(totalNodes);
            int lastLevelLeaves = totalNodes - (1<<msb) + 1;
            // lastLevelLeaves = numNodes - 2^msb(leaves) + 1 = 15 - 2^4 + 1 = 0
            return (nodeID - firstOrderedLeaf + lastLevelLeaves)%numLeaves;
            // ((node - startingLeaf)+lastLevelLeaves)%leaves + 1
        }
        private long summarySizeUnderCurrentNode() {
            int leftMostLeafNode = nodeID;
            int fullLevelUnderCurrentNode = 0;
            while (leftMostLeafNode < leafNodeOffset) {
                leftMostLeafNode = leftMostLeafNode * 2;
                fullLevelUnderCurrentNode++;
            }
            int treeLeftMostNode = 1;
            while (treeLeftMostNode < leafNodeOffset) {
                treeLeftMostNode = treeLeftMostNode * 2;
            }
            int rightMostLeafNode = nodeID;
            while (rightMostLeafNode < leafNodeOffset) {
                rightMostLeafNode = rightMostLeafNode * 2 + 1;
            }

            int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
            // how many leaf nodes are in the full level
            int leavesFullLevel = 1 << lastFullLevel;
            // leaf nodes that do not fit in the full level
            int unbalancedLeafNodes = numLeaves - leavesFullLevel;
            if (rightMostLeafNode >= leftMostLeafNode) {
                return ((1L<<(fullLevelUnderCurrentNode+1)) -1)*summaryBytesLength;
            }
            return ((long) unbalancedLeafNodes*2 - leftMostLeafNode + treeLeftMostNode + (1L<<fullLevelUnderCurrentNode) -1)
                    *summaryBytesLength;
        }
    }

    @Override
    public BKDSummaryTree getPointTree() throws IOException {
        return new BKDSummaryTree(
                packedIndex.clone(),
                this.in.clone(),
                this.packedSummary.clone(),
                this.packedSummaryData.clone(),
                config,
                numLeaves,
                version,
                pointCount,
                minPackedValue,
                maxPackedValue,
                isTreeBalanced);
    }
}
