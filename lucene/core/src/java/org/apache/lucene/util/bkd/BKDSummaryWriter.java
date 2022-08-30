package org.apache.lucene.util.bkd;

import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.codecs.MutableSummaryPointTree;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class BKDSummaryWriter extends BKDWriter {

    public BKDSummaryWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, BKDConfig config,
                            double maxMBSortInHeap, long totalPointCount) {
        super(maxDoc, tempDir, tempFileNamePrefix, config, maxMBSortInHeap, totalPointCount);
    }

    public Runnable writeField(
            IndexOutput metaOut,
            IndexOutput indexOut,
            IndexOutput dataOut,
            IndexOutput summaryIndexOut,
            IndexOutput summaryDataOut,
            String fieldName,
            MutableSummaryPointTree reader,
            SummaryMergeFunction<?> mergeSummary)
            throws IOException {
        assert config.numDims == 1;
        return writeField1Dim(metaOut, indexOut, dataOut, summaryIndexOut, summaryDataOut, fieldName, reader, mergeSummary);
    }

    Runnable writeField1Dim(
            IndexOutput metaOut,
            IndexOutput indexOut,
            IndexOutput dataOut,
            IndexOutput summaryIndexOut,
            IndexOutput summaryDataOut,
            String fieldName,
            MutablePointTree reader,
            SummaryMergeFunction<?> mergeSummary)
            throws IOException {
        MutablePointTreeReaderUtils.sort(config, getMaxDoc(), reader, 0, Math.toIntExact(reader.size()));

        final OneDimensionBKDSummaryWriter oneDimWriter =
                new OneDimensionBKDSummaryWriter(metaOut, indexOut, dataOut, summaryIndexOut, summaryDataOut,
                        mergeSummary);

        reader.visitDocValues(
                new PointValues.IntersectVisitor() {

                    @Override
                    public void visit(int docID, byte[] packedValue) throws IOException {
                        oneDimWriter.add(packedValue, docID);
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue, byte[] summaryData) throws IOException {
                        oneDimWriter.add(packedValue, summaryData, docID);
                    }

                    @Override
                    public void visit(int docID) {
                        throw new IllegalStateException();
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                });

        return oneDimWriter.finish();
    }

    class OneDimensionBKDSummaryWriter extends OneDimensionBKDWriter {

        IndexOutput summaryDataOut;
        IndexOutput summaryIndexOut;

        long summaryIndexFP, summaryDataFP;

        final List<Long> summaryLeafBlockFPs = new ArrayList<>();
        final List<byte[]> leafBlockSummary = new ArrayList<>();
        byte[] summaryLeafNodeValue;
        SummaryMergeFunction<?> mergeFunction;
        int summaryBytesLength = 0;
        long packedDataLength = 0;
        OneDimensionBKDSummaryWriter(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, IndexOutput summaryIndexOut,
                              IndexOutput summaryDataOut, SummaryMergeFunction<?> mergeFunction) {
            super(metaOut, indexOut, dataOut);
            this.summaryDataOut = summaryDataOut;
            this.summaryIndexOut = summaryIndexOut;
            this.summaryBytesLength = mergeFunction.getSummarySize();
            this.summaryIndexFP = summaryIndexOut.getFilePointer();
            this.summaryDataFP = summaryDataOut.getFilePointer();

            this.mergeFunction = mergeFunction;
            this.summaryLeafNodeValue = new byte[config.maxPointsInLeafNode * summaryBytesLength];
            packedDataLength = 0;
        }

        void add(byte[] packedValue, byte[] summaryValue, int docID) throws IOException {
            int leafCount = getLeafCount();
            add(packedValue, docID);
            addSummary( leafCount + 1 , summaryValue);
        }

        @Override
        public Runnable finish() throws IOException {
            int leftOverPoints = getLeafCount();
            Runnable bkdWriterFinishResult = super.finish();
            if (leftOverPoints > 0) {
                writeLeafBlockSummaryData(leftOverPoints);
            }

            if (valueCount == 0) {
                return null;
            }

            pointCount = valueCount;

            BKDTreeSummaryLeafNodes summaryLeafNodes = new BKDTreeSummaryLeafNodes() {
                @Override
                public long getSummaryLeafFP(int index) {
                    return summaryLeafBlockFPs.get(index);
                }

                @Override
                public byte[] getLeafSummary(int index) {
                    return leafBlockSummary.get(index);
                }
            };
            int numLeaves = summaryLeafBlockFPs.size();
            return (() -> {
                try {
                    bkdWriterFinishResult.run();
                    writeSummaryIndex(metaOut, summaryIndexOut, numLeaves, summaryLeafNodes, summaryBytesLength,
                            summaryIndexFP);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }


        private void addSummary(int leafCount, byte[] summaryValue) throws IOException {
            System.arraycopy(
                    summaryValue,
                    0,
                    summaryLeafNodeValue,
                    (leafCount - 1) * summaryBytesLength,
                    summaryBytesLength);
            if (leafCount == config.maxPointsInLeafNode) {
                writeLeafBlockSummaryData(leafCount);
            }
        }

        void writeLeafBlockSummaryData(int leafCount) throws IOException {
            if (summaryLeafNodeValue.length == 0) {
                return;
            }
            summaryLeafBlockFPs.add(summaryDataOut.getFilePointer());
            // summaryLeafOut.writeBytes(summaryLeafNodeValue, 0, summaryBytesLength*config.maxPointsInLeafNode);
            byte[] cumulativeLeafSummary = new byte[summaryBytesLength];
            byte[] currentValue = new byte[summaryBytesLength];

            System.arraycopy(summaryLeafNodeValue, 0, cumulativeLeafSummary,0, summaryBytesLength);
            // TODO compress the bytes before writing the summary data
            summaryDataOut.writeBytes(summaryLeafNodeValue, summaryBytesLength*leafCount);
            // summaryDataOut.writeInt(leafCount);
            for (int i = 1; i < leafCount; i++) {
                System.arraycopy(summaryLeafNodeValue, i*summaryBytesLength, currentValue,0, summaryBytesLength);
                mergeFunction.merge(cumulativeLeafSummary, currentValue, cumulativeLeafSummary);
                //summaryDataOut.writeBytes(currentValue, summaryBytesLength);
            }
            packedDataLength += summaryBytesLength*leafCount;
            leafBlockSummary.add(cumulativeLeafSummary);
        }

        private void writeSummary(IndexOutput metaOut, IndexOutput summaryIndexOut, int summaryBytesLength,
                                  byte[] packedSummary, long summaryIndexFP)
                throws IOException {
            metaOut.writeVInt(summaryBytesLength);
            metaOut.writeLong(packedSummary.length);
            metaOut.writeLong(packedDataLength);

            metaOut.writeLong(summaryIndexFP);
            metaOut.writeLong(summaryDataFP);

            summaryIndexOut.writeBytes(packedSummary, 0, packedSummary.length);
        }

        private void writeSummaryIndex(
                IndexOutput metaOut,
                IndexOutput summaryIndexOut,
                int numLeaves,
                BKDTreeSummaryLeafNodes summaryLeafNodes,
                int summaryBytesLength,
                long summaryIndexFP)
                throws IOException {
            byte[] packedSummary = packSummary(numLeaves, summaryLeafNodes, summaryBytesLength);
            writeSummary(metaOut, summaryIndexOut, summaryBytesLength, packedSummary, summaryIndexFP);
        }

        private byte[] packSummary(int numLeafNodes, BKDTreeSummaryLeafNodes summaryLeafNodes, int summaryBytesLength)
                throws IOException {
            ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();
            List<byte[]> blocks = new ArrayList<>();
            int totalSize = recursePackSummary(numLeafNodes, 0, summaryLeafNodes, writeBuffer,
                    summaryBytesLength, new byte[summaryBytesLength], blocks);

            // Compact the byte[] blocks into single byte index:
            byte[] summary = new byte[totalSize];
            int upto = 0;
            for (byte[] block : blocks) {
                System.arraycopy(block, 0, summary, upto, block.length);
                upto += block.length;
            }
            assert upto == totalSize;
            return summary;
        }

        private int recursePackSummary(
                int numLeaves,
                int leavesOffset,
                BKDTreeSummaryLeafNodes summaryLeafNodes,
                ByteBuffersDataOutput writeBuffer,
                int summaryBytesLength,
                byte[] nodeSummary, List<byte[]> blocks) throws IOException {

            if (numLeaves == 1) {
                System.arraycopy(summaryLeafNodes.getLeafSummary(leavesOffset), 0, nodeSummary, 0, summaryBytesLength);
                writeBuffer.writeBytes(nodeSummary);
                //writeBuffer.writeLong(summaryLeafNodes.getSummaryLeafFP(leavesOffset));
                return appendBlock(writeBuffer, blocks);
            } else {
                int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
                byte[] leftSummary = new byte[summaryBytesLength];
                int leftBytes = recursePackSummary(numLeftLeafNodes, leavesOffset, summaryLeafNodes, writeBuffer,
                        summaryBytesLength, leftSummary, blocks);
                byte[] rightSummary = new byte[summaryBytesLength];
                int rightBytes = recursePackSummary(numLeaves - numLeftLeafNodes,
                        leavesOffset + numLeftLeafNodes, summaryLeafNodes, writeBuffer, summaryBytesLength, rightSummary, blocks);
                mergeFunction.merge(leftSummary, rightSummary, nodeSummary);
                // System.arraycopy(leftSummary, 0, nodeSummary, 0, summaryBytesLength);
                writeBuffer.writeBytes(nodeSummary);
                return leftBytes + rightBytes + appendBlock(writeBuffer, blocks);
            }
        }
    }
    private interface BKDTreeSummaryLeafNodes {
        long getSummaryLeafFP(int index);
        byte[] getLeafSummary(int index);

    }

    public interface SummaryMergeFunction<T> {
        int getSummarySize(); // in bytes

        /**
         * Merge a and b into c
         */
        void merge(byte[] a, byte[] b, byte[] c);
        T unpackBytes(byte[] val);
        void packBytes(T val, byte[] res);
    }
}
