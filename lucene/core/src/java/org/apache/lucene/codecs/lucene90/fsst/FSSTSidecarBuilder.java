/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package org.apache.lucene.codecs.lucene90.fsst;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Offline tool that reads existing Lucene segments and creates FSST sidecar files (.fdvd/.fdvm)
 * for specified keyword fields. No reindexing required.
 *
 * <p>Usage: java FSSTSidecarBuilder &lt;indexPath&gt; &lt;symbolTableBasePath&gt; &lt;field1&gt; [field2] ...
 */
public class FSSTSidecarBuilder {

    public static final String FSST_DATA_CODEC = "Lucene90FSSTPDocValuesData";
    public static final String FSST_DATA_EXTENSION = "fdvd";
    public static final String FSST_META_CODEC = "Lucene90FSSTPDocValuesMetadata";
    public static final String FSST_META_EXTENSION = "fdvm";
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: FSSTSidecarBuilder <indexPath> <symbolTableBasePath> <sidecarOutputDir> <field1> [field2] ...");
            System.exit(1);
        }

        Path indexPath = Path.of(args[0]);
        Path symbolTableBasePath = Path.of(args[1]);
        Path sidecarOutputDir = Path.of(args[2]);
        Files.createDirectories(sidecarOutputDir);
        Set<String> fields = Set.of(java.util.Arrays.copyOfRange(args, 3, args.length));

        try (MMapDirectory dir = new MMapDirectory(indexPath);
             MMapDirectory outDir = new MMapDirectory(sidecarOutputDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            for (LeafReaderContext ctx : reader.leaves()) {
                SegmentReader segReader = (SegmentReader) ctx.reader();
                SegmentCommitInfo info = segReader.getSegmentInfo();
                String segmentName = info.info.name;

                for (String field : fields) {
                    Path tablePath = symbolTableBasePath.resolve(field + ".fsst");
                    if (!Files.exists(tablePath)) {
                        System.err.println("Symbol table not found: " + tablePath);
                        continue;
                    }

                    // Try SortedDocValues first, then SortedSetDocValues
                    SortedDocValues sorted = segReader.getSortedDocValues(field);
                    SortedSetDocValues sortedSet = sorted != null ? null : segReader.getSortedSetDocValues(field);

                    long valueCount;
                    if (sorted != null) {
                        valueCount = sorted.getValueCount();
                    } else if (sortedSet != null) {
                        valueCount = sortedSet.getValueCount();
                    } else {
                        System.out.println("  [" + segmentName + "] Field '" + field + "' has no sorted doc values, skipping");
                        continue;
                    }

                    System.out.println("  [" + segmentName + "] Building FSST sidecar for '" + field
                        + "' (" + valueCount + " unique terms)");

                    buildSidecar(outDir, segmentName, field, sorted, sortedSet, valueCount, tablePath);
                }
            }
        }
        System.out.println("Done.");
    }

    static void buildSidecar(
        Directory dir,
        String segmentName,
        String field,
        SortedDocValues sorted,
        SortedSetDocValues sortedSet,
        long valueCount,
        Path tablePath
    ) throws IOException {
        FSSTSymbolTable symbolTable = FSSTSymbolTable.load(tablePath);
        FSSTCompressor compressor = new FSSTCompressor(symbolTable);

        // File names: <segment>_<field>.fdvd / .fdvm
        String fileBase = segmentName + "_" + field;
        String dataFileName = fileBase + "." + FSST_DATA_EXTENSION;
        String metaFileName = fileBase + "." + FSST_META_EXTENSION;

        byte[] tableBytes = symbolTable.toBytes();
        byte[] compressBuf = new byte[65536];
        int maxTermLength = 0;

        try (IndexOutput dataOut = dir.createOutput(dataFileName, IOContext.DEFAULT);
             IndexOutput metaOut = dir.createOutput(metaFileName, IOContext.DEFAULT)) {

            // Write codec headers
            CodecUtil.writeIndexHeader(dataOut, FSST_DATA_CODEC, 0,
                new byte[16], ""); // dummy segment ID and suffix
            CodecUtil.writeIndexHeader(metaOut, FSST_META_CODEC, 0,
                new byte[16], "");

            // Meta: field name + term count
            metaOut.writeString(field);
            metaOut.writeVLong(valueCount);

            // Data: symbol table
            metaOut.writeVInt(tableBytes.length);
            dataOut.writeBytes(tableBytes, 0, tableBytes.length);

            // Per-term offsets
            metaOut.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
            ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
            try (ByteBuffersIndexOutput addressOutput =
                     new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
                DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                    metaOut, addressOutput, valueCount + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);

                long dataStart = dataOut.getFilePointer();

                // Compress each term
                for (long ord = 0; ord < valueCount; ord++) {
                    writer.add(dataOut.getFilePointer() - dataStart);

                    BytesRef term;
                    if (sorted != null) {
                        term = sorted.lookupOrd((int) ord);
                    } else {
                        term = sortedSet.lookupOrd(ord);
                    }

                    int compLen = compressor.compress(term.bytes, term.offset, term.length, compressBuf);
                    dataOut.writeBytes(compressBuf, 0, compLen);
                    maxTermLength = Math.max(maxTermLength, term.length);
                }

                // Sentinel
                writer.add(dataOut.getFilePointer() - dataStart);
                writer.finish();

                // Meta: maxTermLength, data offsets
                metaOut.writeInt(maxTermLength);
                metaOut.writeLong(dataStart);
                metaOut.writeLong(dataOut.getFilePointer() - dataStart);

                // Write address data
                long addrStart = dataOut.getFilePointer();
                addressBuffer.copyTo(dataOut);
                metaOut.writeLong(addrStart);
                metaOut.writeLong(dataOut.getFilePointer() - addrStart);
            }

            // Write footers
            CodecUtil.writeFooter(metaOut);
            CodecUtil.writeFooter(dataOut);
        }

        long dataSize = dir.fileLength(dataFileName);
        long metaSize = dir.fileLength(metaFileName);
        System.out.println("    Created " + dataFileName + " (" + dataSize + " bytes) + "
            + metaFileName + " (" + metaSize + " bytes)");
        System.out.println("    Terms: " + valueCount + ", max term length: " + maxTermLength);
    }
}
