/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package org.apache.lucene.codecs.lucene90.fsst;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Utility to extract FSSTCompressedAccess from a LeafReaderContext, bypassing all wrappers.
 */
public final class FSSTAccessUtil {

    private static final Logger LOG = Logger.getLogger(FSSTAccessUtil.class.getName());

    private FSSTAccessUtil() {}

    public static FSSTCompressedAccess getCompressedAccess(LeafReaderContext ctx, String fieldName) {
        try {
            LeafReader reader = unwrapToCodecReader(ctx.reader());
            LOG.info("[FSST] unwrapped=" + reader.getClass().getName()
                + " isCodecReader=" + (reader instanceof CodecReader));
            if (reader instanceof CodecReader codecReader) {
                SortedDocValues sdv = codecReader.getSortedDocValues(fieldName);
                if (sdv instanceof FSSTCompressedAccess f && f.hasCompressedAccess()) {
                    LOG.info("[FSST] Found via SortedDocValues");
                    return f;
                }
                SortedSetDocValues ssdv = codecReader.getSortedSetDocValues(fieldName);
                LOG.info("[FSST] ssdv=" + (ssdv != null ? ssdv.getClass().getName() : "null")
                    + " isFSST=" + (ssdv instanceof FSSTCompressedAccess));
                if (ssdv instanceof FSSTCompressedAccess f2 && f2.hasCompressedAccess()) {
                    LOG.info("[FSST] Found via SortedSetDocValues");
                    return f2;
                }
                if (ssdv != null) {
                    SortedDocValues inner = DocValues.unwrapSingleton(ssdv);
                    LOG.info("[FSST] inner=" + (inner != null ? inner.getClass().getName() : "null")
                        + " isFSST=" + (inner instanceof FSSTCompressedAccess));
                    if (inner instanceof FSSTCompressedAccess f3 && f3.hasCompressedAccess()) {
                        LOG.info("[FSST] Found via unwrapped singleton");
                        return f3;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warning("[FSST] error: " + e);
        }
        return null;
    }

    private static LeafReader unwrapToCodecReader(LeafReader reader) {
        while (reader instanceof FilterLeafReader flr) {
            reader = flr.getDelegate();
        }
        return reader;
    }
}
