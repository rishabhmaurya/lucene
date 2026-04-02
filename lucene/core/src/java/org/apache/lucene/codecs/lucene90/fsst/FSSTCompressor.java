/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package org.apache.lucene.codecs.lucene90.fsst;

/**
 * FSST compressor: greedy longest-match encoding. Uses per-first-byte lists for symbol lookup.
 */
public final class FSSTCompressor {

  private final FSSTSymbolTable table;
  // For each possible first byte, list of symbol codes that start with that byte
  private final int[][] codesByFirstByte = new int[256][];

  public FSSTCompressor(FSSTSymbolTable table) {
    this.table = table;
    // Count symbols per first byte
    int[] counts = new int[256];
    for (int c = 0; c < FSSTSymbolTable.MAX_SYMBOLS; c++) {
      if (table.symbolLength(c) > 0) {
        counts[table.firstByte(c) & 0xFF]++;
      }
    }
    // Allocate arrays
    for (int b = 0; b < 256; b++) {
      codesByFirstByte[b] = new int[counts[b]];
      counts[b] = 0; // reuse as index
    }
    // Fill
    for (int c = 0; c < FSSTSymbolTable.MAX_SYMBOLS; c++) {
      if (table.symbolLength(c) > 0) {
        int fb = table.firstByte(c) & 0xFF;
        codesByFirstByte[fb][counts[fb]++] = c;
      }
    }
  }

  public int compress(byte[] input, int off, int len, byte[] output) {
    int pos = off, end = off + len, outPos = 0;
    while (pos < end) {
      int fb = input[pos] & 0xFF;
      int bestCode = -1, bestLen = 0;
      for (int c : codesByFirstByte[fb]) {
        int sLen = table.symbolLength(c);
        if (sLen > bestLen && pos + sLen <= end) {
          boolean match = true;
          for (int j = 1; j < sLen; j++) {
            if (input[pos + j] != table.symbolByteAt(c, j)) {
              match = false;
              break;
            }
          }
          if (match) {
            bestCode = c;
            bestLen = sLen;
          }
        }
      }
      if (bestCode >= 0) {
        output[outPos++] = (byte) bestCode;
        pos += bestLen;
      } else {
        output[outPos++] = (byte) FSSTSymbolTable.ESCAPE;
        output[outPos++] = input[pos++];
      }
    }
    return outPos;
  }
}
