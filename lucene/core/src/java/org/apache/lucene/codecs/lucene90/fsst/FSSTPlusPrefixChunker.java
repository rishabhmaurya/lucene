/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene90.fsst;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Finds optimal prefix-sharing chunks within a block of sorted terms using dynamic programming.
 *
 * <p>Given B sorted terms, partitions them into contiguous chunks where each chunk shares a common
 * prefix (the LCP of all terms in the chunk). The DP minimizes total compressed size accounting for
 * prefix storage, per-string overhead, and estimated suffix sizes.
 *
 * <p>Based on "FSST+: Enhancing String Compression Through Common Prefix Extraction" (Yan Lanna
 * Alexandre, CWI, 2025).
 */
public final class FSSTPlusPrefixChunker {

  /** Block size — number of terms processed together. Fits in L1 cache. */
  public static final int BLOCK_SIZE = 128;

  /** Per-string overhead when prefix is used: 1 byte prefix_len + 2 bytes jump_back_offset. */
  private static final int PREFIX_OVERHEAD = 3;

  /** Per-string overhead when no prefix: 1 byte prefix_len (=0). */
  private static final int NO_PREFIX_OVERHEAD = 1;

  /** Minimum prefix length to justify the overhead. */
  private static final int MIN_PREFIX_LEN = 4;

  private static final VarHandle VH_LE_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private final int[] lcps = new int[BLOCK_SIZE - 1];
  private final int[] dp = new int[BLOCK_SIZE + 1];
  private final int[] parent = new int[BLOCK_SIZE + 1];
  private final int[] chunkStartBuf = new int[BLOCK_SIZE];
  private final int[] chunkEndBuf = new int[BLOCK_SIZE];
  private final int[] chunkPrefixLenBuf = new int[BLOCK_SIZE];
  private int numChunks;

  /** Compute LCP between two byte ranges using 8-byte-at-a-time comparison. */
  static int lcp(byte[] buf, int aOff, int aLen, int bOff, int bLen) {
    int limit = Math.min(aLen, bLen);
    int i = 0;
    for (; i + 8 <= limit; i += 8) {
      long av = (long) VH_LE_LONG.get(buf, aOff + i);
      long bv = (long) VH_LE_LONG.get(buf, bOff + i);
      if (av != bv) {
        return i + Long.numberOfTrailingZeros(av ^ bv) / 8;
      }
    }
    for (; i < limit; i++) {
      if (buf[aOff + i] != buf[bOff + i]) return i;
    }
    return limit;
  }

  /**
   * Find optimal prefix chunks for a block of sorted terms.
   *
   * @param buf buffer containing all terms concatenated
   * @param offsets offsets[i] = start of term i in buf; offsets[n] = end of last term
   * @param n number of terms in this block (≤ BLOCK_SIZE)
   * @param fsstRatio estimated FSST compression ratio (e.g. 0.67)
   * @return number of chunks found
   */
  public int solve(byte[] buf, int[] offsets, int n, float fsstRatio) {
    if (n <= 0) {
      numChunks = 0;
      return 0;
    }
    if (n == 1) {
      numChunks = 1;
      chunkStartBuf[0] = 0;
      chunkEndBuf[0] = 1;
      chunkPrefixLenBuf[0] = 0;
      return 1;
    }

    // Step 1: compute adjacent LCPs
    for (int i = 0; i < n - 1; i++) {
      lcps[i] = lcp(buf, offsets[i], offsets[i + 1] - offsets[i], offsets[i + 1], offsets[i + 2] - offsets[i + 1]);
    }

    // Precompute prefix sum of term lengths for O(1) range sum
    // cumLen[i] = sum of term lengths for terms [0..i)
    long[] cumLen = new long[n + 1];
    for (int i = 0; i < n; i++) {
      cumLen[i + 1] = cumLen[i] + (offsets[i + 1] - offsets[i]);
    }

    // Step 2: DP — dp[i] = min estimated compressed size for terms [0..i)
    dp[0] = 0;
    for (int i = 1; i <= n; i++) {
      dp[i] = Integer.MAX_VALUE;
      int minLcp = Integer.MAX_VALUE;
      for (int j = i - 1; j >= 0; j--) {
        if (j < i - 1) {
          minLcp = Math.min(minLcp, lcps[j]);
        } else {
          minLcp = offsets[j + 1] - offsets[j]; // single term
        }

        int chunkSize = i - j;
        int prefixLen = (chunkSize == 1 || minLcp < MIN_PREFIX_LEN) ? 0 : minLcp;

        // O(1) cost calculation using prefix sums
        long totalTermBytes = cumLen[i] - cumLen[j]; // sum of all term lengths in [j, i)
        int cost;
        if (prefixLen > 0) {
          int prefixCost = Math.max(1, (int) (prefixLen * fsstRatio));
          long suffixBytes = totalTermBytes - (long) chunkSize * prefixLen;
          int suffixCost = Math.max(chunkSize, (int) (suffixBytes * fsstRatio));
          cost = prefixCost + chunkSize * PREFIX_OVERHEAD + suffixCost;
        } else {
          cost = chunkSize * NO_PREFIX_OVERHEAD + Math.max(chunkSize, (int) (totalTermBytes * fsstRatio));
        }

        if (dp[j] != Integer.MAX_VALUE && dp[j] + cost < dp[i]) {
          dp[i] = dp[j] + cost;
          parent[i] = j;
        }

        if (minLcp == 0 && chunkSize > 1) break;
      }
    }

    // Step 3: backtrack
    numChunks = 0;
    int i = n;
    while (i > 0) {
      int j = parent[i];
      chunkStartBuf[numChunks] = j;
      chunkEndBuf[numChunks] = i;
      int chunkSize = i - j;
      if (chunkSize == 1) {
        chunkPrefixLenBuf[numChunks] = 0;
      } else {
        int ml = Integer.MAX_VALUE;
        for (int k = j; k < i - 1; k++) ml = Math.min(ml, lcps[k]);
        chunkPrefixLenBuf[numChunks] = (ml >= MIN_PREFIX_LEN) ? ml : 0;
      }
      numChunks++;
      i = j;
    }

    // Reverse
    for (int a = 0, b = numChunks - 1; a < b; a++, b--) {
      swap(chunkStartBuf, a, b);
      swap(chunkEndBuf, a, b);
      swap(chunkPrefixLenBuf, a, b);
    }
    return numChunks;
  }

  public int numChunks() { return numChunks; }
  public int chunkStart(int c) { return chunkStartBuf[c]; }
  public int chunkEnd(int c) { return chunkEndBuf[c]; }
  public int chunkPrefixLen(int c) { return chunkPrefixLenBuf[c]; }

  private static void swap(int[] arr, int a, int b) {
    int tmp = arr[a]; arr[a] = arr[b]; arr[b] = tmp;
  }
}
