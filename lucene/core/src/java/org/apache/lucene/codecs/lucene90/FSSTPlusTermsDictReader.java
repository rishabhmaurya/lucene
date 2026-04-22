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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * FSST+ TermsDictReader — DP-based prefix extraction + per-term FSST suffix compression.
 */
final class FSSTPlusTermsDictReader implements TermsDictReader {

  private final Lucene90DocValuesProducer.TermsDictEntry entry;
  private final IndexInput data;
  private final FSSTPlusTermsDict dict;

  FSSTPlusTermsDictReader(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data)
      throws IOException {
    this.entry = entry;
    this.data = data;
    this.dict = new FSSTPlusTermsDict(entry, data);
  }

  @Override
  public BytesRef lookupOrd(int ord) throws IOException {
    return dict.lookupOrd(ord);
  }

  @Override
  public long lookupTerm(BytesRef key) throws IOException {
    long lo = 0, hi = entry.termsDictSize - 1;
    while (lo <= hi) {
      long mid = (lo + hi) >>> 1;
      int cmp = dict.lookupOrd((int) mid).compareTo(key);
      if (cmp < 0) lo = mid + 1;
      else if (cmp > 0) hi = mid - 1;
      else return mid;
    }
    return -1L - lo;
  }

  @Override
  public TermsEnum termsEnum() throws IOException {
    // Reset sequential state for clean iteration
    dict.seqOrd = -1;
    dict.seqLocalIdx = -1;
    dict.seqSuffixPos = -1;
    dict.cachedBlockIdx = -1;
    final long count = entry.termsDictSize;
    return new BaseTermsEnum() {
      long ord = -1;
      BytesRef current;

      @Override
      public BytesRef next() throws IOException {
        if (++ord >= count) return null;
        current = dict.next();
        return current;
      }

      @Override
      public SeekStatus seekCeil(BytesRef text) throws IOException {
        for (long i = 0; i < count; i++) {
          BytesRef t = dict.lookupOrd((int) i);
          int cmp = t.compareTo(text);
          if (cmp == 0) { ord = i; current = t; return SeekStatus.FOUND; }
          if (cmp > 0) { ord = i; current = t; return SeekStatus.NOT_FOUND; }
        }
        ord = count;
        return SeekStatus.END;
      }

      @Override
      public void seekExact(long o) throws IOException { ord = o; current = dict.lookupOrd((int) o); }
      @Override public BytesRef term() { return current; }
      @Override public long ord() { return ord; }
      @Override public int docFreq() { return 0; }
      @Override public long totalTermFreq() { return 0; }
      @Override public PostingsEnum postings(PostingsEnum reuse, int flags) { return null; }
      @Override public ImpactsEnum impacts(int flags) { return null; }
    };
  }

  @Override
  public long getValueCount() {
    return entry.termsDictSize;
  }
}
