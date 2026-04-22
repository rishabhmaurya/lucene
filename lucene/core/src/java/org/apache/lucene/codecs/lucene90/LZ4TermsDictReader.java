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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * LZ4 TermsDictReader — delegates to the existing TermsDict (BaseTermsEnum) which handles
 * LZ4 block decompression with front-coding.
 */
final class LZ4TermsDictReader implements TermsDictReader {

  private final Lucene90DocValuesProducer.TermsDictEntry entry;
  private final IndexInput data;
  private final boolean merging;
  private final Lucene90DocValuesProducer.TermsDict termsDict;

  LZ4TermsDictReader(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data, boolean merging)
      throws IOException {
    this.entry = entry;
    this.data = data;
    this.merging = merging;
    this.termsDict = new Lucene90DocValuesProducer.TermsDict(entry, data, merging);
  }

  @Override
  public BytesRef lookupOrd(int ord) throws IOException {
    termsDict.seekExact(ord);
    return termsDict.term();
  }

  @Override
  public long lookupTerm(BytesRef key) throws IOException {
    TermsEnum.SeekStatus status = termsDict.seekCeil(key);
    switch (status) {
      case FOUND:
        return termsDict.ord();
      case NOT_FOUND:
      case END:
      default:
        return -1L - termsDict.ord();
    }
  }

  @Override
  public TermsEnum termsEnum() throws IOException {
    return new Lucene90DocValuesProducer.TermsDict(entry, data, merging);
  }

  @Override
  public long getValueCount() {
    return entry.termsDictSize;
  }
}
