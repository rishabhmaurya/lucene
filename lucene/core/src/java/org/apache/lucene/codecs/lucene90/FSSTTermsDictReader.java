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
 * FSST TermsDictReader — per-term FSST compression with O(1) random access.
 */
final class FSSTTermsDictReader implements TermsDictReader {

  private final Lucene90DocValuesProducer.TermsDictEntry entry;
  private final IndexInput data;
  private final FSSTTermsDict dict;

  FSSTTermsDictReader(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data)
      throws IOException {
    this.entry = entry;
    this.data = data;
    this.dict = new FSSTTermsDict(entry, data);
  }

  @Override
  public BytesRef lookupOrd(int ord) throws IOException {
    return dict.lookupOrd(ord);
  }

  @Override
  public long lookupTerm(BytesRef key) throws IOException {
    TermsEnum.SeekStatus status = dict.seekCeil(key);
    switch (status) {
      case FOUND:
        return dict.ord();
      case NOT_FOUND:
      case END:
      default:
        return -1L - dict.ord();
    }
  }

  @Override
  public TermsEnum termsEnum() throws IOException {
    return dict;
  }

  @Override
  public long getValueCount() {
    return entry.termsDictSize;
  }
}
