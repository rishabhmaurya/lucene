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
 * Abstraction for reading a sorted term dictionary. Implementations handle different compression
 * schemes (LZ4, FSST, FSST+) while providing a uniform interface for lookupOrd, lookupTerm,
 * and termsEnum.
 */
interface TermsDictReader {

  /** Look up the term at the given ordinal. */
  BytesRef lookupOrd(int ord) throws IOException;

  /**
   * Look up the ordinal for the given term. Returns the ordinal if found, or {@code -1 - insertionPoint}
   * if not found (same contract as {@link java.util.Arrays#binarySearch}).
   */
  long lookupTerm(BytesRef key) throws IOException;

  /** Return a TermsEnum for iterating all terms. Used by OrdinalMap.build during merge. */
  TermsEnum termsEnum() throws IOException;

  /** Return the number of unique terms. */
  long getValueCount();

  /** Create the appropriate reader based on the encoding byte in the term dict entry. */
  static TermsDictReader create(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data, boolean merging)
      throws IOException {
    if (entry.encoding == Lucene90DocValuesConsumer.TERMS_DICT_FSST_PLUS) {
      return new FSSTPlusTermsDictReader(entry, data);
    } else if (entry.encoding == Lucene90DocValuesConsumer.TERMS_DICT_FSST) {
      return new FSSTTermsDictReader(entry, data);
    } else {
      return new LZ4TermsDictReader(entry, data, merging);
    }
  }
}
