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

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/** Interface for accessing FSST-compressed term bytes in the doc values term dictionary. */
public interface FSSTCompressedAccess {

  /** Returns the compressed bytes for the given ordinal. The returned BytesRef may be reused. */
  BytesRef lookupCompressedOrd(long ord) throws IOException;

  /** Decompresses the compressed bytes into output. Returns decompressed length. */
  int decompress(BytesRef compressed, byte[] output) throws IOException;

  /** Returns true if this instance supports compressed access. */
  boolean hasCompressedAccess();

  /**
   * Extracts FSSTCompressedAccess from a SortedSetDocValues, unwrapping singleton wrappers.
   * Returns null if the underlying doc values don't support compressed access.
   */
  static FSSTCompressedAccess unwrap(SortedSetDocValues dv) {
    if (dv instanceof FSSTCompressedAccess f && f.hasCompressedAccess()) return f;
    SortedDocValues single = DocValues.unwrapSingleton(dv);
    if (single instanceof FSSTCompressedAccess f && f.hasCompressedAccess()) return f;
    return null;
  }
}
