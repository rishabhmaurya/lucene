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
package org.apache.lucene.benchmark.jmh;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.TermsDictMode;
import org.apache.lucene.codecs.lucene90.fsst.FSSTCompressedAccess;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark comparing LZ4 vs FSST term dictionary lookupOrd performance. Tests sequential and
 * random access patterns on sorted doc values with URL-like string terms.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(
    value = 1,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules",
      "jdk.incubator.vector"
    })
public class FSSTDocValuesBenchmark {

  @Param({"LZ4", "FSST"})
  String mode;

  @Param({"10000"})
  int numTerms;

  private Path tempDir;
  private MMapDirectory directory;
  private DirectoryReader reader;
  private SortedDocValues docValues;
  private int valueCount;
  private int[] randomOrds;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory("fsst-bench");
    directory = new MMapDirectory(tempDir);

    TermsDictMode termsDictMode =
        mode.equals("FSST") ? TermsDictMode.FSST : TermsDictMode.LZ4;
    Lucene90DocValuesFormat dvFormat = new Lucene90DocValuesFormat(4096, termsDictMode);
    Lucene104Codec codec =
        new Lucene104Codec() {
          @Override
          public DocValuesFormat getDocValuesFormatForField(String field) {
            return dvFormat;
          }
        };

    // Create index with URL-like terms
    IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec);
    try (IndexWriter writer = new IndexWriter(directory, conf)) {
      for (int i = 0; i < numTerms; i++) {
        Document doc = new Document();
        String url =
            "http://example" + (i % 100) + ".com/page/" + i + "/detail?id=" + (i * 7 % 9999);
        doc.add(new SortedDocValuesField("url", new BytesRef(url)));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(directory);
    docValues = reader.leaves().get(0).reader().getSortedDocValues("url");
    valueCount = docValues.getValueCount();

    // Pre-generate random ordinals for random access benchmark
    Random rng = new Random(42);
    randomOrds = new int[10000];
    for (int i = 0; i < randomOrds.length; i++) {
      randomOrds[i] = rng.nextInt(valueCount);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    // Clean up temp files
    for (var f : java.nio.file.Files.list(tempDir).toList()) {
      java.nio.file.Files.deleteIfExists(f);
    }
    java.nio.file.Files.deleteIfExists(tempDir);
  }

  /** Sequential lookupOrd: iterate all ordinals in order. */
  @Benchmark
  public void sequentialLookupOrd(Blackhole bh) throws Exception {
    for (int ord = 0; ord < valueCount; ord++) {
      bh.consume(docValues.lookupOrd(ord));
    }
  }

  /** Random lookupOrd: access ordinals in random order. */
  @Benchmark
  public void randomLookupOrd(Blackhole bh) throws Exception {
    for (int ord : randomOrds) {
      bh.consume(docValues.lookupOrd(ord));
    }
  }

  /** lookupCompressedOrd (FSST only — returns compressed bytes without decompression). */
  @Benchmark
  public void lookupCompressedOrd(Blackhole bh) throws Exception {
    if (docValues instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
      for (int ord : randomOrds) {
        bh.consume(fsst.lookupCompressedOrd(ord));
      }
    } else {
      // Fallback for LZ4 — just do regular lookupOrd
      for (int ord : randomOrds) {
        bh.consume(docValues.lookupOrd(ord));
      }
    }
  }
}
