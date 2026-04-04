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

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 * Microbenchmark comparing LZ4 vs FSST doc values term dictionary performance. Supports real
 * Wikipedia data via -p dataFile=/path/to/enwiki-lines.txt or falls back to synthetic data.
 *
 * <p>Run with real data:
 *
 * <pre>
 * java --add-modules jdk.incubator.vector -jar lucene-benchmark-jmh.jar \
 *   FSSTDocValuesBenchmark -p dataFile=/path/to/enwiki.txt -p numTerms=50000
 * </pre>
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

  @Param({"50000"})
  int numTerms;

  /** Path to enwiki line docs file. Empty string uses synthetic data. */
  @Param({""})
  String dataFile;

  private Path tempDir;
  private MMapDirectory directory;
  private DirectoryReader reader;
  private SortedDocValues docValues;
  private int valueCount;
  private int[] randomOrds;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    List<String> terms =
        (dataFile != null && !dataFile.isEmpty())
            ? loadTerms(dataFile, numTerms)
            : generateSyntheticTerms(numTerms);

    tempDir = Files.createTempDirectory("fsst-bench");
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

    // Index and measure indexing time
    long rawBytes = 0;
    long indexStart = System.nanoTime();
    IndexWriterConfig conf = new IndexWriterConfig().setCodec(codec);
    conf.setUseCompoundFile(false); // separate files for size measurement
    try (IndexWriter writer = new IndexWriter(directory, conf)) {
      for (String term : terms) {
        Document doc = new Document();
        byte[] bytes = term.getBytes(StandardCharsets.UTF_8);
        rawBytes += bytes.length;
        doc.add(new SortedDocValuesField("field", new BytesRef(bytes)));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
    long indexMs = (System.nanoTime() - indexStart) / 1_000_000;

    // Measure file sizes
    long dvdSize = 0, dvmSize = 0;
    try (var stream = Files.walk(tempDir)) {
      for (var f : stream.toList()) {
        String name = f.getFileName().toString();
        if (name.endsWith(".dvd")) dvdSize += Files.size(f);
        if (name.endsWith(".dvm")) dvmSize += Files.size(f);
      }
    }

    reader = DirectoryReader.open(directory);
    docValues = reader.leaves().get(0).reader().getSortedDocValues("field");
    valueCount = docValues.getValueCount();

    Random rng = new Random(42);
    randomOrds = new int[valueCount];
    for (int i = 0; i < randomOrds.length; i++) {
      randomOrds[i] = rng.nextInt(valueCount);
    }

    // Report stats
    long totalSize = dvdSize + dvmSize;
    System.out.printf(
        "%n[%s] docs=%d uniqueTerms=%d rawBytes=%,d dvd+dvm=%,d ratio=%.1f%% indexTime=%dms%n",
        mode,
        terms.size(),
        valueCount,
        rawBytes,
        totalSize,
        totalSize * 100.0 / rawBytes,
        indexMs);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    for (var f : Files.list(tempDir).toList()) Files.deleteIfExists(f);
    Files.deleteIfExists(tempDir);
  }

  /** Sequential lookupOrd: iterate all ordinals in order. */
  @Benchmark
  public void sequentialLookupOrd(Blackhole bh) throws Exception {
    for (int ord = 0; ord < valueCount; ord++) {
      bh.consume(docValues.lookupOrd(ord));
    }
  }

  /** Strided lookupOrd: every 4th ordinal (simulates filtered scan). */
  @Benchmark
  public void stridedLookupOrd(Blackhole bh) throws Exception {
    for (int ord = 0; ord < valueCount; ord += 4) {
      bh.consume(docValues.lookupOrd(ord));
    }
  }

  /** Clustered lookupOrd: bursts of varying length with gaps (simulates aggregation). */
  @Benchmark
  public void clusteredLookupOrd(Blackhole bh) throws Exception {
    final int[] burstLens = {3, 7, 12, 5, 20, 2, 9, 15, 4, 8};
    final int gap = 50;
    int ord = 0;
    int burstIdx = 0;
    while (ord < valueCount) {
      int burstLen = burstLens[burstIdx % burstLens.length];
      int burstEnd = Math.min(ord + burstLen, valueCount);
      for (int i = ord; i < burstEnd; i++) {
        bh.consume(docValues.lookupOrd(i));
      }
      ord = burstEnd + gap;
      burstIdx++;
    }
  }

  /** Random lookupOrd: access ordinals in random order. */
  @Benchmark
  public void randomLookupOrd(Blackhole bh) throws Exception {
    for (int ord : randomOrds) {
      bh.consume(docValues.lookupOrd(ord));
    }
  }

  /** FSST-only: random compressed ordinal access (no decompression). Skipped for LZ4. */
  @Benchmark
  public void randomLookupCompressedOrd(Blackhole bh) throws Exception {
    if (docValues instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
      for (int ord : randomOrds) {
        bh.consume(fsst.lookupCompressedOrd(ord));
      }
    }
  }

  /** FSST-only: sequential compressed ordinal access (no decompression). Skipped for LZ4. */
  @Benchmark
  public void sequentialLookupCompressedOrd(Blackhole bh) throws Exception {
    if (docValues instanceof FSSTCompressedAccess fsst && fsst.hasCompressedAccess()) {
      for (int ord = 0; ord < valueCount; ord++) {
        bh.consume(fsst.lookupCompressedOrd(ord));
      }
    }
  }

  /** Load terms from a tab-delimited file. Detects enwiki (field 1) vs geonames (fields 2+9+18). */
  private static List<String> loadTerms(String path, int maxTerms) throws Exception {
    boolean isGeonames = path.contains("allCountries");
    List<String> terms = new ArrayList<>();
    try (var br = new BufferedReader(new FileReader(path, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null && terms.size() < maxTerms) {
        if (line.startsWith("FIELDS_HEADER")) continue;
        if (isGeonames) {
          // Parse only needed fields by finding tab positions
          int t1 = line.indexOf('\t');
          if (t1 < 0) continue;
          int t2 = line.indexOf('\t', t1 + 1);
          if (t2 < 0) continue;
          String name = line.substring(t1 + 1, t2);
          // Find field 9 (country code) — skip to 8th tab
          int pos = t2;
          for (int f = 2; f < 8 && pos >= 0; f++) pos = line.indexOf('\t', pos + 1);
          if (pos < 0) continue;
          int t9 = line.indexOf('\t', pos + 1);
          if (t9 < 0) continue;
          String country = line.substring(pos + 1, t9);
          // Find field 18 (timezone) — skip to 17th tab
          for (int f = 9; f < 17 && t9 >= 0; f++) t9 = line.indexOf('\t', t9 + 1);
          if (t9 < 0) continue;
          int t18 = line.indexOf('\t', t9 + 1);
          String tz = (t18 >= 0) ? line.substring(t9 + 1, t18) : line.substring(t9 + 1);
          terms.add(name + ", " + country + ", " + tz);
        } else {
          int tab = line.indexOf('\t');
          if (tab > 0) terms.add(line.substring(0, tab));
        }
      }
    }
    return terms;
  }

  /** Generate synthetic URL-like terms as fallback. */
  private static List<String> generateSyntheticTerms(int count) {
    List<String> terms = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      terms.add(
          "http://example" + (i % 100) + ".com/page/" + i + "/detail?id=" + (i * 7 % 9999));
    }
    return terms;
  }
}
