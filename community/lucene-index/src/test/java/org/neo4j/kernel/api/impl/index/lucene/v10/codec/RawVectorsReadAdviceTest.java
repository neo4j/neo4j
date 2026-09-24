/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.lucene.v10.codec;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory;
import org.neo4j.kernel.api.impl.index.lucene.v10.LuceneDirectoryReaderAccess;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructureAccess;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/// [Isolated] because the advisor these tests substitute is process-wide static state that
/// [LuceneKnnBinaryQuantizedVectorFormat#fieldsReader] reads, and this module runs test classes
/// concurrently.
@Isolated
class RawVectorsReadAdviceTest {
    /// The codec only marks `.vec` when something can prefetch it -- see
    /// [LuceneKnnBinaryQuantizedVectorFormat#fieldsReader] -- and the advisor that does the
    /// prefetching needs native access, which is Linux only. These tests are about the marking
    /// mechanism rather than about that gate, so they stand an advisor in that declines every
    /// range: available, so the advice is applied, and inert, so nothing else changes.
    @BeforeEach
    void makeAdviceAvailable() {
        RawVectorsReadAdvice.clearRejectedForTesting();
        RawVectorAdvisors.overrideForTesting((slice, offset, length) -> false);
    }

    @AfterEach
    void restoreAdviceAvailability() {
        RawVectorAdvisors.clearOverrideForTesting();
        RawVectorsReadAdvice.clearRejectedForTesting();
    }

    private static final LuceneContext CONTEXT = LuceneContext.LUCENE_10;
    private static final KernelVersion KERNEL_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    private static final VectorIndexVersion VECTOR_INDEX_VERSION =
            VectorIndexVersion.latestSupportedVersion(KERNEL_VERSION);

    private static final int DIMENSIONS = 64;
    private static final int DOCUMENTS = 30;

    private static final String RAW_VECTORS = ".vec";
    private static final String QUANTIZED_VECTORS = ".veq";
    private static final String HNSW_GRAPH = ".vex";
    private static final String COMPOUND = ".cfs";

    @Test
    void adviseNothingByDefault() {
        assertThat(RawVectorsReadAdvice.READ_ADVICE.apply("_0_Foo_0.vec", IOContext.DEFAULT))
                .isEmpty();
    }

    /// The regression this whole class exists to avoid: Lucene opens the graph, the codes and the
    /// raw vectors all with [DataAccessHint#RANDOM], so honouring that hint advises all three --
    /// including the two that need fault-around to warm up.
    @Test
    void ignoreLucenesOwnRandomAccessHint() {
        assertThat(RawVectorsReadAdvice.READ_ADVICE.apply(
                        "_0_Foo_0.veq", IOContext.DEFAULT.withHints(DataAccessHint.RANDOM)))
                .isEmpty();
    }

    @Test
    void markOnlyRawVectors() throws IOException {
        RecordingDirectory opened = new RecordingDirectory(new ByteBuffersDirectory());
        Directory marking = RawVectorsReadAdvice.markRawVectors(opened);

        for (String name : List.of("_0_Foo_0" + RAW_VECTORS, "_0_Foo_0" + QUANTIZED_VECTORS, "_0" + COMPOUND)) {
            try (IndexOutput output = opened.createOutput(name, IOContext.DEFAULT)) {
                output.writeLong(0);
            }
            marking.openInput(name, IOContext.DEFAULT).close();
        }

        assertThat(opened.adviceFor("_0_Foo_0" + RAW_VECTORS)).contains(ReadAdvice.RANDOM);
        assertThat(opened.adviceFor("_0_Foo_0" + QUANTIZED_VECTORS)).isEmpty();
        assertThat(opened.adviceFor("_0" + COMPOUND)).isEmpty();
    }

    /// [IOContext#withHints] *replaces* the hint set rather than adding to it, so marking has to
    /// carry the reader's own hints across. `Lucene99FlatVectorsReader` opens `.vec` with
    /// [FileTypeHint#DATA] and [FileDataHint#KNN_VECTORS]; losing those would stay invisible until
    /// something starts reading them, which is why it is asserted rather than assumed.
    @Test
    void keepTheHintsTheReaderAlreadySet() throws IOException {
        String name = "_0_Foo_0" + RAW_VECTORS;
        ArrayList<IOContext> reaching = new ArrayList<>();

        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
                output.writeLong(0);
            }
            FilterDirectory capturing = new FilterDirectory(directory) {
                @Override
                public IndexInput openInput(String file, IOContext context) throws IOException {
                    reaching.add(context);
                    return in.openInput(file, context);
                }
            };

            RawVectorsReadAdvice.markRawVectors(capturing)
                    .openInput(name, IOContext.DEFAULT.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS))
                    .close();
        }

        assertThat(reaching).singleElement().satisfies(context -> {
            assertThat(context.hints()).contains(FileTypeHint.DATA, FileDataHint.KNN_VECTORS);
            assertThat(RawVectorsReadAdvice.READ_ADVICE.apply(name, context)).contains(ReadAdvice.RANDOM);
        });
    }

    /// The case a file-name predicate cannot express: inside a compound segment the only name
    /// [MMapDirectory] is ever handed is the container's, so the advice has to be decided from the
    /// context that `Lucene90CompoundReader` forwards into `IndexInput.slice`.
    @Test
    void adviseRawVectorsRandomInsideACompoundSegment(@TempDir Path dir) throws IOException {
        List<Request> advice = readAdviceRequestedWhileReading(dir, 1.0);

        assertThat(advice).isNotEmpty();
        assertThat(advice.stream().filter(Request::isRandom))
                .as("advice requested: %s", advice)
                .isNotEmpty()
                .allMatch(request -> request.name.endsWith(COMPOUND));
    }

    /// The safety property, and the reason the two tests above need an advisor at all. `MADV_RANDOM`
    /// costs the kernel's fault-around and the prefetch is what pays it back, so a runtime that
    /// cannot prefetch -- a platform without native access -- must not advise either: it is worse
    /// than doing neither, since the readahead is gone and nothing has queued the reads in its
    /// place.
    @Test
    void neverAdviseWhenNothingCanPrefetch(@TempDir Path dir) throws IOException {
        RawVectorAdvisors.overrideForTesting(null);

        List<Request> advice = readAdviceRequestedWhileReading(dir, 1.0);

        assertThat(advice).isNotEmpty();
        assertThat(advice.stream().filter(Request::isRandom))
                .as("advice requested: %s", advice)
                .isEmpty();
    }

    @Test
    void adviseRawVectorsRandomInAPlainSegment(@TempDir Path dir) throws IOException {
        List<Request> advice = readAdviceRequestedWhileReading(dir, 0.0);

        assertThat(advice.stream().filter(Request::isRandom))
                .as("advice requested: %s", advice)
                .isNotEmpty()
                .allMatch(request -> request.name.endsWith(RAW_VECTORS));
    }

    /// Nothing but the raw vectors, in either layout. Stated separately from the two tests above
    /// because it is the half that regresses silently: advising too much still works, just slower.
    @Test
    void neverAdviseTheGraphOrTheCodes(@TempDir Path dir) throws IOException {
        assertThat(readAdviceRequestedWhileReading(dir.resolve("plain"), 0.0).stream()
                        .filter(Request::isRandom)
                        .map(request -> request.name))
                .noneMatch(name -> name.endsWith(QUANTIZED_VECTORS) || name.endsWith(HNSW_GRAPH));
    }

    @Test
    void openRawVectorsUnmarkedWhenTheAdviceIsRejected() throws IOException {
        String name = "_0_Foo_0" + RAW_VECTORS;
        List<IOContext> reaching = new ArrayList<>();

        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
                output.writeLong(0);
            }
            FilterDirectory rejecting = new FilterDirectory(directory) {
                @Override
                public IndexInput openInput(String file, IOContext context) throws IOException {
                    reaching.add(context);
                    if (RawVectorsReadAdvice.READ_ADVICE.apply(file, context).isPresent()) {
                        throw new IOException("Call to posix_madvise failed with return code 11.");
                    }
                    return in.openInput(file, context);
                }
            };

            RawVectorsReadAdvice.markRawVectors(rejecting)
                    .openInput(name, IOContext.DEFAULT.withHints(DataAccessHint.RANDOM))
                    .close();
        }

        assertThat(reaching).hasSize(2);
        assertThat(RawVectorsReadAdvice.READ_ADVICE.apply(name, reaching.getFirst()))
                .contains(ReadAdvice.RANDOM);
        assertThat(RawVectorsReadAdvice.READ_ADVICE.apply(name, reaching.getLast()))
                .isEmpty();
    }

    /// The rejection is latched rather than retried: the next marked open would be rejected the
    /// same way, and each rejected attempt still asks the kernel for the doomed VMA split.
    @Test
    void stopMarkingAfterARejectedAdvice() throws IOException {
        String name = "_0_Foo_0" + RAW_VECTORS;
        ArrayList<IOContext> reaching = new ArrayList<>();

        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
                output.writeLong(0);
            }
            FilterDirectory rejecting = new FilterDirectory(directory) {
                @Override
                public IndexInput openInput(String file, IOContext context) throws IOException {
                    reaching.add(context);
                    if (RawVectorsReadAdvice.READ_ADVICE.apply(file, context).isPresent()) {
                        throw new IOException("Call to posix_madvise failed with return code 11.");
                    }
                    return in.openInput(file, context);
                }
            };

            Directory marking = RawVectorsReadAdvice.markRawVectors(rejecting);
            marking.openInput(name, IOContext.DEFAULT).close();
            reaching.clear();
            marking.openInput(name, IOContext.DEFAULT).close();
        }

        assertThat(RawVectorsReadAdvice.adviseRejected()).contains("return code 11");
        assertThat(reaching)
                .singleElement()
                .satisfies(context -> assertThat(RawVectorsReadAdvice.READ_ADVICE.apply(name, context))
                        .isEmpty());
    }

    /// A real read failure is not the advice failing: the fallback rethrows whatever the unmarked
    /// open produces, so a genuinely broken `.vec` still fails loudly instead of being swallowed.
    @Test
    void stillFailWhenTheFileCannotBeReadAtAll() throws IOException {
        String name = "_0_Foo_0" + RAW_VECTORS;
        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            FilterDirectory failing = new FilterDirectory(directory) {
                @Override
                public IndexInput openInput(String file, IOContext context) throws IOException {
                    throw new IOException("Read past EOF");
                }
            };

            Directory marking = RawVectorsReadAdvice.markRawVectors(failing);
            assertThatIOException()
                    .isThrownBy(() -> marking.openInput(name, IOContext.DEFAULT))
                    .withMessage("Read past EOF");
        }
    }

    /// Builds a binary quantized index on a real [MMapDirectory], reads every vector back, and
    /// returns every read advice the directory was asked to resolve. `noCFSRatio` of 1.0 leaves the
    /// segment compound, 0.0 unpacks it -- Neo4j always flushes compound, so the plain layout only
    /// exists after a merge.
    private static List<Request> readAdviceRequestedWhileReading(Path dir, double noCFSRatio) throws IOException {
        CopyOnWriteArrayList<Request> requests = new CopyOnWriteArrayList<>();

        try (MMapDirectory mmap = new MMapDirectory(dir)) {
            // Without madvise support the provider never resolves an advice, so there is nothing
            // to record and the test would pass vacuously.
            assumeThat(MMapDirectory.supportsMadvise()).isTrue();
            mmap.setReadAdvice((name, context) -> {
                Optional<ReadAdvice> resolved = RawVectorsReadAdvice.READ_ADVICE.apply(name, context);
                requests.add(new Request(name, resolved));
                return resolved;
            });

            // No NRTCachingDirectory: it answers small files from RAM, and a file that is never
            // mapped is never advised.
            Lucene10Directory directory = new Lucene10Directory(mmap);
            write(directory, noCFSRatio);
            requests.clear(); // writing opens files too; only the read path is under test

            try (LuceneDirectoryReader reader = directory.open()) {
                readEveryVector(reader);
            }
        }
        return List.copyOf(requests);
    }

    private static void readEveryVector(LuceneDirectoryReader reader) throws IOException {
        for (LeafReaderContext leaf : LuceneDirectoryReaderAccess.getLeaves(reader)) {
            FloatVectorValues values = leaf.reader().getFloatVectorValues(fieldName());
            if (values == null) {
                continue;
            }
            KnnVectorValues.DocIndexIterator iterator = values.iterator();
            for (int doc = iterator.nextDoc(); doc != Integer.MAX_VALUE; doc = iterator.nextDoc()) {
                values.vectorValue(iterator.index());
            }
        }
    }

    private static void write(LuceneDirectory directory, double noCFSRatio) throws IOException {
        VectorIndexConfig config = indexConfig();
        LuceneCodec codec = CONTEXT.codecsFactory().codecFor(config);
        LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig(new KeywordAnalyzer()).setCodec(codec);
        writerConfig.setMergingParameters(noCFSRatio, 1024, LOG_BYTE_SIZED, 10, 10, 1000, 8.0, 1000);

        VectorDocumentStructure documentStructure = documentStructure();
        LuceneDocumentsFactory documentFactory = CONTEXT.documentsFactory();
        Neo4jVectorSimilarityFunction similarityFunction = (Neo4jVectorSimilarityFunction) config.similarityFunction();

        try (LuceneIndexWriter writer = directory.newWriter(writerConfig)) {
            for (int id = 0; id < DOCUMENTS; id++) {
                Value[] values = new Value[] {Values.floatArray(embedding(id))};
                LuceneDocument document =
                        documentFactory.createVectorDocument(documentStructure, id, similarityFunction, values);
                writer.addDocument(document);
                if (id == DOCUMENTS / 2) {
                    writer.commit();
                }
            }
            // A flushed segment is compound whatever noCFSRatio says -- IndexWriterConfig decides
            // that, and Lucene10Directory hardcodes it to true. Only the merge policy consults
            // noCFSRatio, so the plain layout needs an actual merge.
            writer.forceMerge(1);
        }
    }

    private static VectorIndexConfig indexConfig() {
        SettingsAccessor indexSettings = new IndexSettingObjectMapAccessor(Map.ofEntries(
                entry(IndexSetting.vector_Dimensions(), DIMENSIONS),
                entry(IndexSetting.vector_Similarity_Function(), "EUCLIDEAN"),
                entry(IndexSetting.vector_Quantization_Type(), VectorQuantizationType.BINARY.name())));
        return VECTOR_INDEX_VERSION.indexSettingValidator(KERNEL_VERSION).validateToTypedConfig(indexSettings);
    }

    private static VectorDocumentStructure documentStructure() {
        return VectorDocumentStructureAccess.documentStructureFor(VECTOR_INDEX_VERSION);
    }

    private static String fieldName() {
        return documentStructure().vectorValueKeyFor(DIMENSIONS);
    }

    private static float[] embedding(int id) {
        float[] embedding = new float[DIMENSIONS];
        Arrays.fill(embedding, 0.5f);
        embedding[0] = 0.5f + id * 0.1f;
        return embedding;
    }

    private record Request(String name, Optional<ReadAdvice> advice) {
        boolean isRandom() {
            return advice.filter(ReadAdvice.RANDOM::equals).isPresent();
        }

        @Override
        public String toString() {
            return name + "->" + advice.map(Enum::name).orElse("default");
        }
    }

    /// Records the advice the marking wrapper's context resolves to, per file name.
    private static final class RecordingDirectory extends FilterDirectory {
        private final List<Request> opened = new ArrayList<>();

        RecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opened.add(new Request(name, RawVectorsReadAdvice.READ_ADVICE.apply(name, context)));
            return in.openInput(name, context);
        }

        Set<ReadAdvice> adviceFor(String name) {
            return opened.stream()
                    .filter(request -> request.name.equals(name))
                    .flatMap(request -> request.advice.stream())
                    .collect(java.util.stream.Collectors.toSet());
        }
    }
}
