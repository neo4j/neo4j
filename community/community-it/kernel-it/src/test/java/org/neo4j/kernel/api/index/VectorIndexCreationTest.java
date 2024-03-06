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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.eclipse.collections.api.list.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Tokens;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

public class VectorIndexCreationTest {

    @ImpermanentDbmsExtension
    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class VectorIndexCreationTestBase {
        protected static final List<String> PROP_KEYS =
                new Tokens.Suppliers.PropertyKey("vector", Tokens.Suppliers.Suffixes.incrementing()).get(2);
        private final ImmutableList<VectorIndexVersion> validVersions;
        private final ImmutableList<VectorIndexVersion> invalidVersions;

        @Inject
        private GraphDatabaseAPI db;

        private VectorIndexVersion latestSupportedVersion;
        protected int[] propKeyIds;

        VectorIndexCreationTestBase(KernelVersion introducedKernelVersion) {
            final var partitioned = VectorIndexVersion.KNOWN_VERSIONS.partition(
                    indexVersion -> indexVersion.minimumRequiredKernelVersion().isAtLeast(introducedKernelVersion));
            this.validVersions = partitioned.getSelected();
            this.invalidVersions = partitioned.getRejected();
        }

        @BeforeAll
        void setup() throws Exception {
            latestSupportedVersion = VectorIndexVersion.latestSupportedVersion(db.getDependencyResolver()
                    .resolveDependency(KernelVersionProvider.class)
                    .kernelVersion());

            try (final var tx = db.beginTx()) {
                final var ktx = ((InternalTransaction) tx).kernelTransaction();
                propKeyIds = Tokens.Factories.PROPERTY_KEY.getIds(ktx, PROP_KEYS);
                setup(ktx);
                tx.commit();
            }
        }

        abstract void setup(KernelTransaction ktx) throws KernelException;

        @BeforeEach
        void dropAllIndexes() {
            try (final var tx = db.beginTx()) {
                tx.schema().getIndexes().forEach(IndexDefinition::drop);
                tx.commit();
            }
        }

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldAcceptTestDefaults(VectorIndexVersion version) {
            assertDoesNotThrow(() -> createVectorIndex(version, defaultConfig(), propKeyIds[0]));
        }

        @Test
        void shouldAcceptTestDefaultsCoreAPI() {
            assertDoesNotThrow(() -> createVectorIndex(defaultSettings(), PROP_KEYS.get(1)));
        }

        // index provider checks

        @ParameterizedTest
        @MethodSource("invalidVersions")
        @EnabledIf("hasInvalidVersions")
        void shouldRejectVectorIndexOnUnsupportedVersions(VectorIndexVersion version) {
            assertUnsupportedIndex(() -> createVectorIndex(version, defaultConfig(), propKeyIds[0]));
        }

        // schema checks

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldRejectCompositeKeys(VectorIndexVersion version) {
            assertUnsupportedComposite(() -> createVectorIndex(version, defaultConfig(), propKeyIds));
        }

        @Test
        void shouldRejectCompositeKeysCoreAPI() {
            assertUnsupportedComposite(() -> createVectorIndex(defaultSettings(), PROP_KEYS));
        }

        // config checks

        // config: dimensionality

        @ParameterizedTest
        @MethodSource
        @EnabledIf("hasValidVersions")
        void shouldAcceptValidDimensions(VectorIndexVersion version, int dimensions) {
            final var config = defaultConfigWith(IndexSetting.vector_Dimensions(), dimensions);
            assertDoesNotThrow(() -> createVectorIndex(version, config, propKeyIds[0]));
        }

        Stream<Arguments> shouldAcceptValidDimensions() {
            return validVersions().flatMap(version -> validDimensions(version)
                    .mapToObj(dimension -> Arguments.of(version, dimension)));
        }

        @ParameterizedTest
        @MethodSource
        void shouldAcceptValidDimensionsCoreAPI(int dimensions) {
            final var settings = defaultSettingsWith(IndexSetting.vector_Dimensions(), dimensions);
            assertDoesNotThrow(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        IntStream shouldAcceptValidDimensionsCoreAPI() {
            return validDimensions(latestSupportedVersion);
        }

        static IntStream validDimensions(VectorIndexVersion version) {
            final var max = version.maxDimensions();
            // vector dimensions from known embedding provider models
            return IntStream.of(1, 256, 512, 738, 1024, 1408, 1536, 2048, 3072, 4096, max)
                    .filter(d -> d <= max);
        }

        @ParameterizedTest
        @MethodSource
        @EnabledIf("hasValidVersions")
        void shouldRejectIllegalDimensions(VectorIndexVersion version, int dimensions) {
            final var config = defaultConfigWith(IndexSetting.vector_Dimensions(), dimensions);
            assertIllegalDimensions(() -> createVectorIndex(version, config, propKeyIds[0]));
        }

        Stream<Arguments> shouldRejectIllegalDimensions() {
            return validVersions()
                    .flatMap(version -> IntStream.of(-1, 0).mapToObj(dimension -> Arguments.of(version, dimension)));
        }

        @ParameterizedTest
        @ValueSource(ints = {-1, 0})
        void shouldRejectIllegalDimensionsCoreAPI(int dimensions) {
            final var settings = defaultSettingsWith(IndexSetting.vector_Dimensions(), dimensions);
            assertIllegalDimensions(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldRejectUnsupportedDimensions(VectorIndexVersion version) {
            final var dimensions = version.maxDimensions() + 1;
            final var config = defaultConfigWith(IndexSetting.vector_Dimensions(), dimensions);
            assertUnsupportedDimensions(() -> createVectorIndex(version, config, propKeyIds[0]));
        }

        @Test
        void shouldRejectUnsupportedDimensionsCoreAPI() {
            final var dimensions = latestSupportedVersion.maxDimensions() + 1;
            final var settings = defaultSettingsWith(IndexSetting.vector_Dimensions(), dimensions);
            assertUnsupportedDimensions(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldRequireDimensions(VectorIndexVersion version) {
            final var config = defaultConfigWithout(IndexSetting.vector_Dimensions());
            assertMissingExpectedSetting(
                    IndexSetting.vector_Dimensions(), () -> createVectorIndex(version, config, propKeyIds[0]));
        }

        @Test
        void shouldRequireDimensionsCoreAPI() {
            final var settings = defaultSettingsWithout(IndexSetting.vector_Dimensions());
            assertMissingExpectedSetting(
                    IndexSetting.vector_Dimensions(), () -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        // config: similarity functions

        @ParameterizedTest
        @MethodSource
        @EnabledIf("hasValidVersions")
        void shouldAcceptValidSimilarityFunction(
                VectorIndexVersion version, VectorSimilarityFunction similarityFunction) {
            final var similarityFunctionName = similarityFunction.name();
            final var config = defaultConfigWith(IndexSetting.vector_Similarity_Function(), similarityFunctionName);
            assertDoesNotThrow(() -> createVectorIndex(version, config, propKeyIds[0]));
        }

        private Stream<Arguments> shouldAcceptValidSimilarityFunction() {
            return validVersions()
                    .flatMap(version -> version
                            .supportedSimilarityFunctions()
                            .asLazy()
                            .collect(similarityFunction -> Arguments.of(version, similarityFunction))
                            .toList()
                            .stream());
        }

        @ParameterizedTest
        @MethodSource
        @EnabledIf("hasValidVersions")
        void shouldAcceptValidSimilarityFunctionCoreAPI(VectorSimilarityFunction similarityFunction) {
            final var similarityFunctionName = similarityFunction.name();
            final var settings = defaultSettingsWith(IndexSetting.vector_Similarity_Function(), similarityFunctionName);
            assertDoesNotThrow(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        private Iterable<VectorSimilarityFunction> shouldAcceptValidSimilarityFunctionCoreAPI() {
            return latestSupportedVersion.supportedSimilarityFunctions();
        }

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldRejectIllegalSimilarityFunction(VectorIndexVersion version) {
            final var similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
            final var config = defaultConfigWith(IndexSetting.vector_Similarity_Function(), similarityFunctionName);
            assertIllegalSimilarityFunction(version, () -> createVectorIndex(version, config, propKeyIds[0]));
        }

        @Test
        void shouldRejectIllegalSimilarityFunctionCoreAPI() {
            final var similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
            final var settings = defaultSettingsWith(IndexSetting.vector_Similarity_Function(), similarityFunctionName);
            assertIllegalSimilarityFunction(
                    latestSupportedVersion, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        @ParameterizedTest
        @MethodSource("validVersions")
        @EnabledIf("hasValidVersions")
        void shouldRequireSimilarityFunction(VectorIndexVersion version) {
            final var config = defaultConfigWithout(IndexSetting.vector_Similarity_Function());
            assertMissingExpectedSetting(
                    IndexSetting.vector_Similarity_Function(), () -> createVectorIndex(version, config, propKeyIds[0]));
        }

        @Test
        void shouldRequireSimilarityFunctionCoreAPI() {
            final var settings = defaultSettingsWithout(IndexSetting.vector_Similarity_Function());
            assertMissingExpectedSetting(
                    IndexSetting.vector_Similarity_Function(), () -> createVectorIndex(settings, PROP_KEYS.get(1)));
        }

        private boolean hasValidVersions() {
            return !validVersions.isEmpty();
        }

        private Stream<VectorIndexVersion> validVersions() {
            return validVersions.stream();
        }

        private boolean hasInvalidVersions() {
            return !invalidVersions.isEmpty();
        }

        private Stream<VectorIndexVersion> invalidVersions() {
            return invalidVersions.stream();
        }

        private void createVectorIndex(VectorIndexVersion version, IndexConfig config, int... propKeyIds)
                throws KernelException {
            try (final var tx = db.beginTx()) {
                final var ktx = ((InternalTransaction) tx).kernelTransaction();
                final var prototype = IndexPrototype.forSchema(schemaDescriptor(propKeyIds))
                        .withIndexType(IndexType.VECTOR)
                        .withIndexProvider(version.descriptor())
                        .withIndexConfig(config);
                ktx.schemaWrite().indexCreate(prototype);
                tx.commit();
            }
        }

        protected abstract SchemaDescriptor schemaDescriptor(int... propKeyIds);

        private void createVectorIndex(Map<IndexSetting, Object> settings, String propKey) {
            createVectorIndex(settings, List.of(propKey));
        }

        private void createVectorIndex(Map<IndexSetting, Object> settings, List<String> propKeys) {
            try (final var tx = db.beginTx()) {
                createVectorIndex(indexCreator(tx), settings, propKeys);
                tx.commit();
            }
        }

        private void createVectorIndex(
                IndexCreator creator, Map<IndexSetting, Object> settings, List<String> propKeys) {
            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi()).withIndexConfiguration(settings);
            for (final var propKey : propKeys) {
                creator = creator.on(propKey);
            }
            creator.create();
        }

        protected abstract IndexCreator indexCreator(Transaction tx);

        private static IndexConfig defaultConfig() {
            return IndexSettingUtil.defaultConfigForTest(IndexType.VECTOR.toPublicApi());
        }

        private static IndexConfig defaultConfigWith(IndexSetting setting, Object value) {
            return configFrom(defaultSettingsWith(setting, value));
        }

        private static IndexConfig defaultConfigWithout(IndexSetting setting) {
            return configFrom(defaultSettingsWithout(setting));
        }

        private static IndexConfig configFrom(Map<IndexSetting, Object> settings) {
            return IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap(settings);
        }

        private static Map<IndexSetting, Object> defaultSettings() {
            return IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi());
        }

        private static Map<IndexSetting, Object> defaultSettingsWith(IndexSetting setting, Object value) {
            final var settings = new HashMap<>(defaultSettings());
            settings.put(setting, value);
            return Collections.unmodifiableMap(settings);
        }

        private static Map<IndexSetting, Object> defaultSettingsWithout(IndexSetting setting) {
            final var settings = new HashMap<>(defaultSettings());
            settings.remove(setting);
            return Collections.unmodifiableMap(settings);
        }

        private static void assertDoesNotThrow(ThrowingCallable callable) {
            assertThatCode(callable).doesNotThrowAnyException();
        }

        private static void assertUnsupportedIndex(ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContainingAll("vector indexes with provider", "are not supported");
        }

        private static void assertIllegalDimensions(ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(IllegalArgumentException.class)
                    .cause()
                    .hasMessageContainingAll(
                            IndexSetting.vector_Dimensions().getSettingName(), "is expected to be positive");
        }

        private static void assertUnsupportedDimensions(ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContainingAll(IndexSetting.vector_Dimensions().getSettingName(), "set greater than");
        }

        private static void assertIllegalSimilarityFunction(VectorIndexVersion version, ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(IllegalArgumentException.class)
                    .cause()
                    .hasMessageContainingAll(
                            "is an unsupported vector similarity function",
                            "Supported",
                            version.supportedSimilarityFunctions()
                                    .asLazy()
                                    .collect(VectorSimilarityFunction::name)
                                    .toString());
        }

        private static void assertUnsupportedComposite(ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContainingAll(
                            "Composite indexes are not supported for", IndexType.VECTOR.name(), "index type");
        }

        private static void assertMissingExpectedSetting(IndexSetting setting, ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(IllegalArgumentException.class)
                    .rootCause()
                    .hasMessageContainingAll(setting.getSettingName(), "is expected to have been set");
        }
    }

    @Nested
    class NodeIndex extends VectorIndexCreationTestBase {
        private static final Label LABEL = Tokens.Factories.LABEL.fromName("Vector");

        private int labelId;

        NodeIndex() {
            super(KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED);
        }

        @Override
        void setup(KernelTransaction ktx) throws KernelException {
            labelId = Tokens.Factories.LABEL.getId(ktx, LABEL);
        }

        @Override
        protected SchemaDescriptor schemaDescriptor(int... propKeyIds) {
            return SchemaDescriptors.forLabel(labelId, propKeyIds);
        }

        @Override
        protected IndexCreator indexCreator(Transaction tx) {
            return tx.schema().indexFor(LABEL);
        }
    }

    @Nested
    class RelIndex extends VectorIndexCreationTestBase {

        private static final RelationshipType REL_TYPE = Tokens.Factories.RELATIONSHIP_TYPE.fromName("VECTOR");
        private int relTypeId;

        RelIndex() {
            super(KernelVersion.VERSION_VECTOR_2_INTRODUCED);
        }

        @Override
        void setup(KernelTransaction ktx) throws KernelException {
            relTypeId = Tokens.Factories.RELATIONSHIP_TYPE.getId(ktx, REL_TYPE);
        }

        @Override
        protected SchemaDescriptor schemaDescriptor(int... propKeyIds) {
            return SchemaDescriptors.forRelType(relTypeId, propKeyIds);
        }

        @Override
        protected IndexCreator indexCreator(Transaction tx) {
            return tx.schema().indexFor(REL_TYPE);
        }
    }
}
