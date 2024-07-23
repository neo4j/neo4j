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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.MathUtil.ceil;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.SetIterable;
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
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorQuantization;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Tokens;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorIndexCreationTest {
    private static final VectorIndexVersion LATEST =
            VectorIndexVersion.latestSupportedVersion(LatestVersions.LATEST_KERNEL_VERSION);

    abstract static class Entity {
        private final Factory factory;
        private final VectorIndexVersion minimumVersionForEntity;

        Entity(Factory factory, VectorIndexVersion minimumVersion) {
            this.factory = factory;
            this.minimumVersionForEntity = minimumVersion;
        }

        @Nested
        class IndexProvider extends TestBase {
            private final SetIterable<VectorIndexVersion> invalidVersions;

            IndexProvider() {
                super(Entity.this.factory, inclusiveVersionRangeFrom(minimumVersionForEntity));
                this.invalidVersions = VectorIndexVersion.KNOWN_VERSIONS.toSet().difference(validVersions());
            }

            @ParameterizedTest
            @MethodSource("invalidVersions")
            @EnabledIf("hasInvalidVersions")
            void shouldRejectVectorIndexOnUnsupportedVersions(VectorIndexVersion version) {
                assertUnsupportedIndex(() -> createVectorIndex(version, defaultSettings(), propKeyIds[0]));
            }

            SetIterable<VectorIndexVersion> invalidVersions() {
                return invalidVersions;
            }

            boolean hasInvalidVersions() {
                return invalidVersions.notEmpty();
            }

            private static void assertUnsupportedIndex(ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContainingAll("vector indexes with provider", "are not supported");
            }
        }

        @Nested
        class Schema extends TestBase {
            Schema() {
                super(Entity.this.factory, inclusiveVersionRangeFrom(minimumVersionForEntity));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            void shouldRejectCompositeKeys(VectorIndexVersion version) {
                assertUnsupportedComposite(() -> createVectorIndex(version, defaultSettings(), propKeyIds));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectCompositeKeysCoreAPI() {
                assertUnsupportedComposite(() -> createVectorIndex(defaultSettings(), PROP_KEYS));
            }

            private static void assertUnsupportedComposite(ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContainingAll(
                                "Composite indexes are not supported for", IndexType.VECTOR.name(), "index type");
            }
        }

        @Nested
        class Dimensions extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Dimensions();

            Dimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, int dimensions) {
                final var settings = defaultSettings().withDimensions(dimensions);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(dimensions));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(dimensions));
            }

            Iterable<Arguments> shouldAcceptSupported() {
                return validVersions().asLazy().flatCollect(version -> supported(1, version.maxDimensions())
                        .asLazy()
                        .collect(dimension -> Arguments.of(version, dimension)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int dimensions) {
                final var settings = defaultSettings().withDimensions(dimensions);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(dimensions));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(dimensions));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxDimensions());
            }

            static RichIterable<Integer> supported(int min, int max) {
                return Lists.immutable.of(min, ceil(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int dimensions) {
                final var settings = defaultSettings().withDimensions(dimensions);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Iterable<Arguments> shouldRejectUnsupported() {
                return validVersions().asLazy().flatCollect(version -> unsupportedDimensions(version)
                        .asLazy()
                        .collect(dimension -> Arguments.of(version, dimension)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int dimensions) {
                final var settings = defaultSettings().withDimensions(dimensions);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedDimensions(LATEST);
            }

            static RichIterable<Integer> unsupportedDimensions(VectorIndexVersion version) {
                return Lists.immutable.of(-1, 0, version.maxDimensions() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                SETTING.getSettingName(),
                                "must be between 1 and",
                                String.valueOf(version.maxDimensions()),
                                "inclusively");
            }
        }

        @Nested
        class RequiredDimensions extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Dimensions();

            RequiredDimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRange(
                                max(minimumVersionForEntity, VectorIndexVersion.V1_0), VectorIndexVersion.V1_0));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRequireSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }
        }

        @Nested
        class OptionalDimensions extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Dimensions();

            OptionalDimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2_0)));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertMissingSetting(SETTING, index.getIndexConfig());
                // config via schema store
                assertMissingSetting(SETTING, findIndex(index.getName()).getIndexConfig());
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertMissingSetting(SETTING, index.getIndexConfig());
                // config via schema store
                assertMissingSetting(SETTING, findIndex(index.getName()).getIndexConfig());
            }
        }

        @Nested
        class SimilarityFunctions extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Similarity_Function();

            SimilarityFunctions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, VectorSimilarityFunction similarityFunction) {
                final var settings = defaultSettings().withSimilarityFunction(similarityFunction);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.stringValue(similarityFunction.name()));
                // config via schema store
                assertSettingHasValue(
                        SETTING,
                        findIndex(index.getName()).getIndexConfig(),
                        Values.stringValue(similarityFunction.name()));
            }

            Iterable<Arguments> shouldAcceptSupported() {
                return validVersions().asLazy().flatCollect(version -> supported(version)
                        .asLazy()
                        .collect(similarityFunction -> Arguments.of(version, similarityFunction)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(VectorSimilarityFunction similarityFunction) {
                final var settings = defaultSettings().withSimilarityFunction(similarityFunction);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.stringValue(similarityFunction.name()));
                // config via schema store
                assertSettingHasValue(
                        SETTING,
                        findIndex(index.getName()).getIndexConfig(),
                        Values.stringValue(similarityFunction.name()));
            }

            static Iterable<VectorSimilarityFunction> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            static RichIterable<VectorSimilarityFunction> supported(VectorIndexVersion version) {
                return version.supportedSimilarityFunctions();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRejectUnsupported(VectorIndexVersion version) {
                final var similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
                final var settings = defaultSettings().withSimilarityFunction(similarityFunctionName);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI() {
                final var similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
                final var settings = defaultSettings().withSimilarityFunction(similarityFunctionName);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                "is an unsupported",
                                SETTING.getSettingName(),
                                "Supported",
                                version.supportedSimilarityFunctions()
                                        .asLazy()
                                        .collect(VectorSimilarityFunction::name)
                                        .toString());
            }
        }

        @Nested
        class RequiredSimilarityFunction extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Similarity_Function();

            RequiredSimilarityFunction() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRange(
                                max(minimumVersionForEntity, VectorIndexVersion.V1_0), VectorIndexVersion.V1_0));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRequireSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }
        }

        @Nested
        class DefaultedSimilarityFunction extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Similarity_Function();
            private static final Value DEFAULT_VALUE = Values.stringValue("COSINE");

            DefaultedSimilarityFunction() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2_0)));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }
        }

        @Nested
        class Quantization extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Quantization();
            private static final Value DEFAULT_VALUE = Values.stringValue(VectorQuantization.LUCENE.name());

            Quantization() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, VectorQuantization quantization) {
                final var settings = defaultSettings().withQuantization(quantization);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.stringValue(quantization.name()));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.stringValue(quantization.name()));
            }

            Iterable<Arguments> shouldAcceptSupported() {
                return validVersions().asLazy().flatCollect(version -> supported(version)
                        .asLazy()
                        .collect(similarityFunction -> Arguments.of(version, similarityFunction)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(VectorQuantization quantization) {
                final var settings = defaultSettings().withQuantization(quantization);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.stringValue(quantization.name()));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.stringValue(quantization.name()));
            }

            Iterable<VectorQuantization> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            RichIterable<VectorQuantization> supported(VectorIndexVersion version) {
                return version.supportedQuantizations();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRejectUnsupported(VectorIndexVersion version) {
                final var quantizationName = "ClearlyThisIsNotAQuantization";
                final var settings = defaultSettings().withQuantization(quantizationName);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI() {
                final var quantizationName = "ClearlyThisIsNotAQuantization";
                final var settings = defaultSettings().withQuantization(quantizationName);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                "is an unsupported",
                                IndexSetting.vector_Quantization().getSettingName(),
                                "Supported",
                                version.supportedQuantizations()
                                        .asLazy()
                                        .collect(VectorQuantization::name)
                                        .toString());
            }
        }

        @Nested
        class HnswM extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Hnsw_M();
            private static final Value DEFAULT_VALUE = Values.intValue(16);

            HnswM() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, int M) {
                final var settings = defaultSettings().withHnswM(M);
                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(M));
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(M));
            }

            Iterable<Arguments> shouldAcceptSupported() {
                return validVersions().asLazy().flatCollect(version -> supported(1, version.maxHnswM())
                        .asLazy()
                        .collect(M -> Arguments.of(version, M)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int M) {
                final var settings = defaultSettings().withHnswM(M);
                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(M));
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(M));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxHnswM());
            }

            static RichIterable<Integer> supported(int min, int max) {
                return Lists.immutable.of(min, ceil(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int M) {
                final var settings = defaultSettings().withHnswM(M);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Iterable<Arguments> shouldRejectUnsupported() {
                return validVersions()
                        .asLazy()
                        .flatCollect(version -> unsupportedM(version).asLazy().collect(M -> Arguments.of(version, M)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int M) {
                final var settings = defaultSettings().withHnswM(M);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedM(LATEST);
            }

            static RichIterable<Integer> unsupportedM(VectorIndexVersion version) {
                return Lists.immutable.of(-1, 0, version.maxHnswM() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                IndexSetting.vector_Hnsw_M().getSettingName(),
                                "must be between 1 and",
                                String.valueOf(version.maxHnswM()),
                                "inclusively");
            }
        }

        @Nested
        class HnswEfConstruction extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Hnsw_Ef_Construction();
            private static final Value DEFAULT_VALUE = Values.intValue(100);

            HnswEfConstruction() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, int efConstruction) {
                final var settings = defaultSettings().withHnswEfConstruction(efConstruction);
                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(efConstruction));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(efConstruction));
            }

            Iterable<Arguments> shouldAcceptSupported() {
                return validVersions().asLazy().flatCollect(version -> supported(1, version.maxHnswEfConstruction())
                        .asLazy()
                        .collect(efConstruction -> Arguments.of(version, efConstruction)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int efConstruction) {
                final var settings = defaultSettings().withHnswEfConstruction(efConstruction);
                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(efConstruction));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(efConstruction));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxHnswEfConstruction());
            }

            static RichIterable<Integer> supported(int min, int max) {
                return Lists.immutable.of(min, ceil(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                final var settings = defaultSettings().unset(SETTING);

                final var ref = new MutableObject<IndexDescriptor>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                final var index = ref.getValue();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int efConstruction) {
                final var settings = defaultSettings().withHnswEfConstruction(efConstruction);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Iterable<Arguments> shouldRejectUnsupported() {
                return validVersions().asLazy().flatCollect(version -> unsupportedEfConstruction(version)
                        .asLazy()
                        .collect(M -> Arguments.of(version, M)));
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int efConstruction) {
                final var settings = defaultSettings().withHnswEfConstruction(efConstruction);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedEfConstruction(LATEST);
            }

            static RichIterable<Integer> unsupportedEfConstruction(VectorIndexVersion version) {
                return Lists.immutable.of(-1, 0, version.maxHnswEfConstruction() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                IndexSetting.vector_Hnsw_Ef_Construction().getSettingName(),
                                "must be between 1 and",
                                String.valueOf(version.maxHnswEfConstruction()),
                                "inclusively");
            }
        }

        private static void assertDoesNotThrow(ThrowingCallable callable) {
            assertThatCode(callable).doesNotThrowAnyException();
        }

        private static void assertSettingHasValue(IndexSetting setting, IndexConfig indexConfig, Value value) {
            assertThat(indexConfig.<Value>get(setting.getSettingName())).isEqualTo(value);
        }

        private static void assertMissingSetting(IndexSetting setting, IndexConfig indexConfig) {
            assertThat(indexConfig.asMap()).doesNotContainKey(setting.getSettingName());
        }

        private static void assertMissingExpectedSetting(IndexSetting setting, ThrowingCallable callable) {
            assertThatThrownBy(callable)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll(setting.getSettingName(), "is expected to have been set");
        }
    }

    @Nested
    class Node extends Entity {
        Node() {
            super(NodeIndexFactory.INSTANCE, VectorIndexVersion.V1_0);
        }
    }

    @Nested
    class Relationship extends Entity {
        Relationship() {
            super(RelIndexFactory.INSTANCE, VectorIndexVersion.V2_0);
        }
    }

    @ImpermanentDbmsExtension
    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class TestBase {
        protected static final List<String> PROP_KEYS =
                new Tokens.Suppliers.PropertyKey("vector", Tokens.Suppliers.Suffixes.incrementing()).get(2);

        protected final Factory factory;
        private final SetIterable<VectorIndexVersion> validVersions;

        @Inject
        private GraphDatabaseAPI db;

        protected int tokenId;
        protected int[] propKeyIds;

        TestBase(Factory factory, SetIterable<VectorIndexVersion> validVersions) {
            this.factory = factory;
            this.validVersions = validVersions;
        }

        @BeforeAll
        void setup() throws Exception {
            try (final var tx = db.beginTx()) {
                final var ktx = ((InternalTransaction) tx).kernelTransaction();
                tokenId = factory.tokenId(ktx);
                propKeyIds = Tokens.Factories.PROPERTY_KEY.getIds(ktx, PROP_KEYS);
                tx.commit();
            }
        }

        @BeforeEach
        void dropAllIndexes() {
            try (final var tx = db.beginTx()) {
                tx.schema().getIndexes().forEach(IndexDefinition::drop);
                tx.commit();
            }
        }

        protected SetIterable<VectorIndexVersion> validVersions() {
            return validVersions;
        }

        protected boolean hasValidVersions() {
            return validVersions.notEmpty();
        }

        protected boolean latestIsValid() {
            return validVersions.contains(LATEST);
        }

        protected static VectorIndexVersion max(VectorIndexVersion... versions) {
            return Sets.mutable.of(versions).max();
        }

        protected static SetIterable<VectorIndexVersion> inclusiveVersionRangeFrom(VectorIndexVersion from) {
            return inclusiveVersionRange(from, LATEST);
        }

        protected static SetIterable<VectorIndexVersion> inclusiveVersionRange(
                VectorIndexVersion from, VectorIndexVersion to) {
            return VectorIndexVersion.KNOWN_VERSIONS
                    .asLazy()
                    .select(version -> from.compareTo(version) <= 0 && version.compareTo(to) <= 0)
                    .toSet();
        }

        protected IndexDescriptor createVectorIndex(
                VectorIndexVersion version, VectorIndexSettings settings, int... propKeyIds) throws KernelException {
            final IndexDescriptor indexDescriptor;
            try (final var tx = db.beginTx()) {
                final var ktx = ((InternalTransaction) tx).kernelTransaction();
                final var prototype = IndexPrototype.forSchema(factory.schemaDescriptor(tokenId, propKeyIds))
                        .withIndexType(IndexType.VECTOR)
                        .withIndexProvider(version.descriptor())
                        .withIndexConfig(settings.toIndexConfig());
                indexDescriptor = ktx.schemaWrite().indexCreate(prototype);
                tx.commit();
            }
            return indexDescriptor;
        }

        protected IndexDescriptor createVectorIndex(VectorIndexSettings settings, String propKey) {
            return createVectorIndex(settings, List.of(propKey));
        }

        protected IndexDescriptor createVectorIndex(VectorIndexSettings settings, List<String> propKeys) {
            final IndexDescriptor indexDescriptor;
            try (final var tx = db.beginTx()) {
                final var index = createVectorIndex(factory.indexCreator(tx), settings, propKeys);
                indexDescriptor = ((IndexDefinitionImpl) index).getIndexReference();
                tx.commit();
            }
            return indexDescriptor;
        }

        protected IndexDefinition createVectorIndex(
                IndexCreator creator, VectorIndexSettings settings, List<String> propKeys) {
            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi()).withIndexConfiguration(settings.toMap());
            for (final var propKey : propKeys) {
                creator = creator.on(propKey);
            }
            return creator.create();
        }

        protected IndexDescriptor findIndex(String name) {
            try (final var tx = db.beginTx()) {
                final var index = tx.schema().getIndexByName(name);
                return ((IndexDefinitionImpl) index).getIndexReference();
            }
        }
    }

    abstract static class Factory {
        abstract int tokenId(KernelTransaction ktx) throws KernelException;

        abstract SchemaDescriptor schemaDescriptor(int tokenId, int... propKeyIds);

        abstract IndexCreator indexCreator(Transaction tx);
    }

    private static class NodeIndexFactory extends Factory {
        private static final NodeIndexFactory INSTANCE = new NodeIndexFactory();
        private static final Label LABEL = Tokens.Factories.LABEL.fromName("Vector");

        private NodeIndexFactory() {}

        @Override
        int tokenId(KernelTransaction ktx) throws KernelException {
            return Tokens.Factories.LABEL.getId(ktx, LABEL);
        }

        @Override
        protected SchemaDescriptor schemaDescriptor(int labelId, int... propKeyIds) {
            return SchemaDescriptors.forLabel(labelId, propKeyIds);
        }

        @Override
        protected IndexCreator indexCreator(Transaction tx) {
            return tx.schema().indexFor(LABEL);
        }
    }

    private static class RelIndexFactory extends Factory {
        private static final RelIndexFactory INSTANCE = new RelIndexFactory();
        private static final RelationshipType REL_TYPE = Tokens.Factories.RELATIONSHIP_TYPE.fromName("VECTOR");

        private RelIndexFactory() {}

        @Override
        int tokenId(KernelTransaction ktx) throws KernelException {
            return Tokens.Factories.RELATIONSHIP_TYPE.getId(ktx, REL_TYPE);
        }

        @Override
        protected SchemaDescriptor schemaDescriptor(int relTypeId, int... propKeyIds) {
            return SchemaDescriptors.forRelType(relTypeId, propKeyIds);
        }

        @Override
        protected IndexCreator indexCreator(Transaction tx) {
            return tx.schema().indexFor(REL_TYPE);
        }
    }

    private static final Map<IndexSetting, Object> DEFAULT_SETTINGS_FOR_TESTING =
            IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi());

    static VectorIndexSettings defaultSettings() {
        return VectorIndexSettings.from(DEFAULT_SETTINGS_FOR_TESTING);
    }
}
