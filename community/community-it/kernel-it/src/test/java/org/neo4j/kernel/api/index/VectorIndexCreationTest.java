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

import static java.lang.Math.ceilDiv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.api.schema.vector.VectorTestUtils.inclusiveVersionRange;
import static org.neo4j.kernel.api.schema.vector.VectorTestUtils.inclusiveVersionRangeFrom;
import static org.neo4j.kernel.api.schema.vector.VectorTestUtils.max;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
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
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
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
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Tokens;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorIndexCreationTest {
    private static final KernelVersion KERNEL_VERSION = LatestVersions.LATEST_KERNEL_VERSION;
    private static final VectorIndexVersion LATEST = VectorIndexVersion.latestSupportedVersion(KERNEL_VERSION);

    abstract static class Entity {
        private final Factory factory;
        private final VectorIndexVersion minimumVersionForEntity;

        Entity(Factory factory, VectorIndexVersion minimumVersion) {
            this.factory = factory;
            this.minimumVersionForEntity = minimumVersion;
        }

        @Nested
        class IndexProvider extends TestBase {
            private final Set<VectorIndexVersion> invalidVersions;

            IndexProvider() {
                super(Entity.this.factory, inclusiveVersionRangeFrom(minimumVersionForEntity));
                Set<VectorIndexVersion> validVersions = validVersions();
                Set<VectorIndexVersion> invalidVersions = EnumSet.noneOf(VectorIndexVersion.class);
                for (VectorIndexVersion indexVersion : VectorIndexVersion.KNOWN_VERSIONS) {
                    if (!validVersions.contains(indexVersion)) {
                        invalidVersions.add(indexVersion);
                    }
                }
                this.invalidVersions = Collections.unmodifiableSet(invalidVersions);
            }

            @ParameterizedTest
            @MethodSource("invalidVersions")
            @EnabledIf("hasInvalidVersions")
            void shouldRejectVectorIndexOnUnsupportedVersions(VectorIndexVersion version) {
                assertUnsupportedIndex(() -> createVectorIndex(version, defaultSettings(), propKeyIds[0]));
            }

            Stream<VectorIndexVersion> invalidVersions() {
                return invalidVersions.stream();
            }

            boolean hasInvalidVersions() {
                return !invalidVersions.isEmpty();
            }

            private static void assertUnsupportedIndex(ThrowingCallable operation) {
                ErrorGqlStatusObjectAssertions.assertThatThrownBy(operation)
                        .isInstanceOf(InvalidArgumentException.class)
                        .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31)
                        .hasMessageContainingAll(
                                "Creating a relationship vector index with provider",
                                "is not supported in Neo4j",
                                "Please use a newer index provider");
            }
        }

        @Nested
        class Schema extends TestBase {
            Schema() {
                super(Entity.this.factory, inclusiveVersionRangeFrom(minimumVersionForEntity));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            void shouldOnlyAcceptCompositeKeysInSupportedVersions(VectorIndexVersion version) {
                switch (version) {
                    case UNKNOWN, V1_0, V2_0 ->
                        assertUnsupportedComposite(() -> createVectorIndex(version, defaultSettings(), propKeyIds));
                    case V3_0 -> assertDoesNotThrow(() -> createVectorIndex(version, defaultSettings(), propKeyIds));
                }
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectCompositeKeysCoreAPI() {
                assertDoesNotThrow(() -> createVectorIndex(defaultSettings(), PROP_KEYS));
            }

            private static void assertUnsupportedComposite(ThrowingCallable operation) {
                ErrorGqlStatusObjectAssertions.assertThatThrownBy(operation)
                        .isInstanceOf(InvalidArgumentException.class)
                        .hasMessageContainingAll(
                                "Creating a filtering vector index with provider ",
                                "Please use a newer index provider.")
                        .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31);
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
                VectorIndexSettings settings = defaultSettings().withDimensions(dimensions);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(dimensions));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(dimensions));
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int dimension : supported(1, version.maxDimensions())) {
                        builder.add(Arguments.of(version, dimension));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int dimensions) {
                VectorIndexSettings settings = defaultSettings().withDimensions(dimensions);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(dimensions));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(dimensions));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxDimensions());
            }

            static Iterable<Integer> supported(int min, int max) {
                return List.of(min, ceilDiv(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int dimensions) {
                VectorIndexSettings settings = defaultSettings().withDimensions(dimensions);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Stream<Arguments> shouldRejectUnsupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int dimension : unsupportedDimensions(version)) {
                        builder.add(Arguments.of(version, dimension));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int dimensions) {
                VectorIndexSettings settings = defaultSettings().withDimensions(dimensions);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            static Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedDimensions(LATEST);
            }

            static Iterable<Integer> unsupportedDimensions(VectorIndexVersion version) {
                return List.of(-1, 0, version.maxDimensions() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
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
                VectorIndexSettings settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }
        }

        @Nested
        class OptionalDimensions extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Dimensions();

            OptionalDimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V3_0)));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertMissingSetting(SETTING, index.getIndexConfig());
                // config via schema store
                assertMissingSetting(SETTING, findIndex(index.getName()).getIndexConfig());
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

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
                VectorIndexSettings settings = defaultSettings().withSimilarityFunction(similarityFunction);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(
                        SETTING, index.getIndexConfig(), Values.stringValue(similarityFunction.functionName()));
                // config via schema store
                assertSettingHasValue(
                        SETTING,
                        findIndex(index.getName()).getIndexConfig(),
                        Values.stringValue(similarityFunction.functionName()));
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (VectorSimilarityFunction similarityFunction : supported(version)) {
                        builder.add(Arguments.of(version, similarityFunction));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(VectorSimilarityFunction similarityFunction) {
                VectorIndexSettings settings = defaultSettings().withSimilarityFunction(similarityFunction);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(
                        SETTING, index.getIndexConfig(), Values.stringValue(similarityFunction.functionName()));
                // config via schema store
                assertSettingHasValue(
                        SETTING,
                        findIndex(index.getName()).getIndexConfig(),
                        Values.stringValue(similarityFunction.functionName()));
            }

            static Iterable<VectorSimilarityFunction> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            static Iterable<VectorSimilarityFunction> supported(VectorIndexVersion version) {
                return version.supportedSimilarityFunctions();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRejectUnsupported(VectorIndexVersion version) {
                String similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
                VectorIndexSettings settings = defaultSettings().withSimilarityFunction(similarityFunctionName);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI() {
                String similarityFunctionName = "ClearlyThisIsNotASimilarityFunction";
                VectorIndexSettings settings = defaultSettings().withSimilarityFunction(similarityFunctionName);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                StringJoiner supported = new StringJoiner(", ", "[", "]");
                for (VectorSimilarityFunction similarityFunction : version.supportedSimilarityFunctions()) {
                    supported.add(similarityFunction.functionName().toUpperCase(Locale.ROOT));
                }

                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
                        .hasMessageContainingAll(
                                "is an unsupported", SETTING.getSettingName(), "Supported", supported.toString());
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
                VectorIndexSettings settings = defaultSettings().unset(SETTING);
                assertMissingExpectedSetting(SETTING, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);
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
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }
        }

        @Nested
        class DefaultSearchExpansionFactor extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Default_Search_Expansion_Factor();

            private static Value defaultValue(VectorIndexVersion version, VectorQuantizationType quantizationType) {
                double defaultSearchExpansionFactor =
                        switch (quantizationType) {
                            case NONE -> 1.0;
                            case SCALAR -> 1.5;
                            case BINARY -> version.compareTo(VectorIndexVersion.V2026_07) >= 0 ? 3.0 : 2.0;
                        };
                return Values.doubleValue(defaultSearchExpansionFactor);
            }

            DefaultSearchExpansionFactor() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2026_06)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, double defaultSearchExpansionFactor) {
                VectorIndexSettings settings =
                        defaultSettings().withDefaultSearchExpansionFactor(defaultSearchExpansionFactor);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                Value value = Values.doubleValue(defaultSearchExpansionFactor);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), value);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), value);
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (double defaultSearchExpansionFactor : supported(1.0, 10_000.0)) {
                        builder.add(Arguments.of(version, defaultSearchExpansionFactor));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(double defaultSearchExpansionFactor) {
                VectorIndexSettings settings =
                        defaultSettings().withDefaultSearchExpansionFactor(defaultSearchExpansionFactor);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                Value value = Values.doubleValue(defaultSearchExpansionFactor);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), value);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), value);
            }

            Iterable<Double> shouldAcceptSupportedCoreAPI() {
                return supported(1.0, 10_000.0);
            }

            Iterable<Double> supported(double min, double max) {
                return List.of(min, (min + max) / 2.0, max);
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version, VectorQuantizationType quantizationType) {
                VectorIndexSettings settings =
                        defaultSettings().withQuantizationType(quantizationType).unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                Value defautValue = defaultValue(version, quantizationType);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defautValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defautValue);
            }

            Stream<Arguments> shouldAcceptMissingSetting() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (VectorQuantizationType quantizationType : version.supportedQuantizationTypes()) {
                        builder.add(Arguments.of(version, quantizationType));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI(VectorQuantizationType quantizationType) {
                VectorIndexSettings settings =
                        defaultSettings().withQuantizationType(quantizationType).unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                Value defautValue = defaultValue(LATEST, quantizationType);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defautValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defautValue);
            }

            static Stream<VectorQuantizationType> shouldAcceptMissingSettingCoreAPI() {
                Stream.Builder<VectorQuantizationType> builder = Stream.builder();
                for (VectorQuantizationType quantizationType : LATEST.supportedQuantizationTypes()) {
                    builder.add(quantizationType);
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, double defaultSearchExpansionFactor) {
                VectorIndexSettings settings =
                        defaultSettings().withDefaultSearchExpansionFactor(defaultSearchExpansionFactor);
                assertUnsupported(() -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Stream<Arguments> shouldRejectUnsupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (double defaultSearchExpansionFactor : unsupported()) {
                        builder.add(Arguments.of(version, defaultSearchExpansionFactor));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource("unsupported")
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(double defaultSearchExpansionFactor) {
                VectorIndexSettings settings =
                        defaultSettings().withDefaultSearchExpansionFactor(defaultSearchExpansionFactor);
                assertUnsupported(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            static Iterable<Double> unsupported() {
                return List.of(-1.0, 0.0, Math.nextDown(1.0), Math.nextUp(10_000.0));
            }

            private static void assertUnsupported(ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
                        .hasMessageContainingAll(
                                SETTING.getSettingName(), "must be between 1.0 and 10000.0 inclusively");
            }
        }

        @Nested
        class QuantizationEnabled extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Quantization_Enabled();
            private static final Value DEFAULT_VALUE = BooleanValue.TRUE;

            QuantizationEnabled() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRange(
                                max(minimumVersionForEntity, VectorIndexVersion.V2_0), VectorIndexVersion.V3_0));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, boolean quantizationEnabled) {
                VectorIndexSettings settings = defaultSettings().withQuantizationEnabled(quantizationEnabled);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.booleanValue(quantizationEnabled));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.booleanValue(quantizationEnabled));
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (boolean quantizationEnabled : supported(version)) {
                        builder.add(Arguments.of(version, quantizationEnabled));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(boolean quantizationEnabled) {
                VectorIndexSettings settings = defaultSettings().withQuantizationEnabled(quantizationEnabled);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.booleanValue(quantizationEnabled));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.booleanValue(quantizationEnabled));
            }

            Iterable<Boolean> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            static Iterable<Boolean> supported(VectorIndexVersion version) {
                return version.supportedQuantizationBooleans();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }
        }

        @Nested
        class QuantizationTypes extends TestBase {
            private static final IndexSetting SETTING = IndexSetting.vector_Quantization_Type();

            // Currently the parameter is unused, it has been added in preparation
            // for when we change the default quantization type
            @SuppressWarnings("unused")
            private static Value defaultValue(VectorIndexVersion version) {
                VectorQuantizationType quantizationType = VectorQuantizationType.SCALAR;
                return Values.utf8Value(quantizationType.name());
            }

            private static Value defaultValue(VectorIndexVersion version, boolean quantizationEnabled) {
                if (quantizationEnabled) {
                    return defaultValue(version);
                } else {
                    return Values.utf8Value(VectorQuantizationType.NONE.name());
                }
            }

            QuantizationTypes() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V2026_06)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, VectorQuantizationType quantizationType) {
                VectorIndexSettings settings = defaultSettings().withQuantizationType(quantizationType);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                Value value = Values.utf8Value(quantizationType.name().toUpperCase(Locale.ROOT));

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), value);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), value);
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (VectorQuantizationType quantizationType : supported(version)) {
                        builder.add(Arguments.of(version, quantizationType));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(VectorQuantizationType quantizationType) {
                VectorIndexSettings settings = defaultSettings().withQuantizationType(quantizationType);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                Value value = Values.utf8Value(quantizationType.name().toUpperCase(Locale.ROOT));

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), value);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), value);
            }

            Iterable<VectorQuantizationType> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            Iterable<VectorQuantizationType> supported(VectorIndexVersion version) {
                return version.supportedQuantizationTypes();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldRejectUnsupported(VectorIndexVersion version) {
                String quantizationTypeName = "ClearlyThisIsNotAQuantizationType";
                VectorIndexSettings settings = defaultSettings()
                        .withDefaultSearchExpansionFactor(2.0)
                        .withQuantizationType(quantizationTypeName);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI() {
                String quantizationTypeName = "ClearlyThisIsNotAQuantizationType";
                VectorIndexSettings settings = defaultSettings()
                        .withDefaultSearchExpansionFactor(2.0)
                        .withQuantizationType(quantizationTypeName);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                StringJoiner supported = new StringJoiner(", ", "[", "]");
                for (VectorQuantizationType quantizationType : version.supportedQuantizationTypes()) {
                    supported.add(quantizationType.name());
                }

                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
                        .hasMessageContainingAll(
                                "is an unsupported", SETTING.getSettingName(), "Supported", supported.toString());
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                Value defaultValue = defaultValue(version);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defaultValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defaultValue);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                Value defaultValue = defaultValue(LATEST);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defaultValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defaultValue);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSettingWithConflictingDependentSetting(VectorIndexVersion version) {
                VectorIndexSettings settings =
                        defaultSettings().unset(SETTING).set(IndexSetting.vector_Quantization_Enabled(), false);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                Value defautValue = defaultValue(version, false);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defautValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defautValue);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingWithConflictingDependentSettingCoreAPI() {
                VectorIndexSettings settings =
                        defaultSettings().unset(SETTING).set(IndexSetting.vector_Quantization_Enabled(), false);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                Value defautValue = defaultValue(LATEST, false);

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), defautValue);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), defautValue);
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
                VectorIndexSettings settings = defaultSettings().withHnswM(M);
                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(M));
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(M));
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int M : supported(1, version.maxHnswM())) {
                        builder.add(Arguments.of(version, M));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int M) {
                VectorIndexSettings settings = defaultSettings().withHnswM(M);
                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(M));
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(M));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxHnswM());
            }

            static Iterable<Integer> supported(int min, int max) {
                return List.of(min, ceilDiv(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int M) {
                VectorIndexSettings settings = defaultSettings().withHnswM(M);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Stream<Arguments> shouldRejectUnsupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int M : unsupportedM(version)) {
                        builder.add(Arguments.of(version, M));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int M) {
                VectorIndexSettings settings = defaultSettings().withHnswM(M);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            static Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedM(LATEST);
            }

            static Iterable<Integer> unsupportedM(VectorIndexVersion version) {
                return List.of(-1, 0, version.maxHnswM() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
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
                VectorIndexSettings settings = defaultSettings().withHnswEfConstruction(efConstruction);
                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(efConstruction));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(efConstruction));
            }

            Stream<Arguments> shouldAcceptSupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int efConstruction : supported(1, version.maxHnswEfConstruction())) {
                        builder.add(Arguments.of(version, efConstruction));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldAcceptSupportedCoreAPI(int efConstruction) {
                VectorIndexSettings settings = defaultSettings().withHnswEfConstruction(efConstruction);
                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), Values.intValue(efConstruction));
                // config via schema store
                assertSettingHasValue(
                        SETTING, findIndex(index.getName()).getIndexConfig(), Values.intValue(efConstruction));
            }

            static Iterable<Integer> shouldAcceptSupportedCoreAPI() {
                return supported(1, LATEST.maxHnswEfConstruction());
            }

            static Iterable<Integer> supported(int min, int max) {
                return List.of(min, ceilDiv(max - min, 2), max);
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            @EnabledIf("hasValidVersions")
            void shouldAcceptMissingSetting(VectorIndexVersion version) {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(version, settings, propKeyIds[0])));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldAcceptMissingSettingCoreAPI() {
                VectorIndexSettings settings = defaultSettings().unset(SETTING);

                MutableObject<IndexDescriptor> ref = new MutableObject<>();
                assertDoesNotThrow(() -> ref.setValue(createVectorIndex(settings, PROP_KEYS.get(1))));
                IndexDescriptor index = ref.get();

                // config committed in tx
                assertSettingHasValue(SETTING, index.getIndexConfig(), DEFAULT_VALUE);
                // config via schema store
                assertSettingHasValue(SETTING, findIndex(index.getName()).getIndexConfig(), DEFAULT_VALUE);
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectUnsupported(VectorIndexVersion version, int efConstruction) {
                VectorIndexSettings settings = defaultSettings().withHnswEfConstruction(efConstruction);
                assertUnsupported(version, () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            Stream<Arguments> shouldRejectUnsupported() {
                Stream.Builder<Arguments> builder = Stream.builder();
                for (VectorIndexVersion version : validVersions()) {
                    for (int efConstruction : unsupportedEfConstruction(version)) {
                        builder.add(Arguments.of(version, efConstruction));
                    }
                }
                return builder.build();
            }

            @ParameterizedTest
            @MethodSource
            @EnabledIf("latestIsValid")
            void shouldRejectUnsupportedCoreAPI(int efConstruction) {
                VectorIndexSettings settings = defaultSettings().withHnswEfConstruction(efConstruction);
                assertUnsupported(LATEST, () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            static Iterable<Integer> shouldRejectUnsupportedCoreAPI() {
                return unsupportedEfConstruction(LATEST);
            }

            static Iterable<Integer> unsupportedEfConstruction(VectorIndexVersion version) {
                return List.of(-1, 0, version.maxHnswEfConstruction() + 1);
            }

            private static void assertUnsupported(VectorIndexVersion version, ThrowingCallable callable) {
                assertThatThrownBy(callable)
                        .isInstanceOf(InvalidArgumentException.class)
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
                    .isInstanceOf(InvalidArgumentException.class)
                    .hasMessageContainingAll(
                            "setting is expected to have been set", "Expected", setting.getSettingName());
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

    @ImpermanentDbmsExtension(configurationCallback = "configure")
    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class TestBase {
        protected static final List<String> PROP_KEYS =
                new Tokens.Suppliers.PropertyKey("vector", Tokens.Suppliers.Suffixes.incrementing()).get(2);

        protected final Factory factory;
        private final Set<VectorIndexVersion> validVersions;

        @Inject
        private GraphDatabaseAPI db;

        protected int tokenId;
        protected int[] propKeyIds;

        TestBase(Factory factory, Set<VectorIndexVersion> validVersions) {
            this.factory = factory;
            this.validVersions = Collections.unmodifiableSet(validVersions);
        }

        @ExtensionCallback
        static void configure(TestDatabaseManagementServiceBuilder builder) {
            builder.setConfig(GraphDatabaseInternalSettings.always_use_latest_index_provider, false)
                    .setConfig(GraphDatabaseInternalSettings.latest_kernel_version, KERNEL_VERSION.version())
                    .setConfig(
                            GraphDatabaseInternalSettings.latest_runtime_version,
                            DbmsRuntimeVersion.fromKernelVersion(KERNEL_VERSION).getVersion());
        }

        @BeforeAll
        void setup() throws Exception {
            try (Transaction tx = db.beginTx()) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                tokenId = factory.tokenId(ktx);
                propKeyIds = Tokens.Factories.PROPERTY_KEY.getIds(ktx, PROP_KEYS);
                tx.commit();
            }
        }

        @BeforeEach
        void dropAllIndexes() {
            try (Transaction tx = db.beginTx()) {
                tx.schema().getIndexes().forEach(IndexDefinition::drop);
                tx.commit();
            }
        }

        protected Set<VectorIndexVersion> validVersions() {
            return validVersions;
        }

        protected boolean hasValidVersions() {
            return !validVersions.isEmpty();
        }

        protected boolean latestIsValid() {
            return validVersions.contains(LATEST);
        }

        protected IndexDescriptor createVectorIndex(
                VectorIndexVersion version, VectorIndexSettings settings, int... propKeyIds) throws KernelException {
            IndexDescriptor indexDescriptor;
            try (Transaction tx = db.beginTx()) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                IndexPrototype prototype = IndexPrototype.forSchema(factory.schemaDescriptor(tokenId, propKeyIds))
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
            IndexDescriptor indexDescriptor;
            try (Transaction tx = db.beginTx()) {
                IndexDefinition index = createVectorIndex(factory.indexCreator(tx), settings, propKeys);
                indexDescriptor = ((IndexDefinitionImpl) index).getIndexReference();
                tx.commit();
            }
            return indexDescriptor;
        }

        protected static IndexDefinition createVectorIndex(
                IndexCreator creator, VectorIndexSettings settings, SequencedCollection<String> propKeys) {
            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi()).withIndexConfiguration(settings.toMap());
            for (String propKey : propKeys) {
                creator = creator.on(propKey);
            }
            return creator.create();
        }

        protected IndexDescriptor findIndex(String name) {
            try (Transaction tx = db.beginTx()) {
                IndexDefinition index = tx.schema().getIndexByName(name);
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
