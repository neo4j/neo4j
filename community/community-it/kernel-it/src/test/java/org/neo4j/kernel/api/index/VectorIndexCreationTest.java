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
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.internal.helpers.MathUtil.ceil;

import java.util.List;
import java.util.Map;
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
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Tokens;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

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
            IndexProvider() {
                super(Entity.this.factory, inclusiveVersionRangeFrom(minimumVersionForEntity));
            }

            @ParameterizedTest
            @MethodSource
            void shouldRejectVectorIndexOnUnsupportedVersions(VectorIndexVersion version) {
                assumeThat(version).as("skip if no unsupported versions").isNotEqualTo(VectorIndexVersion.UNKNOWN);
                assertUnsupportedIndex(() -> createVectorIndex(version, defaultSettings(), propKeyIds[0]));
            }

            Iterable<VectorIndexVersion> shouldRejectVectorIndexOnUnsupportedVersions() {
                return Sets.mutable.with(VectorIndexVersion.values()).difference(validVersions());
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
            Dimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, int dimensions) {
                final var settings = defaultSettings().withDimensions(dimensions);
                assertDoesNotThrow(() -> createVectorIndex(version, settings, propKeyIds[0]));
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
                assertDoesNotThrow(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
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
                                IndexSetting.vector_Dimensions().getSettingName(),
                                "must be between 1 and",
                                String.valueOf(version.maxDimensions()),
                                "inclusively");
            }
        }

        @Nested
        class RequiredDimensions extends TestBase {
            RequiredDimensions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            void shouldRequireSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(IndexSetting.vector_Dimensions());
                assertMissingExpectedSetting(
                        IndexSetting.vector_Dimensions(), () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                final var settings = defaultSettings().unset(IndexSetting.vector_Dimensions());
                assertMissingExpectedSetting(
                        IndexSetting.vector_Dimensions(), () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }
        }

        @Nested
        class SimilarityFunctions extends TestBase {
            SimilarityFunctions() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource
            void shouldAcceptSupported(VectorIndexVersion version, VectorSimilarityFunction similarityFunction) {
                final var settings = defaultSettings().withSimilarityFunction(similarityFunction);
                assertDoesNotThrow(() -> createVectorIndex(version, settings, propKeyIds[0]));
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
                assertDoesNotThrow(() -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }

            static Iterable<VectorSimilarityFunction> shouldAcceptSupportedCoreAPI() {
                return supported(LATEST);
            }

            static RichIterable<VectorSimilarityFunction> supported(VectorIndexVersion version) {
                return version.supportedSimilarityFunctions();
            }

            @ParameterizedTest
            @MethodSource("validVersions")
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
                                IndexSetting.vector_Similarity_Function().getSettingName(),
                                "Supported",
                                version.supportedSimilarityFunctions()
                                        .asLazy()
                                        .collect(VectorSimilarityFunction::name)
                                        .toString());
            }
        }

        @Nested
        class RequiredSimilarityFunction extends TestBase {
            RequiredSimilarityFunction() {
                super(
                        Entity.this.factory,
                        inclusiveVersionRangeFrom(max(minimumVersionForEntity, VectorIndexVersion.V1_0)));
            }

            @ParameterizedTest
            @MethodSource("validVersions")
            void shouldRequireSetting(VectorIndexVersion version) {
                final var settings = defaultSettings().unset(IndexSetting.vector_Similarity_Function());
                assertMissingExpectedSetting(
                        IndexSetting.vector_Similarity_Function(),
                        () -> createVectorIndex(version, settings, propKeyIds[0]));
            }

            @Test
            @EnabledIf("latestIsValid")
            void shouldRequireSettingCoreAPI() {
                final var settings = defaultSettings().unset(IndexSetting.vector_Similarity_Function());
                assertMissingExpectedSetting(
                        IndexSetting.vector_Similarity_Function(), () -> createVectorIndex(settings, PROP_KEYS.get(1)));
            }
        }

        private static void assertDoesNotThrow(ThrowingCallable callable) {
            assertThatCode(callable).doesNotThrowAnyException();
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

        protected void createVectorIndex(VectorIndexVersion version, VectorIndexSettings settings, int... propKeyIds)
                throws KernelException {
            try (final var tx = db.beginTx()) {
                final var ktx = ((InternalTransaction) tx).kernelTransaction();
                final var prototype = IndexPrototype.forSchema(factory.schemaDescriptor(tokenId, propKeyIds))
                        .withIndexType(IndexType.VECTOR)
                        .withIndexProvider(version.descriptor())
                        .withIndexConfig(settings.toIndexConfig());
                ktx.schemaWrite().indexCreate(prototype);
                tx.commit();
            }
        }

        protected void createVectorIndex(VectorIndexSettings settings, String propKey) {
            createVectorIndex(settings, List.of(propKey));
        }

        protected void createVectorIndex(VectorIndexSettings settings, List<String> propKeys) {
            try (final var tx = db.beginTx()) {
                createVectorIndex(factory.indexCreator(tx), settings, propKeys);
                tx.commit();
            }
        }

        protected void createVectorIndex(IndexCreator creator, VectorIndexSettings settings, List<String> propKeys) {
            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi()).withIndexConfiguration(settings.toMap());
            for (final var propKey : propKeys) {
                creator = creator.on(propKey);
            }
            creator.create();
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
