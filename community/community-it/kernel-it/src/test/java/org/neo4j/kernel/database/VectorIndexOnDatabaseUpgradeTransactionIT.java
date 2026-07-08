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
package org.neo4j.kernel.database;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaStatementCreator;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Tokens.Factories;
import org.neo4j.test.Tokens.Suppliers.Label;
import org.neo4j.test.Tokens.Suppliers.PropertyKey;
import org.neo4j.test.Tokens.Suppliers.RelationshipType;
import org.neo4j.test.Tokens.Suppliers.UUID;
import org.neo4j.test.UpgradeTestUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

class VectorIndexOnDatabaseUpgradeTransactionIT {

    @Nested
    class Kernel extends Base {

        @Override
        protected void createIndex(
                Transaction tx,
                EntityType entityType,
                TokenIds tokenIds,
                VectorIndexVersion indexVersion,
                VectorIndexSettings settings)
                throws KernelException {
            KernelTransaction ktx = kernelTransaction(tx);
            ktx.schemaWrite().indexCreate(createIndexPrototype(entityType, tokenIds, indexVersion, settings));
        }

        @ParameterizedTest
        @EnumSource
        void shouldBeBlockedFromCreatingVectorIndexWithMultiToken(EntityType entityType) {
            KernelVersion introducedKernelVersion = KernelVersion.VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING;
            VectorIndexVersion indexVersion = VectorIndexVersion.latestSupportedVersion(introducedKernelVersion);
            KernelVersion previousVersion = previousFrom(introducedKernelVersion);
            setup(previousVersion);
            assertThatThrownBy(() -> {
                        try (Transaction tx = database.beginTx()) {
                            createIndex(tx, entityType, 2, 2, indexVersion, defaultSettings());
                            tx.commit();
                        }
                    })
                    .isInstanceOf(InvalidArgumentException.class)
                    .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31)
                    .hasMessageContainingAll(
                            "Creating a",
                            "vector index is not supported in",
                            previousVersion.name(),
                            "Required version for operation is",
                            introducedKernelVersion.name(),
                            "Please upgrade DBMS");
        }

        @ParameterizedTest
        @EnumSource
        void shouldBePossibleToCreateVectorIndexWithMultiTokenAfterUpgrade(EntityType entityType)
                throws KernelException {
            KernelVersion introducedKernelVersion = KernelVersion.VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING;
            VectorIndexVersion indexVersion = VectorIndexVersion.latestSupportedVersion(introducedKernelVersion);
            KernelVersion previousVersion = previousFrom(introducedKernelVersion);
            setup(previousVersion);
            UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, KERNEL_VERSION);

            try (Transaction tx = database.beginTx()) {
                createIndex(tx, entityType, 2, 2, indexVersion, defaultSettings());
                tx.commit();
            }

            try (Transaction tx = database.beginTx()) {
                assertThat(tx.schema().getIndexes()).hasSize(1);
            }
        }
    }

    @Nested
    class Cypher extends Base {

        @Override
        protected void createIndex(
                Transaction tx,
                EntityType entityType,
                TokenIds tokenIds,
                VectorIndexVersion indexVersion,
                VectorIndexSettings settings)
                throws KernelException {
            IndexPrototype prototype = createIndexPrototype(entityType, tokenIds, indexVersion, settings);
            VectorIndexVersion indexVersionToValidateWith =
                    VectorIndexVersion.fromDescriptor(prototype.getIndexProvider());

            VectorIndexConfig vectorIndexConfig = indexVersionToValidateWith
                    .indexSettingValidator()
                    .validateToTypedConfig(new IndexConfigAccessor(prototype.getIndexConfig()));

            IndexDescriptor index =
                    prototype.withIndexConfig(vectorIndexConfig.config()).materialise(0);
            String createStatement = SchemaStatementCreator.getIndexSchemaStatement(
                    kernelTransaction(tx).tokenRead(), index, indexVersion != VectorIndexVersion.UNKNOWN);
            tx.execute(createStatement);
        }
    }

    @TestDirectoryExtension
    abstract static class Base {
        // keep these for future testing purposes
        protected static final KernelVersion KERNEL_VERSION = LatestVersions.LATEST_KERNEL_VERSION;
        private static final DbmsRuntimeVersion RUNTIME_VERSION = DbmsRuntimeVersion.fromKernelVersion(KERNEL_VERSION);
        private static final Label LABELS = UUID.LABEL;
        private static final RelationshipType REL_TYPES = UUID.RELATIONSHIP_TYPE;
        private static final PropertyKey PROP_KEYS = UUID.PROPERTY_KEY;
        private static final Factories.Label LABEL_IDS = Factories.LABEL;
        private static final Factories.RelationshipType REL_TYPE_IDS = Factories.RELATIONSHIP_TYPE;
        private static final Factories.PropertyKey PROP_KEY_IDS = Factories.PROPERTY_KEY;

        @Inject
        private TestDirectory testDirectory;

        protected DatabaseManagementService dbms;
        protected GraphDatabaseAPI database;
        private KernelVersionProvider kernelVersionProvider;

        @AfterEach
        void tearDown() {
            if (dbms != null) {
                dbms.shutdown();
            }
        }

        @ParameterizedTest
        @MethodSource("indexVersions")
        void shouldBeBlockedFromCreatingVectorIndexOnOlderVersion(
                EntityType entityType, VectorIndexVersion indexVersion) {
            KernelVersion previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
            setup(previousVersion);

            KernelVersion minimumRequiredVersion =
                    switch (entityType) {
                        case NODE -> indexVersion.minimumRequiredKernelVersion();
                        case RELATIONSHIP ->
                            KernelVersion.getForVersion((byte) Math.max(
                                    KernelVersion.VERSION_VECTOR_2_INTRODUCED.version(),
                                    indexVersion.minimumRequiredKernelVersion().version()));
                    };

            assertThatThrownBy(() -> {
                        try (Transaction tx = database.beginTx()) {
                            createIndex(tx, entityType, indexVersion, defaultSettings());
                            tx.commit();
                        }
                    })
                    .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31)
                    .hasMessageContainingAll(
                            "Creating a",
                            entityType.name().toLowerCase(),
                            "vector index with provider",
                            indexVersion.descriptor().name(),
                            "is not supported in",
                            previousVersion.name(),
                            "Required version for operation is",
                            minimumRequiredVersion.name(),
                            "Please upgrade DBMS");
        }

        @ParameterizedTest
        @MethodSource("indexVersions")
        void shouldBePossibleToCreateVectorIndexAfterUpgrade(EntityType entityType, VectorIndexVersion indexVersion)
                throws KernelException {
            KernelVersion previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
            setup(previousVersion);
            UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, KERNEL_VERSION);

            try (Transaction tx = database.beginTx()) {
                createIndex(tx, entityType, indexVersion, defaultSettings());
                tx.commit();
            }

            try (Transaction tx = database.beginTx()) {
                assertThat(getVectorIndexes(tx)).hasSize(1);
            }
        }

        static Stream<Arguments> indexVersions() {
            Stream.Builder<Arguments> builder = Stream.builder();
            builder.add(Arguments.of(EntityType.NODE, VectorIndexVersion.V1_0));
            for (VectorIndexVersion version : VectorIndexVersion.KNOWN_VERSIONS.tailSet(VectorIndexVersion.V2_0)) {
                builder.add(Arguments.of(EntityType.NODE, version));
                builder.add(Arguments.of(EntityType.RELATIONSHIP, version));
            }
            return builder.build();
        }

        @ParameterizedTest
        @MethodSource("nonInitialIndexVersions")
        void createVectorIndexShouldTriggerUpgradeButBePreviousIndexVersion(
                EntityType entityType, VectorIndexVersion indexVersion) throws KernelException {
            KernelVersion previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
            VectorIndexVersion expectedIndexVersion = VectorIndexVersion.latestSupportedVersion(previousVersion);

            setup(previousVersion);
            // write expected tokens before upgrade, as to not create a write
            TokenIds tokenIds = TokenIds.from(database, entityType, 1, 1);
            upgradeDbms(dbms);
            assertKernelVersion(database, previousVersion);

            try (Transaction tx = database.beginTx()) {
                createIndex(tx, entityType, tokenIds, VectorIndexVersion.UNKNOWN, defaultSettings());
                tx.commit();
            }

            assertKernelVersion(database, KERNEL_VERSION);
            try (Transaction tx = database.beginTx()) {
                assertThat(getVectorIndexes(tx))
                        .singleElement(type(IndexDefinitionImpl.class))
                        .extracting(IndexDefinitionImpl::getIndexReference)
                        .extracting(IndexDescriptor::getIndexProvider)
                        .isEqualTo(expectedIndexVersion.descriptor());
            }
        }

        static Stream<Arguments> nonInitialIndexVersions() {
            Stream.Builder<Arguments> builder = Stream.builder();
            builder.add(Arguments.of(EntityType.NODE, VectorIndexVersion.V2_0));
            for (VectorIndexVersion version : VectorIndexVersion.KNOWN_VERSIONS.tailSet(VectorIndexVersion.V3_0)) {
                builder.add(Arguments.of(EntityType.NODE, version));
                builder.add(Arguments.of(EntityType.RELATIONSHIP, version));
            }
            return builder.build();
        }

        @ParameterizedTest
        @MethodSource("introducedSettings")
        void shouldBeBlockedFromCreatingVectorIndexWithNewSettingsOnOlderVersionWithSameIndexVersion(
                VectorIndexVersion indexVersion, EntityType entityType, IndexSetting setting, Object validValue) {
            KernelVersion introducedKernelVersion =
                    VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
            KernelVersion previousVersion = previousFrom(introducedKernelVersion);
            assumeThat(VectorIndexVersion.latestSupportedVersion(previousVersion))
                    .as(
                            "'%s' was introduced with a new %s",
                            setting.getSettingName(), VectorIndexVersion.class.getSimpleName())
                    .isSameAs(indexVersion);

            setup(previousVersion);
            assertThatThrownBy(() -> {
                        try (Transaction tx = database.beginTx()) {
                            createIndex(
                                    tx,
                                    entityType,
                                    indexVersion,
                                    defaultSettings().set(setting, validValue));
                            tx.commit();
                        }
                    })
                    .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31)
                    .hasMessageContainingAll(
                            "Creating a vector index with provided settings is not supported in",
                            previousVersion.name(),
                            "Required version for operation is",
                            introducedKernelVersion.name(),
                            "Please upgrade DBMS");
        }

        @ParameterizedTest
        @MethodSource("introducedSettings")
        void shouldBePossibleToCreateVectorIndexWithNewSettingsAfterUpgrade(
                VectorIndexVersion indexVersion, EntityType entityType, IndexSetting setting, Object validValue)
                throws KernelException {
            KernelVersion introducedKernelVersion =
                    VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
            KernelVersion previousVersion = previousFrom(introducedKernelVersion);
            setup(previousVersion);
            UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, KERNEL_VERSION);

            try (Transaction tx = database.beginTx()) {
                createIndex(tx, entityType, indexVersion, defaultSettings().set(setting, validValue));
                tx.commit();
            }

            try (Transaction tx = database.beginTx()) {
                assertThat(tx.schema().getIndexes()).hasSize(1);
            }
        }

        static Stream<Arguments> introducedSettings() {
            Map<IndexSetting, Object> introducedSettings = Map.ofEntries(
                    entry(IndexSetting.vector_Dimensions(), 1536),
                    entry(IndexSetting.vector_Similarity_Function(), "COSINE"),
                    entry(IndexSetting.vector_Default_Search_Expansion_Factor(), 4.0),
                    entry(IndexSetting.vector_Quantization_Enabled(), true),
                    entry(IndexSetting.vector_Quantization_Type(), "BINARY"),
                    entry(IndexSetting.vector_Hnsw_M(), 32),
                    entry(IndexSetting.vector_Hnsw_Ef_Construction(), 256));

            Builder<Arguments> arguments = Stream.builder();
            for (Entry<IndexSetting, Object> introducedSetting : introducedSettings.entrySet()) {
                IndexSetting setting = introducedSetting.getKey();
                Object validValue = introducedSetting.getValue();

                KernelVersion introducedKernelVersion =
                        VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
                VectorIndexVersion version = VectorIndexVersion.latestSupportedVersion(introducedKernelVersion);
                if (introducedKernelVersion.isAtLeast(KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED)) {
                    arguments.accept(Arguments.of(version, EntityType.NODE, setting, validValue));
                }
                if (introducedKernelVersion.isAtLeast(KernelVersion.VERSION_VECTOR_2_INTRODUCED)) {
                    arguments.accept(Arguments.of(version, EntityType.RELATIONSHIP, setting, validValue));
                }
            }
            return arguments.build();
        }

        private void createIndex(
                Transaction tx, EntityType entityType, VectorIndexVersion indexVersion, VectorIndexSettings settings)
                throws KernelException {
            createIndex(tx, entityType, 1, 1, indexVersion, settings);
        }

        protected void createIndex(
                Transaction tx,
                EntityType entityType,
                int numberOfEntityTokens,
                int numberOfPropertyKeys,
                VectorIndexVersion indexVersion,
                VectorIndexSettings settings)
                throws KernelException {
            createIndex(
                    tx,
                    entityType,
                    TokenIds.from(tx, entityType, numberOfEntityTokens, numberOfPropertyKeys),
                    indexVersion,
                    settings);
        }

        protected abstract void createIndex(
                Transaction tx,
                EntityType entityType,
                TokenIds tokenIds,
                VectorIndexVersion indexVersion,
                VectorIndexSettings settings)
                throws KernelException;

        protected IndexPrototype createIndexPrototype(
                EntityType entityType,
                TokenIds tokenIds,
                VectorIndexVersion indexVersion,
                VectorIndexSettings settings) {
            SchemaDescriptor schemaDescriptor =
                    switch (indexVersion) {
                        case V1_0, V2_0 ->
                            switch (entityType) {
                                case NODE -> SchemaDescriptors.forLabel(tokenIds.entities[0], tokenIds.keys);
                                case RELATIONSHIP -> SchemaDescriptors.forRelType(tokenIds.entities[0], tokenIds.keys);
                            };
                        default -> SchemaDescriptors.forSemanticSearch(entityType, tokenIds.entities, tokenIds.keys);
                    };

            VectorIndexVersion indexProviderToUseIndexVersion = indexVersion == VectorIndexVersion.UNKNOWN
                    ? VectorIndexVersion.latestSupportedVersion(kernelVersion())
                    : indexVersion;

            return IndexPrototype.forSchema(schemaDescriptor)
                    .withName("VectorIndex")
                    .withIndexType(IndexType.VECTOR)
                    .withIndexProvider(indexProviderToUseIndexVersion.descriptor())
                    .withIndexConfig(settings.toIndexConfig());
        }

        protected static KernelTransaction kernelTransaction(Transaction tx) {
            return ((InternalTransaction) tx).kernelTransaction();
        }

        protected KernelVersion kernelVersion() {
            return kernelVersionProvider.kernelVersion();
        }

        record TokenIds(int[] entities, int[] keys) {
            static TokenIds from(
                    Transaction tx, EntityType entityType, int numberOfEntityTokens, int numberOfPropertyKeys)
                    throws KernelException {
                KernelTransaction ktx = kernelTransaction(tx);
                int[] propKeyIds = PROP_KEY_IDS.getIds(ktx, PROP_KEYS.get(numberOfPropertyKeys));
                int[] entityTokenIds =
                        switch (entityType) {
                            case NODE -> LABEL_IDS.getIds(ktx, LABELS.get(numberOfEntityTokens));
                            case RELATIONSHIP -> REL_TYPE_IDS.getIds(ktx, REL_TYPES.get(numberOfEntityTokens));
                        };
                return new TokenIds(entityTokenIds, propKeyIds);
            }

            static TokenIds from(
                    GraphDatabaseService db, EntityType entityType, int numberOfEntityTokens, int numberOfPropertyKeys)
                    throws KernelException {
                TokenIds tokenIds;
                try (Transaction tx = db.beginTx()) {
                    tokenIds = from(tx, entityType, numberOfEntityTokens, numberOfPropertyKeys);
                    tx.commit();
                }
                return tokenIds;
            }
        }

        protected static VectorIndexSettings defaultSettings() {
            return VectorIndexSettings.from(IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi()));
        }

        protected static KernelVersion previousFrom(KernelVersion kernelVersion) {
            if (kernelVersion == KernelVersion.GLORIOUS_FUTURE) {
                kernelVersion = LatestVersions.LATEST_KERNEL_VERSION;
            }
            return KernelVersion.precedingVersion(kernelVersion);
        }

        private static List<IndexDefinition> getVectorIndexes(Transaction tx) {
            return Iterables.stream(tx.schema().getIndexes())
                    .filter(id -> id.getIndexType() == IndexType.VECTOR.toPublicApi())
                    .toList();
        }

        protected void setup(KernelVersion kernelVersion) {
            ZippedStoreCommunity store =
                    switch (kernelVersion) {
                        case V5_10 -> ZippedStoreCommunity.REC_AF11_V510_EMPTY;
                        case V5_15 -> ZippedStoreCommunity.REC_AF11_V515_EMPTY;
                        case V5_22 -> ZippedStoreCommunity.REC_AF11_V522_EMPTY;
                        case V2025_08 -> ZippedStoreCommunity.REC_AF11_V202508_EMPTY;
                        case V2025_11 -> ZippedStoreCommunity.REC_AF11_V202511_EMPTY;
                        case V2026_02 -> ZippedStoreCommunity.REC_AF11_V202602_EMPTY;
                        case V2026_06 -> ZippedStoreCommunity.REC_AF11_V202606_EMPTY;
                        default ->
                            throw InvalidArgumentException.internalError(
                                    this.getClass().getSimpleName(),
                                    "Test not setup to find a %s for %s."
                                            .formatted(ZippedStore.class.getSimpleName(), kernelVersion));
                    };
            setup(store);
        }

        private void setup(ZippedStoreCommunity snapshot) {
            try {
                snapshot.unzip(testDirectory.homePath());
            } catch (IOException exc) {
                fail("Could not setup %s:%s".formatted(snapshot.name(), exc));
            }
            dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                    .setConfig(GraphDatabaseInternalSettings.automatic_upgrade_enabled, false)
                    .setConfig(GraphDatabaseInternalSettings.latest_runtime_version, RUNTIME_VERSION.getVersion())
                    .setConfig(GraphDatabaseInternalSettings.latest_kernel_version, KERNEL_VERSION.version())
                    .setConfig(GraphDatabaseInternalSettings.always_use_latest_index_provider, false)
                    .build();
            database = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
            assertKernelVersion(database, snapshot.statistics().kernelVersion());
            kernelVersionProvider = database.getDependencyResolver().resolveDependency(KernelVersionProvider.class);
        }
    }
}
