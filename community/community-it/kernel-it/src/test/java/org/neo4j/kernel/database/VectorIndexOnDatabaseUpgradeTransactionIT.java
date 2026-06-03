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
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
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
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Tokens;
import org.neo4j.test.UpgradeTestUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class VectorIndexOnDatabaseUpgradeTransactionIT {
    // keep these for future testing purposes
    private static final KernelVersion KERNEL_VERSION = LatestVersions.LATEST_KERNEL_VERSION;
    private static final DbmsRuntimeVersion RUNTIME_VERSION = DbmsRuntimeVersion.fromKernelVersion(KERNEL_VERSION);
    private static final Tokens.Suppliers.Label LABELS = Tokens.Suppliers.UUID.LABEL;
    private static final Tokens.Suppliers.RelationshipType REL_TYPES = Tokens.Suppliers.UUID.RELATIONSHIP_TYPE;
    private static final Tokens.Suppliers.PropertyKey PROP_KEYS = Tokens.Suppliers.UUID.PROPERTY_KEY;
    private static final Tokens.Factories.Label LABEL_IDS = Tokens.Factories.LABEL;
    private static final Tokens.Factories.RelationshipType REL_TYPE_IDS = Tokens.Factories.RELATIONSHIP_TYPE;
    private static final Tokens.Factories.PropertyKey PROP_KEY_IDS = Tokens.Factories.PROPERTY_KEY;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI database;

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("indexVersions")
    void shouldBeBlockedFromCreatingVectorIndexOnOlderVersion(EntityType entityType, VectorIndexVersion indexVersion) {
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
                .isInstanceOf(InvalidArgumentException.class)
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
    void shouldBePossibleToCreateVectorIndexAfterUpgrade(EntityType entityType, VectorIndexVersion indexVersion) {
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

    private static Stream<Arguments> indexVersions() {
        return Stream.of(
                Arguments.of(EntityType.NODE, VectorIndexVersion.V1_0),
                Arguments.of(EntityType.NODE, VectorIndexVersion.V2_0),
                Arguments.of(EntityType.NODE, VectorIndexVersion.V3_0),
                Arguments.of(EntityType.RELATIONSHIP, VectorIndexVersion.V2_0),
                Arguments.of(EntityType.RELATIONSHIP, VectorIndexVersion.V3_0));
    }

    @ParameterizedTest
    @MethodSource("nonInitialIndexVersions")
    void createVectorIndexShouldTriggerUpgradeButBePreviousIndexVersion(
            EntityType entityType, VectorIndexVersion indexVersion) {
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
                    .hasSize(1)
                    .first()
                    .asInstanceOf(InstanceOfAssertFactories.type(IndexDefinitionImpl.class))
                    .extracting(IndexDefinitionImpl::getIndexReference)
                    .extracting(IndexDescriptor::getIndexProvider)
                    .isEqualTo(expectedIndexVersion.descriptor());
        }
    }

    private static Stream<Arguments> nonInitialIndexVersions() {
        return Stream.of(
                Arguments.of(EntityType.NODE, VectorIndexVersion.V2_0),
                Arguments.of(EntityType.NODE, VectorIndexVersion.V3_0),
                Arguments.of(EntityType.RELATIONSHIP, VectorIndexVersion.V3_0));
    }

    @ParameterizedTest
    @MethodSource("introducedSettings")
    void shouldBeBlockedFromCreatingVectorIndexWithNewSettingsOnOlderVersionWithSameIndexVersion(
            VectorIndexVersion indexVersion, EntityType entityType, IndexSetting setting, Object validValue) {
        KernelVersion introducedKernelVersion = VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
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
                                tx, entityType, indexVersion, defaultSettings().set(setting, validValue));
                        tx.commit();
                    }
                })
                .isInstanceOf(InvalidArgumentException.class)
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
            VectorIndexVersion indexVersion, EntityType entityType, IndexSetting setting, Object validValue) {
        KernelVersion introducedKernelVersion = VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
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

    private static Stream<Arguments> introducedSettings() {
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
    void shouldBePossibleToCreateVectorIndexWithMultiTokenAfterUpgrade(EntityType entityType) {
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

    private static IndexDescriptor createIndex(
            Transaction tx, EntityType entityType, VectorIndexVersion indexVersion, VectorIndexSettings settings) {
        return createIndex(tx, entityType, 1, 1, indexVersion, settings);
    }

    private static IndexDescriptor createIndex(
            Transaction tx,
            EntityType entityType,
            int numberOfEntityTokens,
            int numberOfPropertyKeys,
            VectorIndexVersion indexVersion,
            VectorIndexSettings settings) {
        try {
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            return createIndex(
                    ktx,
                    entityType,
                    TokenIds.from(ktx, entityType, numberOfEntityTokens, numberOfPropertyKeys),
                    indexVersion,
                    settings);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexDescriptor createIndex(
            Transaction tx,
            EntityType entityType,
            TokenIds tokenIds,
            VectorIndexVersion indexVersion,
            VectorIndexSettings settings) {
        try {
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            return createIndex(ktx, entityType, tokenIds, indexVersion, settings);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexDescriptor createIndex(
            KernelTransaction ktx,
            EntityType entityType,
            TokenIds tokenIds,
            VectorIndexVersion indexVersion,
            VectorIndexSettings settings)
            throws KernelException {
        SchemaDescriptor schemaDescriptor =
                switch (indexVersion) {
                    case V1_0, V2_0 ->
                        switch (entityType) {
                            case NODE -> SchemaDescriptors.forLabel(tokenIds.entities[0], tokenIds.keys);
                            case RELATIONSHIP -> SchemaDescriptors.forRelType(tokenIds.entities[0], tokenIds.keys);
                        };
                    default -> SchemaDescriptors.forSemanticSearch(entityType, tokenIds.entities, tokenIds.keys);
                };

        IndexPrototype prototype = IndexPrototype.forSchema(schemaDescriptor)
                .withIndexType(IndexType.VECTOR)
                .withIndexProvider(indexVersion.descriptor())
                .withIndexConfig(settings.toIndexConfig());

        return ktx.schemaWrite().indexCreate(prototype);
    }

    record TokenIds(int[] entities, int[] keys) {
        static TokenIds from(
                KernelTransaction ktx, EntityType entityType, int numberOfEntityTokens, int numberOfPropertyKeys)
                throws KernelException {
            int[] propKeyIds = PROP_KEY_IDS.getIds(ktx, PROP_KEYS.get(numberOfPropertyKeys));
            int[] entityTokenIds =
                    switch (entityType) {
                        case NODE -> LABEL_IDS.getIds(ktx, LABELS.get(numberOfEntityTokens));
                        case RELATIONSHIP -> REL_TYPE_IDS.getIds(ktx, REL_TYPES.get(numberOfEntityTokens));
                    };
            return new TokenIds(entityTokenIds, propKeyIds);
        }

        static TokenIds from(
                GraphDatabaseService db, EntityType entityType, int numberOfEntityTokens, int numberOfPropertyKeys) {
            TokenIds tokenIds;
            try (Transaction tx = db.beginTx()) {
                KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
                tokenIds = from(ktx, entityType, numberOfEntityTokens, numberOfPropertyKeys);
                tx.commit();
            } catch (KernelException exception) {
                throw new RuntimeException(exception);
            }
            return tokenIds;
        }
    }

    private static VectorIndexSettings defaultSettings() {
        return VectorIndexSettings.from(IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi()));
    }

    private static KernelVersion previousFrom(KernelVersion kernelVersion) {
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

    private void setup(KernelVersion kernelVersion) {
        ZippedStoreCommunity store =
                switch (kernelVersion) {
                    case V5_10 -> ZippedStoreCommunity.REC_AF11_V510_EMPTY;
                    case V5_15 -> ZippedStoreCommunity.REC_AF11_V515_EMPTY;
                    case V5_22 -> ZippedStoreCommunity.REC_AF11_V522_EMPTY;
                    case V2025_08 -> ZippedStoreCommunity.REC_AF11_V202508_EMPTY;
                    case V2025_11 -> ZippedStoreCommunity.REC_AF11_V202511_EMPTY;
                    case V2026_02 -> ZippedStoreCommunity.REC_AF11_V202602_EMPTY;
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
    }
}
