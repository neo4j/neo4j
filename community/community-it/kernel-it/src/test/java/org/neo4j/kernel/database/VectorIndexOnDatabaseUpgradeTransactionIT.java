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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorQuantization;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
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
    private static final DbmsRuntimeVersion LATEST_RUNTIME_VERSION = LatestVersions.LATEST_RUNTIME_VERSION;
    private static final KernelVersion LATEST_KERNEL_VERSION = LATEST_RUNTIME_VERSION.kernelVersion();
    private static final Label LABEL = Tokens.Suppliers.UUID.LABEL.get();
    private static final RelationshipType REL_TYPE = Tokens.Suppliers.UUID.RELATIONSHIP_TYPE.get();
    private static final String PROP_KEY = Tokens.Suppliers.UUID.PROPERTY_KEY.get();

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
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        assertThatThrownBy(() -> {
                    try (final var tx = database.beginTx()) {
                        createIndex(tx, entityType, indexVersion, defaultSettings());
                        tx.commit();
                    }
                })
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContainingAll(
                        "Failed to create index with provider",
                        indexVersion.descriptor().name(),
                        "Version was",
                        previousVersion.name(),
                        "but required version for operation is",
                        indexVersion.minimumRequiredKernelVersion().name(),
                        "Please upgrade dbms using",
                        "dbms.upgrade()");
    }

    @ParameterizedTest
    @MethodSource("indexVersions")
    void shouldBePossibleToCreateVectorIndexAfterUpgrade(EntityType entityType, VectorIndexVersion indexVersion) {
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, LATEST_KERNEL_VERSION);

        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion, defaultSettings());
            tx.commit();
        }

        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    @ParameterizedTest
    @MethodSource("indexVersions")
    void createVectorIndexShouldTriggerUpgrade(EntityType entityType, VectorIndexVersion indexVersion) {
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        // No exception should be thrown since we expect the upgrade of version to happen before applying the
        // create transaction.
        upgradeDbms(dbms);
        assertKernelVersion(database, previousVersion);
        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion, defaultSettings());
            tx.commit();
        }

        assertKernelVersion(database, LATEST_KERNEL_VERSION);
        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    private static Stream<Arguments> indexVersions() {
        return Stream.of(
                Arguments.of(EntityType.NODE, VectorIndexVersion.V1_0),
                Arguments.of(EntityType.NODE, VectorIndexVersion.V2_0),
                Arguments.of(EntityType.RELATIONSHIP, VectorIndexVersion.V2_0));
    }

    @ParameterizedTest
    @MethodSource("introducedSettings")
    void shouldBeBlockedFromCreatingVectorIndexWithNewSettingsOnOlderVersion(
            EntityType entityType, IndexSetting setting, Object validValue) {
        final var indexVersion = VectorIndexVersion.V2_0;
        final var introducedKernelVersion = VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
        final var previousVersion = previousFrom(introducedKernelVersion);
        setup(previousVersion);
        assertThatThrownBy(() -> {
                    try (final var tx = database.beginTx()) {
                        createIndex(
                                tx, entityType, indexVersion, defaultSettings().set(setting, validValue));
                        tx.commit();
                    }
                })
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContainingAll(
                        "Failed to create vector index with provided settings.",
                        "Version was",
                        previousVersion.name(),
                        "but required version for operation is",
                        introducedKernelVersion.name(),
                        "Please upgrade dbms using",
                        "dbms.upgrade()");
    }

    @ParameterizedTest
    @MethodSource("introducedSettings")
    void shouldBePossibleToCreateVectorIndexWithNewSettingsAfterUpgrade(
            EntityType entityType, IndexSetting setting, Object validValue) {
        final var indexVersion = VectorIndexVersion.V2_0;
        final var introducedKernelVersion = VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
        final var previousVersion = previousFrom(introducedKernelVersion);
        setup(previousVersion);
        UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, LATEST_KERNEL_VERSION);

        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion, defaultSettings().set(setting, validValue));
            tx.commit();
        }

        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    @ParameterizedTest
    @MethodSource("introducedSettings")
    void createVectorIndexWithNewSettingsShouldTriggerUpgrade(
            EntityType entityType, IndexSetting setting, Object validValue) {
        final var indexVersion = VectorIndexVersion.V2_0;
        final var introducedKernelVersion = VectorIndexConfigUtils.INDEX_SETTING_INTRODUCED_VERSIONS.get(setting);
        final var previousVersion = previousFrom(introducedKernelVersion);
        setup(previousVersion);
        // No exception should be thrown since we expect the upgrade of version to happen before applying the
        // create transaction.
        upgradeDbms(dbms);
        assertKernelVersion(database, previousVersion);
        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion, defaultSettings().set(setting, validValue));
            tx.commit();
        }

        assertKernelVersion(database, LATEST_KERNEL_VERSION);
        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    private static Stream<Arguments> introducedSettings() {
        return Stream.of(
                        Tuples.pair(IndexSetting.vector_Quantization(), VectorQuantization.LUCENE.name()),
                        Tuples.pair(IndexSetting.vector_Hnsw_M(), 32),
                        Tuples.pair(IndexSetting.vector_Hnsw_M(), 256))
                .flatMap(pair -> Arrays.stream(EntityType.values())
                        .map(entityType -> Arguments.of(entityType, pair.getOne(), pair.getTwo())));
    }

    private void createIndex(
            Transaction tx, EntityType entityType, VectorIndexVersion indexVersion, VectorIndexSettings settings) {
        try {
            final var ktx = ((TransactionImpl) tx).kernelTransaction();
            final var propKeyId = Tokens.Factories.PROPERTY_KEY.getId(ktx, PROP_KEY);
            final var schemaDescriptor =
                    switch (entityType) {
                        case NODE -> SchemaDescriptors.forLabel(Tokens.Factories.LABEL.getId(ktx, LABEL), propKeyId);
                        case RELATIONSHIP -> SchemaDescriptors.forRelType(
                                Tokens.Factories.RELATIONSHIP_TYPE.getId(ktx, REL_TYPE), propKeyId);
                    };
            final var prototype = IndexPrototype.forSchema(schemaDescriptor)
                    .withIndexType(IndexType.VECTOR)
                    .withIndexProvider(indexVersion.descriptor())
                    .withIndexConfig(settings.toIndexConfig());
            ktx.schemaWrite().indexCreate(prototype);
        } catch (KernelException exception) {
            throw new RuntimeException(exception);
        }
    }

    private VectorIndexSettings defaultSettings() {
        return VectorIndexSettings.from(IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi()));
    }

    private KernelVersion previousFrom(KernelVersion kernelVersion) {
        if (kernelVersion == KernelVersion.GLORIOUS_FUTURE) {
            return LatestVersions.LATEST_KERNEL_VERSION;
        }

        final var previous = (byte) (kernelVersion.version() - 1);
        return KernelVersion.getForVersion(previous);
    }

    private void setup(KernelVersion kernelVersion) {
        final var store =
                switch (kernelVersion) {
                    case V5_10 -> ZippedStoreCommunity.REC_AF11_V510_EMPTY;
                    case V5_15 -> ZippedStoreCommunity.REC_AF11_V515_EMPTY;
                    case V5_22 -> ZippedStoreCommunity.REC_AF11_V522_EMPTY;
                    default -> throw new InvalidArgumentException("Test not setup to find a %s for %s."
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
                .setConfig(GraphDatabaseInternalSettings.latest_runtime_version, LATEST_RUNTIME_VERSION.getVersion())
                .setConfig(GraphDatabaseInternalSettings.latest_kernel_version, LATEST_KERNEL_VERSION.version())
                .build();
        database = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertKernelVersion(database, snapshot.statistics().kernelVersion());
    }
}
