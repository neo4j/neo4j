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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
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
    @MethodSource("indexes")
    void shouldBeBlockedFromCreatingVectorIndexOnOlderVersion(EntityType entityType, VectorIndexVersion indexVersion) {
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        assertThatThrownBy(() -> {
                    try (final var tx = database.beginTx()) {
                        createIndex(tx, entityType, indexVersion);
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
    @MethodSource("indexes")
    void shouldBePossibleToCreateVectorIndexAfterUpgrade(EntityType entityType, VectorIndexVersion indexVersion) {
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        UpgradeTestUtil.upgradeDatabase(dbms, database, previousVersion, LatestVersions.LATEST_KERNEL_VERSION);

        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion);
            tx.commit();
        }

        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void createVectorIndexShouldTriggerUpgrade(EntityType entityType, VectorIndexVersion indexVersion) {
        final var previousVersion = previousFrom(indexVersion.minimumRequiredKernelVersion());
        setup(previousVersion);
        // No exception should be thrown since we expect the upgrade of version to happen before applying the
        // create transaction.
        upgradeDbms(dbms);
        assertKernelVersion(database, previousVersion);
        try (final var tx = database.beginTx()) {
            createIndex(tx, entityType, indexVersion);
            tx.commit();
        }

        assertKernelVersion(database, LatestVersions.LATEST_KERNEL_VERSION);
        try (final var tx = database.beginTx()) {
            assertThat(tx.schema().getIndexes()).hasSize(1);
        }
    }

    private static Stream<Arguments> indexes() {
        return Stream.of(Arguments.of(EntityType.NODE, VectorIndexVersion.V1_0));
    }

    private void createIndex(Transaction tx, EntityType entityType, VectorIndexVersion indexVersion) {
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
                    .withIndexConfig(IndexSettingUtil.defaultConfigForTest(IndexType.VECTOR.toPublicApi()));
            ktx.schemaWrite().indexCreate(prototype);
        } catch (KernelException exception) {
            throw new RuntimeException(exception);
        }
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
                .setConfig(automatic_upgrade_enabled, false)
                .build();
        database = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertKernelVersion(database, snapshot.statistics().kernelVersion());
    }
}
