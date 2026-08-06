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
package org.neo4j.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Consumer;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation;
import org.neo4j.kernel.impl.transaction.EmptyBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public class UpgradeTestUtil {

    private static final Consumer<Iterable<? extends StorageCommand>> ASSERT_DEFAULT_UPGRADE_TRANSACTION =
            c -> assertThat(c).hasSize(1).first().isInstanceOf(StorageCommand.VersionUpgradeCommand.class);

    public static void upgradeDatabase(
            DatabaseManagementService dbms,
            GraphDatabaseAPI db,
            KernelVersion expectedCurrentVersion,
            KernelVersion expectedUpgradedVersions) {
        assertKernelVersion(db, expectedCurrentVersion);

        upgradeDbms(dbms);
        createWriteTransaction(db);

        assertKernelVersion(db, expectedUpgradedVersions);
    }

    public static void createWriteTransaction(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }

    private static KernelVersion getKernelVersion(GraphDatabaseAPI db) {
        return db.getDependencyResolver()
                .resolveDependency(KernelVersionProvider.class)
                .kernelVersion();
    }

    public static void assertKernelVersion(GraphDatabaseAPI database, KernelVersion expectedVersion) {
        assertThat(getKernelVersion(database)).isEqualTo(expectedVersion);
    }

    public static void upgradeDbms(DatabaseManagementService dbms) {
        final var system = dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        try {
            manuallyUpgrade(system);
        } catch (Exception e) {
            throw new RuntimeException("this was unexpected...", e);
        }
    }

    public static void assertUpgradeTransactionInOrder(
            KernelVersion from, KernelVersion to, long fromTxId, GraphDatabaseAPI db) throws Exception {
        assertUpgradeTransactionInOrder(from, to, fromTxId, db, ASSERT_DEFAULT_UPGRADE_TRANSACTION);
    }

    public static void assertUpgradeTransactionInOrder(
            KernelVersion from,
            KernelVersion to,
            long fromTxId,
            GraphDatabaseAPI db,
            Consumer<Iterable<? extends StorageCommand>> upgradeTransactionRequirement)
            throws Exception {
        LogicalTransactionStore lts = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        assertUpgradeTransactionInOrder(
                from, to, fromTxId, () -> lts.getCommandBatches(fromTxId + 1), upgradeTransactionRequirement);
    }

    public static void assertUpgradeTransactionInOrder(
            KernelVersion from,
            KernelVersion to,
            long fromTxId,
            ThrowingSupplier<CommandBatchCursor, IOException> commandBatchCursorSupplier)
            throws Exception {
        assertUpgradeTransactionInOrder(
                from, to, fromTxId, commandBatchCursorSupplier, ASSERT_DEFAULT_UPGRADE_TRANSACTION);
    }

    public static void assertUpgradeTransactionInOrder(
            KernelVersion from,
            KernelVersion to,
            long fromTxId,
            ThrowingSupplier<CommandBatchCursor, IOException> commandBatchCursorSupplier,
            Consumer<Iterable<? extends StorageCommand>> upgradeTransactionRequirement)
            throws Exception {
        ArrayList<KernelVersion> transactionVersions = new ArrayList<>();
        ArrayList<CommittedCommandBatchRepresentation> transactions = new ArrayList<>();
        try (CommandBatchCursor commandBatchCursor = commandBatchCursorSupplier.get()) {
            while (commandBatchCursor.next()) {
                CommittedCommandBatchRepresentation committedCommandBatchRepresentation = commandBatchCursor.get();
                if (committedCommandBatchRepresentation.txId() > fromTxId) {
                    transactions.add(committedCommandBatchRepresentation);
                    KernelVersion kernelVersion;
                    if (committedCommandBatchRepresentation instanceof EmptyBatchRepresentation empty) {
                        kernelVersion = empty.kernelVersion();
                    } else {
                        CompleteBatchRepresentation representation =
                                (CompleteBatchRepresentation) committedCommandBatchRepresentation;
                        kernelVersion = representation.startEntry().kernelVersion();
                    }
                    transactionVersions.add(kernelVersion);
                }
            }
        }
        assertThat(transactionVersions)
                .hasSizeGreaterThanOrEqualTo(2); // at least upgrade transaction and the triggering transaction
        assertThat(transactionVersions)
                .isSortedAccordingTo(
                        Comparator.comparingInt(KernelVersion::versionAsInt)); // Sorted means everything is in order
        assertThat(transactionVersions.getFirst()).isEqualTo(from); // First should be "from" version
        assertThat(transactionVersions.getLast()).isEqualTo(to); // And last the "to" version

        int indexFirstOnNew = transactionVersions.indexOf(to);
        // Upgrade should be last on old version
        CommittedCommandBatchRepresentation upgradeTransaction = transactions.get(indexFirstOnNew - 1);
        CommandBatch commands = upgradeTransaction.commandBatch();
        assertThat(commands).satisfies(upgradeTransactionRequirement);
    }

    public static void manuallyUpgrade(GraphDatabaseService systemDb) throws Exception {
        ((GraphDatabaseAPI) systemDb)
                .getDependencyResolver()
                .resolveDependency(SystemGraphComponents.class)
                .upgradeToCurrent(systemDb);
    }
}
