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
package org.neo4j.kernel.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.UpgradeTestUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.WALService;
import org.neo4j.wal.files.TransactionLogFilesHelper;

@Neo4jLayoutExtension
public class LogFormatSwitchRecoveryIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI systemDb;

    @BeforeEach
    void setUp() {
        startDbms(this::configureLegacyLogsAsLatest, false);
    }

    @AfterEach
    void tearDown() {
        shutdownDbms();
    }

    private void startDbms(
            RecoveryToFutureOverUpgradedVersionsIT.Configuration configuration, boolean allowAutomaticUpgrade) {
        managementService = configuration
                .configure(new TestDatabaseManagementServiceBuilder(neo4jLayout)
                        .setConfig(preallocate_logical_logs, false)
                        .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                        .setConfig(automatic_upgrade_enabled, allowAutomaticUpgrade))
                .build();
        systemDb = (GraphDatabaseAPI) managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    private void shutdownDbms() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    private static DbmsRuntimeVersion findDbmsVersionMatchingKernelVersion(KernelVersion version) {
        for (DbmsRuntimeVersion dbmsRuntimeVersion : DbmsRuntimeVersion.VERSIONS) {
            if (dbmsRuntimeVersion.kernelVersion() == version) {
                return dbmsRuntimeVersion;
            }
        }
        throw new IllegalArgumentException("No matching Dbms version found for " + version.toString());
    }

    private TestDatabaseManagementServiceBuilder configureLegacyLogsAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        LatestVersions.LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES.getVersion())
                .setConfig(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES.version());
    }

    private TestDatabaseManagementServiceBuilder configureEnvelopedLogsAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        findDbmsVersionMatchingKernelVersion(
                                        KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED)
                                .getVersion())
                .setConfig(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED.version())
                .setConfig(GraphDatabaseInternalSettings.envelope_log_format_on_future, true);
    }

    private void writeGraph(GraphDatabaseAPI testDb, int generation) {
        int baseLinkCount = (generation * (generation - 1) / 2) + 1;
        try (Transaction tx = testDb.beginTx()) {
            String generationName = "gen" + generation;
            Label generationLabel = Label.label(generationName);
            Node node1 = tx.createNode(generationLabel);
            node1.setProperty("name", generationName + "_1");
            Node node2 = tx.createNode(generationLabel);
            node2.setProperty("name", generationName + "_2");
            String linkName = "link" + baseLinkCount++;
            Relationship rel = node1.createRelationshipTo(node2, RelationshipType.withName(linkName));
            rel.setProperty("name", linkName);
            for (int prevGeneration = 1; prevGeneration < generation; prevGeneration++) {
                String prevGenerationName = "gen" + prevGeneration;
                Label prevGenerationLabel = Label.label(prevGenerationName);
                // link to an older node in previous checkpoint
                Node oldNode = tx.findNode(prevGenerationLabel, "name", prevGenerationName + "_1");
                linkName = "link" + baseLinkCount++;
                Relationship rel2 = oldNode.createRelationshipTo(node2, RelationshipType.withName(linkName));
                rel2.setProperty("name", linkName);
            }
            tx.commit();
        }
    }

    private void verifyGraph(GraphDatabaseAPI testDb, int generation) {
        int linkCount = 1;
        try (Transaction tx = testDb.beginTx()) {
            // ensure no additional nodes, or relationships
            assertEquals(2L * generation, Iterables.count(tx.getAllNodes()));
            assertEquals(generation * (generation + 1L) / 2L, Iterables.count(tx.getAllRelationships()));
            for (int currentGeneration = 1; currentGeneration <= generation; currentGeneration++) {
                String generationName = "gen" + currentGeneration;
                Label generationLabel = Label.label(generationName);
                assertNotNull(tx.findNode(generationLabel, "name", generationName + "_1"));
                assertNotNull(tx.findNode(generationLabel, "name", generationName + "_2"));
                for (int prevGeneration = 1; prevGeneration < currentGeneration; prevGeneration++) {
                    String linkName = "link" + linkCount++;
                    assertNotNull(tx.findRelationship(RelationshipType.withName(linkName), "name", linkName));
                }
            }
        }
        assertKernelVersion(testDb, KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED);
    }

    private record DatabaseAndSnapshot(GraphDatabaseAPI testDb, Path snapshot, DatabaseLayout layout) {
        String dbName() {
            return testDb.databaseName();
        }
    }

    private DatabaseAndSnapshot prepareUpgradedDatabaseAndSnapshot(String dbName) throws Exception {
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        // write an initial graph captured in the snapshot
        writeGraph(testDb, 1);
        DatabaseLayout layout = neo4jLayout.databaseLayout(testDb.databaseName());
        shutdownDbms();
        // capture DB store with an older version set of nodes and relationships
        Path initialState = layout.databaseDirectory().getParent().resolve("original");
        fileSystem.copyRecursively(layout.databaseDirectory(), initialState);
        // add some more nodes and relationships still under old version
        // but which will ultimately be replayed during recovery
        startDbms(builder -> builder, false);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        // add some nodes and relationships to restore on the old version
        writeGraph(testDb, 2);
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES);
        shutdownDbms();
        // Upgrade to new enveloped logs
        startDbms(this::configureEnvelopedLogsAsLatest, false);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        UpgradeTestUtil.manuallyUpgrade(systemDb);
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES);
        // add some nodes and relationships to restore on the new version
        writeGraph(testDb, 3);
        assertKernelVersion(testDb, KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED);
        return new DatabaseAndSnapshot(testDb, initialState, layout);
    }

    private void restoreDBSnapshot(DatabaseAndSnapshot databaseAndSnapshot) throws Exception {
        // revert the DB store to version with only 2 nodes
        fileSystem.deleteRecursively(databaseAndSnapshot.layout.databaseDirectory());
        fileSystem.copyRecursively(databaseAndSnapshot.snapshot, databaseAndSnapshot.layout.databaseDirectory());
        // kill all checkpoints
        SequentialFileNameHelper checkpointMatcher =
                TransactionLogFilesHelper.forCheckpoints(databaseAndSnapshot.layout.getTransactionLogsDirectory());
        fileSystem.deleteRecursively(
                databaseAndSnapshot.layout.getTransactionLogsDirectory(), checkpointMatcher::isSequentialFile);
    }

    private void verifyRecovery(String dbName, boolean allowCorruption) throws Exception {
        Config config = Config.newBuilder()
                .set(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        findDbmsVersionMatchingKernelVersion(
                                        KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED)
                                .getVersion())
                .set(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED
                                .version()) // allow truncation of corrupted file
                .set(GraphDatabaseInternalSettings.fail_on_corrupted_log_files, !allowCorruption)
                .build();
        DatabaseLayout layout = neo4jLayout.databaseLayout(dbName);
        // should recover on first attempt
        assertTrue(RecoveryHelpers.runRecovery(layout, fileSystem, config));
        // should not require any further recovery
        assertFalse(Recovery.isRecoveryRequired(fileSystem, layout, config, EmptyMemoryTracker.INSTANCE));
        // restart DB and verify content is complete
        startDbms(this::configureEnvelopedLogsAsLatest, false);
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        // validate all 3 generations restored
        verifyGraph(testDb, 3);
        assertKernelVersion(testDb, KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED);
    }

    @Test
    void recoveryShouldReRunOverOldAndNew() throws Exception {
        // create a basic database without enveloped logs and snapshot the gen 1 data
        // then upgrade and add further data to be replayed during recovery
        // gen 2 after snapshot and before upgrade. gen 3 on enveloped logs
        DatabaseAndSnapshot databaseAndSnapshot = prepareUpgradedDatabaseAndSnapshot(DEFAULT_DATABASE_NAME);
        verifyGraph(databaseAndSnapshot.testDb, 3);
        shutdownDbms();
        // roll store files back to state on gen 1 version of data
        // and clear checkpoints
        restoreDBSnapshot(databaseAndSnapshot);
        // confirm recovery process and data integrity
        verifyRecovery(databaseAndSnapshot.dbName(), false);
    }

    @Test
    void recoveryWithUpgradeAndCorruptedItems() throws Exception {
        // create a basic database without enveloped logs and snapshot the gen 1 data
        // then upgrade and add further data to be replayed during recovery
        // gen 2 after snapshot and before upgrade. gen 3 on enveloped logs
        DatabaseAndSnapshot databaseAndSnapshot = prepareUpgradedDatabaseAndSnapshot(DEFAULT_DATABASE_NAME);
        // ensure gen3 flushed to uncorrupted log
        LogFile logFile = databaseAndSnapshot
                .testDb
                .getDependencyResolver()
                .resolveDependency(WALService.class)
                .getLogFile();
        logFile.rotate();
        Path lastLog = logFile.getLogFileForVersion(logFile.getCurrentLogVersion());
        // add some gen 4 content to corrupt
        writeGraph(databaseAndSnapshot.testDb, 4);
        verifyGraph(databaseAndSnapshot.testDb, 4);
        shutdownDbms();
        // roll store files back to state on gen 1 version of data
        // and clear checkpoints
        restoreDBSnapshot(databaseAndSnapshot);
        // corrupt logfile
        try (StoreFileChannel logChannel = fileSystem.write(lastLog)) {
            logChannel.position(128 * 1024); // skip header
            ByteBuffer zeros = ByteBuffer.allocate(10);
            logChannel.write(zeros);
            logChannel.flush();
        }
        // confirm clean recovery process and data integrity up to generation 3
        // with gen 4 excised
        verifyRecovery(databaseAndSnapshot.dbName(), true);
    }

    @Test
    void recoveryWithUpgradeAndTruncation() throws Exception {
        // create a basic database without enveloped logs and snapshot the gen 1 data
        // then upgrade and add further data to be replayed during recovery
        // gen 2 after snapshot and before upgrade. gen 3 on enveloped logs
        DatabaseAndSnapshot databaseAndSnapshot = prepareUpgradedDatabaseAndSnapshot(DEFAULT_DATABASE_NAME);
        // capture current transaction log position
        LogFile logFile = databaseAndSnapshot
                .testDb
                .getDependencyResolver()
                .resolveDependency(WALService.class)
                .getLogFile();
        LogPosition position = logFile.getTransactionLogWriter().getCurrentPosition();
        Path currentLogFile = logFile.getLogFileForVersion(logFile.getCurrentLogVersion());
        // add some gen 4 content to truncate
        writeGraph(databaseAndSnapshot.testDb, 4);
        verifyGraph(databaseAndSnapshot.testDb, 4);
        shutdownDbms();
        // roll store files back to state on gen 1 version of data
        // and clear checkpoints
        restoreDBSnapshot(databaseAndSnapshot);
        // truncate logfile
        fileSystem.truncate(currentLogFile, position.getByteOffset());
        // confirm clean recovery process and data integrity up to generation 3
        // with gen 4 excised
        verifyRecovery(databaseAndSnapshot.dbName(), false);
    }
}
