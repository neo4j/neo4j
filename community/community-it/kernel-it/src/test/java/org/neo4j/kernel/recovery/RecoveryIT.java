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

import static java.lang.String.valueOf;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.BIGGEST_HEADER;
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.STOP;
import static org.neo4j.kernel.recovery.Recovery.context;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLogFile;
import static org.neo4j.kernel.recovery.facade.RecoveryCriteria.ALL;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.tracer.DefaultDatabaseTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.LatestCheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.enveloped.InconsistentLogFilesException;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogRangeInfo;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.facade.RecoveryCriteria;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFileSelection;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
@RandomSupportExtension
@SkipOnSpd
class RecoveryIT {
    private static final int TEN_KB = (int) ByteUnit.kibiBytes(10);
    private static final CursorContextFactory CONTEXT_FACTORY = NULL_CONTEXT_FACTORY;

    private static final IdType TEST_NODE_TYPE = new IdType() {
        @Override
        public String name() {
            return "TestNodeId";
        }

        @Override
        public boolean isSchemaType() {
            return false;
        }

        @Override
        public boolean respectsReservedId() {
            return true;
        }

        @Override
        public boolean highActivity() {
            return false;
        }
    };

    @Inject
    DefaultFileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private RandomSupport random;

    @Inject
    TestDirectory dir;

    DatabaseLayout databaseLayout;

    private TestDatabaseManagementServiceBuilder builder;
    DatabaseManagementService managementService;
    private FakeClock fakeClock;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp() {
        databaseLayout = neo4jLayout.databaseLayout(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void avoidRescanningLogTailInfoOnRecovery() throws Exception {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        // shutdown and init checkpoints
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        CheckpointTracer checkpointTracer = new CheckpointTracer();
        var tracers =
                new DatabaseTracers(checkpointTracer, LockTracer.NONE, PageCacheTracer.NULL, VersionStorageTracer.NULL);
        recoverDatabase(tracers);

        // we should have only one pass over log tails during recovery. 1 check is tail scan to see if recovery is
        // required
        assertEquals(1 + 1, checkpointTracer.getCheckpointOpenCounter());
    }

    @Test
    void isRecoveryRequiredCheckKeepsEmptyLastCheckpointFile() throws Exception {
        var database = createDatabase();
        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);

        generateSomeData(database);

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        logFiles.getCheckpointFile().rotate();

        generateSomeData(database);
        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        int checkpointFilesWithoutVictim = countCheckpointFiles();

        generateSomeData(database);
        // now we have 2 checkpoint log files with checkpoints and one empty
        var victimFilePath = logFiles.getCheckpointFile().rotate().file();
        var config = database.getDependencyResolver().resolveDependency(Config.class);

        managementService.shutdown();

        prepareEmptyLogFile(victimFilePath);

        assertNotEquals(checkpointFilesWithoutVictim, countCheckpointFiles());
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, config, INSTANCE));
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, config, INSTANCE));
        assertNotEquals(checkpointFilesWithoutVictim, countCheckpointFiles());
    }

    @Test
    void recoverDatabaseWithEmptyLastCheckpointFileAndRemoveThatFileAfterRecovery() throws Exception {
        var database = createDatabase();
        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);

        generateSomeData(database);

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        logFiles.getCheckpointFile().rotate();

        generateSomeData(database);
        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        int checkpointFilesWithoutVictim = countCheckpointFiles();

        generateSomeData(database);
        // now we have 2 checkpoint log files with checkpoints and one empty
        var victimFilePath = logFiles.getCheckpointFile().rotate().file();

        managementService.shutdown();

        prepareEmptyLogFile(victimFilePath);

        assertNotEquals(checkpointFilesWithoutVictim, countCheckpointFiles());

        recoverDatabase();

        assertEquals(checkpointFilesWithoutVictim, countCheckpointFiles());
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void recoverDatabaseWithEmptyPreallocatedLastCheckpointFileAndRemoveThatFileAfterRecovery() throws Exception {
        var database = createDatabase();
        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);

        generateSomeData(database);

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        logFiles.getCheckpointFile().rotate();

        generateSomeData(database);
        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        int checkpointFilesWithoutVictim = countCheckpointFiles();

        generateSomeData(database);
        // now we have 2 checkpoint log files with checkpoints and one empty
        var victimFilePath = logFiles.getCheckpointFile().rotate().file();

        managementService.shutdown();

        prepareEmptyZeroedLogFile(victimFilePath);

        assertNotEquals(checkpointFilesWithoutVictim, countCheckpointFiles());

        recoverDatabase();

        assertEquals(checkpointFilesWithoutVictim, countCheckpointFiles());
    }

    @Test
    void recoverTxLogsWithPartiallyWrittenLastRecordInFirstTransactionAfterCheckpoint() throws Exception {
        var database = createDatabase();
        assumeRecordStorageEngine(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var logFileToManipulate = logFiles.getLogFile().getLogRangeInfo().highestFile();

        var marker = withName("Type");
        var propertyName = "a";

        // we do transaction before checkpoint to force token creation that we will use after checkpoint to make sure
        // that its a first transaction after the checkpoint
        try (Transaction tx = database.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, marker);
            node2.setProperty(propertyName, "b");
            tx.commit();
        }

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));

        LogPosition position = logFiles.getLogFile().getTransactionLogWriter().getCurrentPosition();

        // our test big transaction
        try (Transaction tx = database.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, marker);
            node2.setProperty(propertyName, random.nextAlphaNumericString(TEN_KB));
            tx.commit();
        }
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // we write big transaction with huge property command above and here we truncate a bit of that to simulate
        // partially written command
        removeLastKbFromLogFile(logFileToManipulate, position);

        recoverDatabase();
    }

    @Test
    void recoverTxLogsWithPartiallyWrittenLastRecordInFirstTransactionAfterCheckpointFailOnLastPartial()
            throws Exception {
        var database = createDatabase();
        assumeRecordStorageEngine(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var logFileToManipulate = logFiles.getLogFile().getLogRangeInfo().highestFile();

        var marker = withName("Type");
        var propertyName = "a";

        // we do transaction before checkpoint to force token creation that we will use after checkpoint to make sure
        // that its a first transaction after the checkpoint
        try (Transaction tx = database.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, marker);
            node2.setProperty(propertyName, "b");
            tx.commit();
        }

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));

        LogPosition position = logFiles.getLogFile().getTransactionLogWriter().getCurrentPosition();

        // our test big transaction
        try (Transaction tx = database.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, marker);
            node2.setProperty(propertyName, random.nextAlphaNumericString(TEN_KB));
            tx.commit();
        }
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // we write big transaction with huge property command above and here we truncate a bit of that to simulate
        // partially written command
        removeLastKbFromLogFile(logFileToManipulate, position);

        Config config = defaults();
        assertTrue(isRecoveryRequired(databaseLayout, config, EMPTY));

        // Ask recover to not accept last broken records (piggybacks on force fail on corrupted).
        Recovery.Context context = Recovery.contextWithNoLogTail(
                        fileSystem,
                        pageCache,
                        EMPTY,
                        config,
                        databaseLayout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        LATEST_KERNEL_VERSION_PROVIDER)
                .forceFailOnCorruptedLogs();
        assertThatThrownBy(() -> performRecovery(context.recoveryPredicate(RecoveryPredicate.ALL)
                        .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                        .clock(fakeClock)))
                .rootCause()
                .isInstanceOfAny(ChecksumMismatchException.class, InconsistentLogFilesException.class);
    }

    @Test
    void recoverTxLogsWithBrokenFirstEntryInFirstTransactionAfterCheckpoint() throws Exception {
        var database = createDatabase();

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var logFileToManipulate = logFiles.getLogFile().getLogRangeInfo().highestFile();
        var positionForCorruption =
                logFiles.getLogFile().getTransactionLogWriter().getCurrentPosition();

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // append data that will cause broken next entry
        appendBytesToLastLogFile(logFileToManipulate, positionForCorruption, new byte[] {1, 0, 0});

        // we check that recovery is required! (we have broken tail) and do actual recovery
        recoverDatabase();

        var recoveredDatabase = createDatabase();
        // we truncate broken bits and bytes as part of recovery
        LogFiles recoveredDbLogFiles = recoveredDatabase.getDependencyResolver().resolveDependency(LogFiles.class);
        assertEquals(
                positionForCorruption,
                recoveredDbLogFiles.getLogFile().getTransactionLogWriter().getCurrentPosition());
        //
        try (var tx = recoveredDatabase.beginTx()) {
            tx.createNode();
            tx.commit();
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void failToRecoverDatabaseWithCorruptedLastCheckpointFile() throws Exception {
        var database = createDatabase();
        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var checkpointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);

        generateSomeData(database);

        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        logFiles.getCheckpointFile().rotate();

        generateSomeData(database);
        checkpointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        int checkpointFilesWithoutVictim = countCheckpointFiles();

        generateSomeData(database);
        // now we have 2 checkpoint log files with checkpoints and one empty
        var victimFilePath = logFiles.getCheckpointFile().rotate().file();

        managementService.shutdown();

        prepareCorruptedLogFile(victimFilePath);

        assertThatThrownBy(this::recoverDatabase)
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has unreadable header but looks like it also contains some checkpoint data.");
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void recoverDatabaseWithEmptyLastLogFileAndRemoveThatFileAfterRecovery() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        logFiles.getLogFile().rotate();
        generateSomeData(database);

        int logFilesWithoutVictim = countTransactionLogFiles();

        // now we have 2 log files with some data and one empty
        var victimFilePath = logFiles.getLogFile().rotate().file();

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        prepareEmptyZeroedLogFile(victimFilePath);
        assertNotEquals(logFilesWithoutVictim, countTransactionLogFiles());

        recoverDatabase();

        assertEquals(logFilesWithoutVictim, countTransactionLogFiles());
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void recoverDatabaseWithEmptySeveralLastLogFileAndRemoveThatFileAfterRecovery() throws Exception {
        // should not happen but now we can handle that
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        logFiles.getLogFile().rotate();
        generateSomeData(database);

        int logFilesWithoutVictims = countTransactionLogFiles();

        // now we have 2 log files with some data and one empty
        var victimFilePath1 = logFiles.getLogFile().rotate().file();
        var victimFilePath2 = logFiles.getLogFile().rotate().file();

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        prepareEmptyZeroedLogFile(victimFilePath1);
        prepareEmptyZeroedLogFile(victimFilePath2);

        assertNotEquals(logFilesWithoutVictims, countTransactionLogFiles());

        recoverDatabase();

        assertEquals(logFilesWithoutVictims, countTransactionLogFiles());
    }

    @Test
    void recoverDatabaseWithEmptyNotPreallocatedLastLogFileAndRemoveThatFileAfterRecovery() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        logFiles.getLogFile().rotate();
        generateSomeData(database);

        int logFilesWithoutVictim = countTransactionLogFiles();

        // now we have 2 log files with some data and one empty
        var victimFilePath = logFiles.getLogFile().rotate().file();

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        prepareEmptyLogFile(victimFilePath);
        assertNotEquals(logFilesWithoutVictim, countTransactionLogFiles());

        recoverDatabase();

        assertEquals(logFilesWithoutVictim, countTransactionLogFiles());
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void recoverDatabaseWithEmptyLastLogFileWithCorrectNumberOfNodesAndRelationships() throws Exception {
        var database = createDatabase();
        var relationshipType = withName("marker");
        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);

        // file 0
        try (var tx = database.beginTx()) {
            var start = tx.createNode();
            var end = tx.createNode();
            start.createRelationshipTo(end, relationshipType);
            tx.commit();
        }
        logFiles.getLogFile().rotate();

        // file 1
        try (var tx = database.beginTx()) {
            var start = tx.createNode();
            var end = tx.createNode();
            start.createRelationshipTo(end, relationshipType);
            tx.commit();
        }
        logFiles.getLogFile().rotate();

        // file 2
        try (var tx = database.beginTx()) {
            var start = tx.createNode();
            var end = tx.createNode();
            start.createRelationshipTo(end, relationshipType);
            tx.commit();
        }
        logFiles.getLogFile().rotate();

        int logFilesWithoutVictim = countTransactionLogFiles();

        // now we have 2 log files with some data and one empty
        var victimFilePath = logFiles.getLogFile().rotate().file();

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        prepareEmptyZeroedLogFile(victimFilePath);
        assertNotEquals(logFilesWithoutVictim, countTransactionLogFiles());

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try (Transaction transaction = recoveredDatabase.beginTx()) {
            assertEquals(6, count(transaction.getAllNodes()));
            assertEquals(3, count(transaction.getAllRelationships()));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoveryWithLastBrokenRecord() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var logFile = logFiles.getLogFile();
        var currentPosition = logFile.getTransactionLogWriter().getCurrentPosition();
        var logFileToMutate = logFile.getLogFileForVersion(currentPosition.getLogVersion());

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // append data that will cause broken next entry
        appendBytesToLastLogFile(logFileToMutate, currentPosition, new byte[] {1, 0, 0});

        recoverDatabase();
    }

    @Test
    void recoveryWithDataAfterLastBrokenRecord() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var logFile = logFiles.getLogFile();
        var currentPosition = logFile.getTransactionLogWriter().getCurrentPosition();
        var logFileToMutate = logFile.getLogFileForVersion(currentPosition.getLogVersion());

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // append data that will cause broken next entry
        int minimumBytesToConsiderBrokenRecord =
                Config.defaults().get(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create)
                        ? LogEnvelopeHeader.HEADER_SIZE + 1
                        : 4;
        byte[] brokenRecord = new byte[minimumBytesToConsiderBrokenRecord];
        brokenRecord[0] = 1;
        brokenRecord[brokenRecord.length - 1] = 1;
        appendBytesToLastLogFile(logFileToMutate, currentPosition, brokenRecord);

        assertThatThrownBy(this::recoverDatabase)
                .hasStackTraceContaining(
                        "Failure to read transaction log file number 0. Unreadable bytes are encountered after last readable position.");
    }

    @Test
    void recoveryWithLastBrokenRecordTruncateFile() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var logFile = logFiles.getLogFile();
        var currentPosition = logFile.getTransactionLogWriter().getCurrentPosition();
        long fileSizeBeforeMutation = currentPosition.getByteOffset();
        var logFileToMutate = logFile.getLogFileForVersion(currentPosition.getLogVersion());

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // append data that will cause broken next entry
        appendBytesToLastLogFile(logFileToMutate, currentPosition, new byte[] {1, 2, 0, 0});

        assertNotEquals(fileSizeBeforeMutation, fileSystem.getFileSize(logFileToMutate));

        recoverDatabase();

        assertEquals(fileSizeBeforeMutation, fileSystem.getFileSize(logFileToMutate));
    }

    @Test
    void recoveryWithLastBrokenRecordTruncateBigFile() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var logFile = logFiles.getLogFile();
        var currentPosition = logFile.getTransactionLogWriter().getCurrentPosition();
        long fileSizeBeforeMutation = currentPosition.getByteOffset();
        var logFileToMutate = logFile.getLogFileForVersion(currentPosition.getLogVersion());

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        // append data that will cause broken next entry

        var data = new byte[(int) ByteUnit.kibiBytes(random.nextInt(120, 484))];
        data[0] = 1;
        data[1] = 2;
        appendBytesToLastLogFile(logFileToMutate, currentPosition, data);

        assertNotEquals(fileSizeBeforeMutation, fileSystem.getFileSize(logFileToMutate));

        recoverDatabase();

        assertEquals(fileSizeBeforeMutation, fileSystem.getFileSize(logFileToMutate));
    }

    @Test
    void recoveryWithDataLongAfterLastBrokenRecord() throws Exception {
        var database = createDatabase();
        generateSomeData(database);

        var logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        var logFile = logFiles.getLogFile();
        var currentPosition = logFile.getTransactionLogWriter().getCurrentPosition();
        var logFileToMutate = logFile.getLogFileForVersion(currentPosition.getLogVersion());

        managementService.shutdown();

        // remove shutdown checkpoint
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        var data = new byte[(int) ByteUnit.kibiBytes(random.nextInt(20, 789))];
        data[0] = 1;
        data[data.length - random.nextInt(2, 15)] = 2;
        // append data that will cause broken next entry
        appendBytesToLastLogFile(logFileToMutate, currentPosition, data);

        assertThatThrownBy(this::recoverDatabase)
                .hasStackTraceContaining(
                        "Failure to read transaction log file number 0. Unreadable bytes are encountered after last readable position.");
    }

    @Test
    void recoveryWithRemovedLogs() throws Exception {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        int removedFiles = 0;
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        Path[] files = FileUtils.listPaths(transactionLogsDirectory);
        for (Path file : files) {
            fileSystem.deleteFile(file);
            removedFiles++;
        }
        // we removed checkpoint and tx file log
        assertEquals(2, removedFiles);

        // we start db with default config and shut it down
        createDatabase();
        managementService.shutdown();

        // recovery is till required since log files are missing
        assertTrue(isRecoveryRequired(databaseLayout));

        // now we recover with missing log files flag
        recoverDatabase();
        // everything is fine now
        assertFalse(isRecoveryRequired(databaseLayout));

        GraphDatabaseAPI db = createDatabase();
        assertEquals(
                0,
                db.getDependencyResolver()
                        .resolveDependency(LogFiles.class)
                        .getCheckpointFile()
                        .getCurrentLogVersion());
    }

    @Test
    void recoveryWithRemovedOnlyTransactionLogs() throws Exception {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        // shutdown and init checkpoints
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        int removedFiles = 0;
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        Path[] files = FileUtils.listPaths(transactionLogsDirectory);
        for (Path file : files) {
            if (!file.getFileName().getFileName().toString().contains("checkpoint")) {
                fileSystem.deleteFile(file);
                removedFiles++;
            }
        }
        // we removed only tx file log
        assertEquals(1, removedFiles);

        // we start db with default config and shut it down
        createDatabase();
        managementService.shutdown();

        // recovery is till required since log files are missing
        assertTrue(isRecoveryRequired(databaseLayout));

        // now we recover with missing log files flag
        recoverDatabase();
        // everything is fine now
        assertFalse(isRecoveryRequired(databaseLayout));

        GraphDatabaseAPI db = createDatabase();
        assertEquals(
                0,
                db.getDependencyResolver()
                        .resolveDependency(LogFiles.class)
                        .getCheckpointFile()
                        .getCurrentLogVersion());
    }

    @Test
    void recoveryWithRemovedOnlyTransactionLogsAndLotsOfCheckpointFiles() throws Exception {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData(database);

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        LogFiles logFiles = dependencyResolver.resolveDependency(LogFiles.class);
        CheckPointer checkPointer = dependencyResolver.resolveDependency(CheckPointer.class);
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        for (int i = 0; i < 12; i++) {
            checkPointer.forceCheckPoint(new SimpleTriggerInfo("test"));
            checkpointFile.rotate();
        }
        managementService.shutdown();

        // we did a lot of rotations and now in a range of 10-12 files
        LogRangeInfo logRangeInfo = checkpointFile.getLogRangeInfo();
        assertEquals(10, logRangeInfo.lowestVersion());
        assertEquals(12, logRangeInfo.highestVersion());

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        int removedFiles = 0;
        int checkpointFilesLeftOvers = 0;
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        for (Path file : fileSystem.listFiles(transactionLogsDirectory)) {
            if (!file.getFileName().getFileName().toString().contains("checkpoint")) {
                fileSystem.deleteFile(file);
                removedFiles++;
            } else {
                checkpointFilesLeftOvers++;
            }
        }
        // we removed only tx file log
        assertEquals(1, removedFiles);

        // before recovery we have lots of checkpoint files that point to nowhere
        assertEquals(3, checkpointFilesLeftOvers);

        // now we recover with missing log files flag and corrupted log file fine flag
        var recoveryDbms = dbmsWithFailOnCorruptedFalse();
        recoveryDbms.shutdown();

        assertFalse(isRecoveryRequired(databaseLayout));

        // restart in normal mode
        GraphDatabaseAPI db = createDatabase();

        // we have only one checkpoint file now with the highest version
        var restoredCheckpoint =
                db.getDependencyResolver().resolveDependency(LogFiles.class).getCheckpointFile();
        assertEquals(12, restoredCheckpoint.getCurrentLogVersion());
        assertEquals(1, restoredCheckpoint.getMatchedFiles().length);
    }

    @Test
    void recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() throws Throwable {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        assertTrue(isRecoveryRequired(databaseLayout));
    }

    @Test
    void recoveryNotRequiredWhenDatabaseNotFound() throws Exception {
        DatabaseLayout absentDatabase = neo4jLayout.databaseLayout("absent");
        assertFalse(isRecoveryRequired(absentDatabase));
    }

    @Test
    void recoverEmptyDatabase() throws Throwable {
        // The database is only completely empty if we skip the creation of the default indexes initially.
        // Without skipping there will be entries in the transaction logs for the default token indexes, so recovery is
        // required if we remove the checkpoint.
        Config config = Config.newBuilder().set(preallocate_logical_logs, false).build();

        managementService = new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(config)
                .build();
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        assertFalse(isRecoveryRequired(databaseLayout, config));
    }

    @Test
    void recoverDatabaseWithNodes() throws Throwable {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        for (int i = 0; i < numberOfNodes; i++) {
            createSingleNode(database);
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try (Transaction tx = recoveredDatabase.beginTx()) {
            assertEquals(numberOfNodes, count(tx.getAllNodes()));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void tracePageCacheAccessOnDatabaseRecovery() throws Throwable {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        for (int i = 0; i < numberOfNodes; i++) {
            createSingleNode(database);
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        var pageCacheTracer = new DefaultPageCacheTracer();
        var tracers =
                new DatabaseTracers(DatabaseTracer.NULL, LockTracer.NONE, pageCacheTracer, VersionStorageTracer.NULL);
        recoverDatabase(tracers);

        long pins = pageCacheTracer.pins();
        assertThat(pins).isGreaterThan(0);
        assertThat(pageCacheTracer.unpins()).isEqualTo(pins);
        assertThat(pageCacheTracer.hits()).isGreaterThan(0).isLessThanOrEqualTo(pins);
        assertThat(pageCacheTracer.faults()).isGreaterThan(0).isLessThanOrEqualTo(pins);

        GraphDatabaseService recoveredDatabase = createDatabase();
        try (Transaction tx = recoveredDatabase.beginTx()) {
            assertEquals(numberOfNodes, count(tx.getAllNodes()));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodesAndRelationshipsAndRelationshipTypes() throws Throwable {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for (int i = 0; i < numberOfRelationships; i++) {
            try (Transaction transaction = database.beginTx()) {
                Node start = transaction.createNode();
                Node stop = transaction.createNode();
                start.createRelationshipTo(stop, withName(valueOf(i)));
                transaction.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try (Transaction transaction = recoveredDatabase.beginTx()) {
            assertEquals(numberOfNodes, count(transaction.getAllNodes()));
            assertEquals(numberOfRelationships, count(transaction.getAllRelationships()));
            assertEquals(numberOfRelationships, count(transaction.getAllRelationshipTypesInUse()));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithProperties() throws Throwable {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for (int i = 0; i < numberOfRelationships; i++) {
            try (Transaction transaction = database.beginTx()) {
                Node start = transaction.createNode();
                Node stop = transaction.createNode();
                start.setProperty("start" + i, i);
                stop.setProperty("stop" + i, i);
                start.createRelationshipTo(stop, withName(valueOf(i)));
                transaction.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try (Transaction transaction = recoveredDatabase.beginTx()) {
            assertEquals(numberOfNodes, count(transaction.getAllNodes()));
            assertEquals(numberOfRelationships, count(transaction.getAllRelationships()));
            assertEquals(numberOfRelationships, count(transaction.getAllRelationshipTypesInUse()));
            assertEquals(numberOfNodes, count(transaction.getAllPropertyKeys()));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithConstraint() throws Exception {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        String property = "prop";
        Label label = Label.label("myLabel");

        try (Transaction tx = database.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(property).create();
            tx.commit();
        }

        for (int i = 0; i < numberOfNodes; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(label);

                node.setProperty(property, i);
                tx.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        try {
            // let's verify that the constraint has recovered with all values
            // by trying to create duplicates of all nodes under the constraint
            for (int i = 0; i < numberOfNodes; i++) {
                int finalInt = i;
                assertThrows(ConstraintViolationException.class, () -> {
                    try (Transaction tx = recoveredDatabase.beginTx()) {
                        Node node = tx.createNode(label);

                        node.setProperty(property, finalInt);
                        tx.commit();
                    }
                });
            }
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithRelConstraint() throws Exception {
        GraphDatabaseService database = createDatabase();

        int numberOfRels = 10;
        String property = "prop";
        RelationshipType type = RelationshipType.withName("myType");

        try (Transaction tx = database.beginTx()) {
            tx.schema().constraintFor(type).assertPropertyIsUnique(property).create();
            tx.commit();
        }

        for (int i = 0; i < numberOfRels; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode();
                Relationship rel = node.createRelationshipTo(node, type);
                rel.setProperty(property, i);
                tx.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        try {
            // let's verify that the constraint has recovered with all values
            // by trying to create duplicates of all relationships under the constraint
            for (int i = 0; i < numberOfRels; i++) {
                int finalInt = i;
                assertThrows(ConstraintViolationException.class, () -> {
                    try (Transaction tx = recoveredDatabase.beginTx()) {
                        Node node = tx.createNode();
                        Relationship rel = node.createRelationshipTo(node, type);
                        rel.setProperty(property, finalInt);
                        tx.commit();
                    }
                });
            }
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodeIndexes() throws Throwable {
        GraphDatabaseService database = createDatabase();
        int numberOfNodes = 10;
        Label label = Label.label("myLabel");
        String property = "prop";
        String rangeIndex = "range index";
        String textIndex = "text index";
        String fullTextIndex = "full text index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property)
                    .withIndexType(IndexType.TEXT)
                    .withName(textIndex)
                    .create();
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(fullTextIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        for (int i = 0; i < numberOfNodes; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(label);

                node.setProperty(property, "value" + i);
                tx.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline(recoveredDatabase);
        try (InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx()) {
            verifyNodeIndexEntries(numberOfNodes, rangeIndex, transaction, allEntries());
            verifyNodeIndexEntries(numberOfNodes, textIndex, transaction, allEntries());
            verifyNodeIndexEntries(numberOfNodes, fullTextIndex, transaction, fulltextSearch("*"));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodePointIndex() throws Throwable {
        GraphDatabaseService database = createDatabase();
        int numberOfNodes = 10;
        Label label = Label.label("myLabel");
        String property = "prop";
        String pointIndex = "point index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property)
                    .withIndexType(IndexType.POINT)
                    .withName(pointIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        for (int i = 0; i < numberOfNodes; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(label);

                node.setProperty(property, Values.pointValue(CoordinateReferenceSystem.CARTESIAN, i, -i));
                tx.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline(recoveredDatabase);
        try (InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx()) {
            verifyNodeIndexEntries(numberOfNodes, pointIndex, transaction, allEntries());
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithRelationshipIndexes() throws Throwable {
        GraphDatabaseService database = createDatabase();
        int numberOfRelationships = 10;
        RelationshipType type = RelationshipType.withName("TYPE");
        String property = "prop";
        String rangeIndex = "range index";
        String textIndex = "text index";
        String fullTextIndex = "full text index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(type)
                    .on(property)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction
                    .schema()
                    .indexFor(type)
                    .on(property)
                    .withIndexType(IndexType.TEXT)
                    .withName(textIndex)
                    .create();
            transaction
                    .schema()
                    .indexFor(type)
                    .on(property)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(fullTextIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        try (Transaction transaction = database.beginTx()) {
            Node start = transaction.createNode();
            Node stop = transaction.createNode();
            for (int i = 0; i < numberOfRelationships; i++) {
                Relationship relationship = start.createRelationshipTo(stop, type);
                relationship.setProperty(property, "value" + i);
            }
            transaction.commit();
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline(recoveredDatabase);
        try (InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx()) {
            verifyRelationshipIndexEntries(numberOfRelationships, rangeIndex, transaction, allEntries());
            verifyRelationshipIndexEntries(numberOfRelationships, textIndex, transaction, allEntries());
            verifyRelationshipIndexEntries(numberOfRelationships, fullTextIndex, transaction, fulltextSearch("*"));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithRelationshipPointIndex() throws Throwable {
        GraphDatabaseService database = createDatabase();
        int numberOfRelationships = 10;
        RelationshipType type = RelationshipType.withName("TYPE");
        String property = "prop";
        String pointIndex = "point index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(type)
                    .on(property)
                    .withIndexType(IndexType.POINT)
                    .withName(pointIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        try (Transaction transaction = database.beginTx()) {
            Node start = transaction.createNode();
            Node stop = transaction.createNode();
            for (int i = 0; i < numberOfRelationships; i++) {
                Relationship relationship = start.createRelationshipTo(stop, type);
                relationship.setProperty(property, Values.pointValue(CoordinateReferenceSystem.CARTESIAN, i, -i));
            }
            transaction.commit();
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline(recoveredDatabase);
        try (InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx()) {
            verifyRelationshipIndexEntries(numberOfRelationships, pointIndex, transaction, allEntries());
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoverRedefinedIndex() throws Exception {
        // The situation in TX logs we are trying create:
        // +[idx=3; 1 slot] ... [checkpoint] ... -[idx=3; 1 slot] ... +[idx=3; 2 slots]
        // The situation above means that idx=3 has 1 slot according to schema store, but 2 on disk after crash.
        // The recovery procedure used to blow up when this happened

        GraphDatabaseService database = createDatabase();
        Label label = Label.label("myLabel");
        String property1 = "prop1";
        String property2 = "prop2";
        String rangeIndex = "range index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property1)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        try (Transaction tx = database.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(property1, "value1");
            tx.commit();
        }

        // This is important to create the situation described at the beginning of the test.
        // If we don't do this, the schema store will be empty after reverse recovery,
        // which is an absolutely different situation.
        forceCheckpoint(database);

        long droppedIndexId;
        try (Transaction tx = database.beginTx()) {
            var indexDefinition = tx.schema().getIndexByName(rangeIndex);
            droppedIndexId =
                    ((IndexDefinitionImpl) indexDefinition).getIndexReference().getId();
            indexDefinition.drop();

            tx.commit();
        }

        try (Transaction tx = database.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(property1, "value2");
            node.setProperty(property2, "another value");
            tx.commit();
        }

        // This is needed in order for the new index to have the same ID as the deleted one.
        performIdMaintenance(database);

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property1)
                    .on(property2)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction.commit();
        }

        try (Transaction tx = database.beginTx()) {
            var indexDefinition = tx.schema().getIndexByName(rangeIndex);
            // If the IDs don't match it is a different scenario than we intended to test.
            assertEquals(
                    droppedIndexId,
                    ((IndexDefinitionImpl) indexDefinition).getIndexReference().getId());
        }
        awaitIndexesOnline(database);

        try (Transaction tx = database.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(property1, "value3");
            node.setProperty(property2, "another value");
            tx.commit();
        }

        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline(recoveredDatabase);
        try (InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx()) {
            verifyNodeIndexEntries(2, rangeIndex, transaction, allEntries());
            var props = transaction.getAllNodes().stream()
                    .map(n -> n.getProperty(property1))
                    .collect(Collectors.toList());
            assertThat(props).containsExactly("value1", "value2", "value3");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void recoveryStopsExtensionsBeforeCheckpoint() throws Exception {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        var checkpointsBeforeRecovery = countCheckPointsInTransactionLogs();

        var extension = new TestRecoveryExtension(checkpointsBeforeRecovery);
        recoverDatabase(List.of(extension));

        assertThat(extension.stopped).isTrue();
    }

    @Test
    void recoveryDoesNotSucceedOnPanic() throws Exception {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        var extension = new TestPanicRecoveryExtension();
        assertThatThrownBy(() -> recoverDatabase(List.of(extension)))
                .hasRootCauseMessage(TestPanicRecoveryExtension.PANIC_MSG);

        assertThat(extension.panicked).isTrue();
    }

    @RecoveryExtension
    private class TestRecoveryExtension extends ExtensionFactory<TestRecoveryExtension.Dependencies> {
        private final int expectedCheckpointsOnStop;
        boolean stopped = false;

        interface Dependencies {}

        TestRecoveryExtension(int expectedCheckpointsOnStop) {
            super(ExtensionType.DATABASE, "testRecoveryExtension");
            this.expectedCheckpointsOnStop = expectedCheckpointsOnStop;
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
            return LifecycleAdapter.onStop(() -> {
                stopped = true;
                assertThat(countCheckPointsInTransactionLogs()).isEqualTo(expectedCheckpointsOnStop);
            });
        }
    }

    @RecoveryExtension
    private static class TestPanicRecoveryExtension extends ExtensionFactory<TestPanicRecoveryExtension.Dependencies> {
        private static final String PANIC_MSG = "AAAAAAAAAAAAH!";
        boolean panicked = false;

        interface Dependencies {
            DatabaseHealth health();
        }

        TestPanicRecoveryExtension() {
            super(ExtensionType.DATABASE, "testPanicRecoveryExtension");
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
            return LifecycleAdapter.onStart(() -> {
                panicked = true;
                dependencies.health().panic(new RuntimeException(PANIC_MSG));
            });
        }
    }

    private void performIdMaintenance(GraphDatabaseService database) {
        ((GraphDatabaseAPI) database)
                .getDependencyResolver()
                .resolveDependency(IdController.class)
                .maintenance();
    }

    private void forceCheckpoint(GraphDatabaseService database) throws IOException {
        ((GraphDatabaseAPI) database)
                .getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("test checkpoint"));
    }

    private void verifyRelationshipIndexEntries(
            int numberOfRelationships, String indexName, InternalTransaction transaction, PropertyIndexQuery query)
            throws KernelException {
        KernelTransaction ktx = transaction.kernelTransaction();
        IndexDescriptor index = ktx.schemaRead().indexGetForName(indexName);
        IndexReadSession indexReadSession = ktx.dataRead().indexReadSession(index);
        int relationshipsInIndex = 0;
        try (RelationshipValueIndexCursor cursor =
                ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
            ktx.dataRead().relationshipIndexSeek(ktx.queryContext(), indexReadSession, cursor, unconstrained(), query);
            while (cursor.next()) {
                relationshipsInIndex++;
            }
        }
        assertEquals(numberOfRelationships, relationshipsInIndex);
    }

    private void verifyNodeIndexEntries(
            int numberOfNodes, String indexName, InternalTransaction transaction, PropertyIndexQuery query)
            throws KernelException {
        KernelTransaction ktx = transaction.kernelTransaction();
        IndexDescriptor index = ktx.schemaRead().indexGetForName(indexName);
        IndexReadSession indexReadSession = ktx.dataRead().indexReadSession(index);
        int nodesInIndex = 0;
        try (NodeValueIndexCursor cursor =
                ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
            ktx.dataRead().nodeIndexSeek(ktx.queryContext(), indexReadSession, cursor, unconstrained(), query);
            while (cursor.next()) {
                nodesInIndex++;
            }
        }
        assertEquals(numberOfNodes, nodesInIndex);
    }

    @Test
    void recoverDatabaseWithFirstTransactionLogFileWithoutShutdownCheckpoint() throws Throwable {
        GraphDatabaseService database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();
        assertEquals(2, countCheckPointsInTransactionLogs());
        // shutdown and init checkpoints
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        assertEquals(0, countCheckPointsInTransactionLogs());
        assertTrue(isRecoveryRequired(databaseLayout));

        startStopDatabase();

        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 2 checkpoints: first will be created after successful recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs());
    }

    @Test
    void failToStartDatabaseWithRemovedTransactionLogs() throws Throwable {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();

        removeTransactionLogs();

        GraphDatabaseAPI restartedDb = createDatabase();
        try {
            DatabaseStateService<?> dbStateService =
                    restartedDb.getDependencyResolver().resolveDependency(DatabaseStateService.class);

            var failure = dbStateService.causeOfFailure(restartedDb.databaseId());
            assertTrue(failure.isPresent());
            assertThat(failure.get())
                    .rootCause()
                    .hasMessageContaining("Transaction logs are missing and recovery is not possible.");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void startDatabaseWithRemovedSingleTransactionLogFile() throws Throwable {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData(database);
        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();
        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs());

        verifyRecoveryMissingLogs();
    }

    @Test
    void startDatabaseWithRemovedMultipleTransactionLogFiles() throws Throwable {
        GraphDatabaseService database = createDatabase(ByteUnit.mebiBytes(1));
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database);
        }
        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();

        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs());
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemoval() throws Throwable {
        GraphDatabaseService database = createDatabase(ByteUnit.mebiBytes(1));
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database);
        }
        managementService.shutdown();

        removeTransactionLogs();
        assertTrue(isRecoveryRequired(databaseLayout));
        assertEquals(0, countTransactionLogFiles());

        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        GraphDatabaseService service = forcedRecoveryManagementService.database(DEFAULT_DATABASE_NAME);
        createSingleNode(service);
        forcedRecoveryManagementService.shutdown();

        assertEquals(2, countTransactionLogFiles());
        assertEquals(2, countCheckPointsInTransactionLogs());
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        startStopDatabase();

        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 3 checkpoints: one from logs before recovery, second will be created as part of recovery and
        // another on shutdown
        assertEquals(3, countCheckPointsInTransactionLogs());
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemovalWithSeveralFilesWithoutCheckpoint() throws Throwable {
        GraphDatabaseService database = createDatabase(ByteUnit.mebiBytes(1));
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database);
        }
        managementService.shutdown();

        removeFileWithCheckpoint();

        assertEquals(4, countTransactionLogFiles());
        assertEquals(0, countCheckPointsInTransactionLogs());
        assertTrue(isRecoveryRequired(databaseLayout));

        startStopDatabase();
        assertEquals(2, countCheckPointsInTransactionLogs());
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        startStopDatabase();

        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs());
    }

    @Test
    void startDatabaseAfterTransactionLogsRemovalAndKillAfterRecovery() throws Throwable {
        long logThreshold = ByteUnit.mebiBytes(1);
        GraphDatabaseService database = createDatabase(logThreshold);
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database);
        }
        managementService.shutdown();

        removeFileWithCheckpoint();

        assertEquals(4, countTransactionLogFiles());
        assertEquals(0, countCheckPointsInTransactionLogs());
        assertTrue(isRecoveryRequired(databaseLayout));

        startStopDatabase();
        assertEquals(2, countCheckPointsInTransactionLogs());
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        startStopDatabase();

        assertFalse(isRecoveryRequired(databaseLayout));
        // we will have 2 checkpoints here because offset in both of them will be the same
        // and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals(2, countCheckPointsInTransactionLogs());
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        builder = null; // Reset log rotation threshold setting to avoid immediate rotation on `createSingleNode()`.

        GraphDatabaseService service = createDatabase(logThreshold * 2); // Bigger log, to avoid rotation.
        createSingleNode(service);
        this.managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        startStopDatabase();

        assertFalse(isRecoveryRequired(databaseLayout));
        assertEquals(3, countCheckPointsInTransactionLogs());
    }

    @Test
    void recoverDatabaseWithoutOneIdFile() throws Throwable {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        Path idFile = getIdFile(db);
        managementService.shutdown();

        fileSystem.deleteFileOrThrow(idFile);
        assertTrue(isRecoveryRequired(layout));

        performRecovery(Recovery.contextWithNoLogTail(
                fileSystem,
                pageCache,
                EMPTY,
                defaults(),
                layout,
                INSTANCE,
                IOController.DISABLED,
                logProvider,
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER));
        assertFalse(isRecoveryRequired(layout));

        assertTrue(fileSystem.fileExists(idFile));
    }

    @Test
    void shouldPruneLogs() throws Throwable {
        GraphDatabaseAPI db = createDatabase(ByteUnit.kibiBytes(128));
        DatabaseLayout layout = db.databaseLayout();
        for (int i = 0; i < 10; i++) {
            generateSomeData(db);
        }
        Path idFile = getIdFile(db);
        managementService.shutdown();

        assertThat(Arrays.stream(fileSystem.listFiles(layout.getTransactionLogsDirectory()))
                        .filter(path -> path.toString().contains("transaction.db"))
                        .count())
                .isGreaterThan(2);

        fileSystem.deleteFileOrThrow(idFile);
        assertTrue(isRecoveryRequired(layout));

        Config config = defaults(Map.of(GraphDatabaseSettings.keep_logical_logs, "keep_none"));
        performRecovery(Recovery.contextWithNoLogTail(
                fileSystem,
                pageCache,
                EMPTY,
                config,
                layout,
                INSTANCE,
                IOController.DISABLED,
                logProvider,
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER));
        assertThat(Arrays.stream(fileSystem.listFiles(layout.getTransactionLogsDirectory()))
                        .filter(path -> path.toString().contains("transaction.db"))
                        .count())
                .isEqualTo(1);
    }

    @Test
    void recoverDatabaseWithoutIdFiles() throws Throwable {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        var idFiles = getIdFiles(db);
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        for (Path idFile : idFiles) {
            fileSystem.deleteFileOrThrow(idFile);
        }
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase();
        assertFalse(isRecoveryRequired(layout));

        for (Path idFile : idFiles) {
            assertTrue(fileSystem.fileExists(idFile));
        }
    }

    @Test
    void failRecoveryWithMissingStoreFile() throws Exception {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData(database);
        Path storeFile = getStoreFile(database, fileSystem);
        managementService.shutdown();
        fileSystem.deleteFileOrThrow(storeFile);

        GraphDatabaseAPI restartedDb = createDatabase();
        try {
            DatabaseStateService<?> dbStateService =
                    restartedDb.getDependencyResolver().resolveDependency(DatabaseStateService.class);

            var failure = dbStateService.causeOfFailure(restartedDb.databaseId());
            assertTrue(failure.isPresent());
            assertThat(failure.get().getCause())
                    .hasMessageContainingAll(
                            storeFile.getFileName().toString(), "is missing and recovery is not possible");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void failRecoveryWithMissingStoreFileAndIdFile() throws Exception {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData(database);
        Path storeFile = getStoreFile(database, fileSystem);
        Path idFile = getIdFile(database);
        managementService.shutdown();

        // Recovery should not be attempted on any store with missing store files, even if other recoverable files are
        // missing as well.
        fileSystem.deleteFileOrThrow(storeFile);
        fileSystem.deleteFileOrThrow(idFile);

        GraphDatabaseAPI restartedDb = createDatabase();

        try {
            DatabaseStateService<?> dbStateService =
                    restartedDb.getDependencyResolver().resolveDependency(DatabaseStateService.class);

            var failure = dbStateService.causeOfFailure(restartedDb.databaseId());
            assertTrue(failure.isPresent());
            assertThat(failure.get().getCause())
                    .hasMessageContainingAll(
                            storeFile.getFileName().toString(), "is missing and recovery is not possible");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void cancelRecoveryInTheMiddle() throws Throwable {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        assertTrue(isRecoveryRequired(layout));

        Monitors monitors = new Monitors();
        var guardExtensionFactory = new GlobalGuardConsumerTestExtensionFactory();
        var recoveryMonitor = new RecoveryMonitor() {
            private final AtomicBoolean reverseCompleted = new AtomicBoolean();
            private final AtomicBoolean recoveryCompleted = new AtomicBoolean();

            @Override
            public void reverseStoreRecoveryCompleted(long lowestRecoveredAppendIndex) {
                try {
                    guardExtensionFactory.getProvidedGuardConsumer().globalGuard.stop();
                } catch (Exception e) {
                    // do nothing
                }
                reverseCompleted.set(true);
            }

            @Override
            public void transactionLogRecoveryCompleted(long recoveryTimeInMilliseconds, RecoveryMode mode) {
                recoveryCompleted.set(true);
            }

            public boolean isReverseCompleted() {
                return reverseCompleted.get();
            }

            public boolean isRecoveryCompleted() {
                return recoveryCompleted.get();
            }
        };
        monitors.addMonitorListener(recoveryMonitor);
        try (var service = new TestDatabaseManagementServiceBuilder(layout.getNeo4jLayout())
                .addExtension(guardExtensionFactory)
                .setMonitors(monitors)
                .build()) {
            var database = service.database(layout.getDatabaseName());
            assertTrue(recoveryMonitor.isReverseCompleted());
            assertFalse(recoveryMonitor.isRecoveryCompleted());
            assertFalse(
                    guardExtensionFactory.getProvidedGuardConsumer().globalGuard.isAvailable());
            assertFalse(database.isAvailable());
            assertThatThrownBy(database::beginTx).rootCause().isInstanceOf(DatabaseStartAbortedException.class);
        }
    }

    @Test
    void shouldForceRecoveryEvenThoughNotSeeminglyRequired() throws Exception {
        // given
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        Path idFile = getIdFile(db);
        managementService.shutdown();
        assertFalse(isRecoveryRequired(layout));
        var openOptions = db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .getOpenOptions();
        // Make an ID generator, say for the node store, dirty
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fileSystem, immediate(), PageCacheTracer.NULL, "my db");
        try (IdGenerator idGenerator = idGeneratorFactory.open(
                pageCache,
                new StoreFile(idFile),
                TEST_NODE_TYPE,
                () -> 0L /*will not be used*/,
                10_000,
                false,
                Config.defaults(),
                CONTEXT_FACTORY,
                openOptions,
                IdSlotDistribution.SINGLE_IDS)) {
            // Merely opening a marker will make the backing GBPTree dirty
            idGenerator.transactionalMarker(NULL_CONTEXT).close();
        }
        assertFalse(isRecoveryRequired(layout));
        assertTrue(idGeneratorIsDirty(idFile, openOptions));

        // when
        MutableBoolean recoveryRunEvenThoughNoCommitsAfterLastCheckpoint = new MutableBoolean();
        RecoveryStartInformationProvider.Monitor monitor = new RecoveryStartInformationProvider.Monitor() {
            @Override
            public void recoveryNotRequired(LogPosition logPosition) {
                recoveryRunEvenThoughNoCommitsAfterLastCheckpoint.setTrue();
            }
        };
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(monitor);
        Config config = Config.defaults();

        Recovery.performRecovery(Recovery.contextWithNoLogTail(
                        fileSystem,
                        pageCache,
                        EMPTY,
                        config,
                        layout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .recoveryPredicate(RecoveryPredicate.ALL)
                .monitors(monitors)
                .extensionFactories(Iterables.cast(Services.loadAll(ExtensionFactory.class)))
                .startupChecker(null)
                .clock(Clock.systemUTC())
                .force());

        // then
        assertFalse(idGeneratorIsDirty(idFile, openOptions));
        assertTrue(recoveryRunEvenThoughNoCommitsAfterLastCheckpoint.booleanValue());
    }

    @SuppressWarnings("resource")
    @Test
    void resetCheckpointVersionOnMissingLogFiles() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        DependencyResolver resolver = db.getDependencyResolver();
        LogFiles logFiles = resolver.resolveDependency(LogFiles.class);
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        for (int i = 0; i < 10; i++) {
            checkpointFile.rotate();
        }
        assertEquals(10, resolver.resolveDependency(LogMetadataProvider.class).getCheckpointLogVersion());
        managementService.shutdown();

        removeTransactionLogs();

        assertTrue(isRecoveryRequired(layout));

        recoverDatabase();
        assertFalse(isRecoveryRequired(layout));

        assertEquals(
                0,
                createDatabase()
                        .getDependencyResolver()
                        .resolveDependency(LogMetadataProvider.class)
                        .getCheckpointLogVersion());
    }

    @SuppressWarnings("resource")
    @Test
    void recoverySetsCheckpointLogVersionSeveralCheckpointFiles() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);

        var checkpointFile =
                db.getDependencyResolver().resolveDependency(LogFiles.class).getCheckpointFile();
        var appender = (DetachedCheckpointAppender) checkpointFile.getCheckpointAppender();
        var transactionId = new TransactionId(100, 101, LATEST_KERNEL_VERSION, 101, 102, 103);
        appender.rotate();
        appender.checkPoint(
                LogCheckPointEvent.NULL,
                transactionId,
                transactionId.id() + 1,
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                Instant.now(),
                "test1");
        appender.rotate();
        appender.checkPoint(
                LogCheckPointEvent.NULL,
                transactionId,
                transactionId.id() + 1,
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                Instant.now(),
                "test2");
        appender.rotate();
        appender.checkPoint(
                LogCheckPointEvent.NULL,
                transactionId,
                transactionId.id() + 1,
                LatestVersions.LATEST_KERNEL_VERSION,
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()),
                Instant.now(),
                "test3");

        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeFileWithCheckpoint();

        assertTrue(isRecoveryRequired(layout));

        recoverDatabase();
        assertFalse(isRecoveryRequired(layout));

        assertEquals(
                2,
                createDatabase()
                        .getDependencyResolver()
                        .resolveDependency(LogMetadataProvider.class)
                        .getCheckpointLogVersion());
    }

    @Test
    void recoverDatabaseWithAllTransactionsPredicate() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        long expectedLastTransactionId = getMetadataProvider(db).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(ALL);

        db = createDatabase();
        assertEquals(expectedLastTransactionId, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void recoverDatabaseWithIdPredicateHigherToLastAvailable() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        long expectedLastTransactionId = getMetadataProvider(db).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(RecoveryCriteria.until(expectedLastTransactionId + 5));

        db = createDatabase();
        assertEquals(expectedLastTransactionId, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void recoverDatabaseWithIdPredicateLowerToLastAvailable() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        long originalLastCommitted = getMetadataProvider(db).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        long lastTransactionToBeApplied = originalLastCommitted - 5;
        recoverDatabase(RecoveryCriteria.until(lastTransactionToBeApplied));

        db = createDatabase();
        assertEquals(lastTransactionToBeApplied - 1, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void recoverDatabaseWithDatePredicateHigherToLastAvailable() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        var metaDataStore = getMetadataProvider(db);
        long expectedLastCommitTimestamp =
                metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastTransactionId = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(RecoveryCriteria.until(Instant.ofEpochMilli(expectedLastCommitTimestamp + 1)));

        db = createDatabase();
        assertEquals(expectedLastTransactionId, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void recoverDatabaseWithDatePredicateLowerToLastAvailable() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();

        var metaDataStore = getMetadataProvider(db);
        long expectedLastCommitTimestamp =
                metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastCommitted = metaDataStore.getLastCommittedTransactionId();

        fakeClock.forward(4, MINUTES);

        generateSomeData(db);
        long originalLastCommitted = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(RecoveryCriteria.until(Instant.ofEpochMilli(expectedLastCommitTimestamp + 1)));

        db = createDatabase();
        long postRecoveryLastCommittedTxId = getMetadataProvider(db).getLastCommittedTransactionId();
        assertEquals(expectedLastCommitted, postRecoveryLastCommittedTxId);
        assertNotEquals(originalLastCommitted, postRecoveryLastCommittedTxId);
    }

    @Test
    void recoverDatabaseWithIdPredicateWithNothingAfterLastCheckpoint() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData(db);
        long originalLastCommitted = getMetadataProvider(db).getLastCommittedTransactionId();
        db.getDependencyResolver()
                .resolveDependency(CheckPointerImpl.class)
                .forceCheckPoint(new SimpleTriggerInfo("test"));
        generateSomeData(db);
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(RecoveryCriteria.until(originalLastCommitted + 1));

        db = createDatabase();
        assertEquals(originalLastCommitted, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void earlyRecoveryTerminationOnTxIdCriteriaShouldPrintReason() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData(db);
        long originalLastCommitted = getMetadataProvider(db).getLastCommittedTransactionId();
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);
        assertTrue(isRecoveryRequired(layout));

        long restoreUntilTxId = originalLastCommitted - 4;
        recoverDatabase(RecoveryCriteria.until(restoreUntilTxId));

        assertThat(logProvider)
                .containsMessages("Partial database recovery based on provided criteria: transaction id should be < "
                        + restoreUntilTxId + ". " + "Last replayed transaction: transaction id: "
                        + (restoreUntilTxId - 1) + ", time 1970-01-01 00:00:10.000+0000.");
        db = createDatabase();
        assertEquals(restoreUntilTxId - 1, getMetadataProvider(db).getLastCommittedTransactionId());
    }

    @Test
    void earlyRecoveryTerminationOnTxDateCriteriaShouldPrintReason() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();

        var metaDataStore = getMetadataProvider(db);
        long expectedLastCommitTimestamp =
                metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastCommitted = metaDataStore.getLastCommittedTransactionId();

        fakeClock.forward(10, MINUTES);

        generateSomeData(db);
        long originalLastCommitted = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        recoverDatabase(RecoveryCriteria.until(Instant.ofEpochMilli(expectedLastCommitTimestamp + 1)));

        assertThat(logProvider)
                .containsMessages(
                        "Partial database recovery based on provided criteria: transaction date should be before 1970-01-01 00:00:10.001+0000. "
                                + "Last replayed transaction: transaction id: " + expectedLastCommitted
                                + ", time 1970-01-01 00:00:10.000+0000.");

        db = createDatabase();
        long postRecoveryLastCommittedTxId = getMetadataProvider(db).getLastCommittedTransactionId();
        assertEquals(expectedLastCommitted, postRecoveryLastCommittedTxId);
        assertNotEquals(originalLastCommitted, postRecoveryLastCommittedTxId);
    }

    @Test
    void failToReadTransactionOnIncorrectCriteria() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue(isRecoveryRequired(layout));

        assertThatThrownBy(() -> recoverDatabase(RecoveryCriteria.until(2)))
                .hasCauseInstanceOf(RecoveryPredicateException.class)
                .getCause()
                .hasMessageContaining("Partial recovery criteria can't be satisfied. "
                        + "No transaction after checkpoint matching to provided criteria found and fail "
                        + "to read transaction before checkpoint. Recovery criteria: transaction id should be < 2.");

        assertTrue(isRecoveryRequired(layout));
    }

    @Test
    void transactionBeforeCheckpointNotMatchingExpectedCriteria() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData(db);
        DependencyResolver deps = db.getDependencyResolver();
        deps.resolveDependency(CheckPointerImpl.class).forceCheckPoint(new SimpleTriggerInfo("test"));
        long lastTxId = deps.resolveDependency(TransactionIdStore.class).getLastCommittedTransactionId();
        generateSomeData(db);
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        assertTrue(isRecoveryRequired(layout));

        assertThatThrownBy(() -> recoverDatabase(RecoveryCriteria.until(1)))
                .hasCauseInstanceOf(RecoveryPredicateException.class)
                .cause()
                .hasMessageContaining("Partial recovery criteria can't be satisfied. Transaction after and before "
                        + "checkpoint does not satisfy provided recovery criteria. Observed transaction id: " + lastTxId
                        + ", recovery criteria: transaction id should be < 1.");

        assertTrue(isRecoveryRequired(layout));
    }

    @Test
    void useProvidedLogFilesLogTailInfo() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData(db);
        db.getDependencyResolver()
                .resolveDependency(CheckPointerImpl.class)
                .forceCheckPoint(new SimpleTriggerInfo("test"));
        generateSomeData(db);
        managementService.shutdown();

        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        assertTrue(isRecoveryRequired(layout));

        LogTailMetadata spiedLogTail = Mockito.spy(buildLogFiles().getTailMetadata());
        performRecovery(context(
                        fileSystem,
                        pageCache,
                        EMPTY,
                        Config.defaults(),
                        databaseLayout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        spiedLogTail)
                .clock(fakeClock));
        verify(spiedLogTail, times(1)).getLastTransactionLogPosition();

        assertFalse(isRecoveryRequired(layout));
    }

    @Test
    void recoverDatabaseWithLogPosition() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData(db);
        DatabaseLayout layout = db.databaseLayout();
        StorageEngineFactory storageEngineFactory =
                db.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        LogMetadataProvider metadataProvider = getMetadataProvider(db);
        long originalLastCommitted = metadataProvider.getLastCommittedTransactionId();
        LogPosition lastLogPosition = metadataProvider.getLastCommittedBatch().logPositionAfter();
        generateSomeData(db);
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);

        RecoveryCriteria recoveryCriteria = RecoveryCriteria.untilPosition(lastLogPosition);
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        assertTrue(isRecoveryRequired(databaseLayout, config, predicate));

        Recovery.performRecovery(Recovery.contextWithNoLogTail(
                        fileSystem,
                        pageCache,
                        EMPTY,
                        config,
                        databaseLayout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        LATEST_KERNEL_VERSION_PROVIDER)
                .recoveryPredicate(predicate)
                .monitors(monitors)
                .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                .clock(fakeClock)
                .incompleteTransactionAction(STOP));

        // Verify where we recovered to by checking the checkpoint created by recovery
        LogTailMetadata tailMetadata = new LogTailExtractor(fileSystem, config, storageEngineFactory, EMPTY)
                .getTailMetadata(databaseLayout, INSTANCE);
        assertThat(tailMetadata.getLastCheckPoint().get().transactionLogPosition())
                .isEqualTo(lastLogPosition);
        assertThat(tailMetadata.getLastCheckPoint().get().transactionId().id()).isEqualTo(originalLastCommitted);

        assertFalse(isRecoveryRequired(databaseLayout, config, predicate));
    }

    @Test
    void noRecoveryNeededWithLogPositionAtCheckpoint() throws Exception {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();

        generateSomeData(db);
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("My checkpoint"));
        LatestCheckpointInfo latestCheckpointInfo = checkPointer.latestCheckPointInfo();
        generateSomeData(db);
        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);
        assertTrue(isRecoveryRequired(layout));

        RecoveryCriteria recoveryCriteria =
                RecoveryCriteria.untilPosition(latestCheckpointInfo.checkpointedLogPosition());
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        assertFalse(isRecoveryRequired(databaseLayout, config, predicate));

        assertFalse(performRecovery(Recovery.contextWithNoLogTail(
                                fileSystem,
                                pageCache,
                                EMPTY,
                                config,
                                databaseLayout,
                                INSTANCE,
                                IOController.DISABLED,
                                logProvider,
                                LATEST_KERNEL_VERSION_PROVIDER)
                        .recoveryPredicate(predicate)
                        .monitors(monitors)
                        .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                        .clock(fakeClock)
                        .incompleteTransactionAction(STOP))
                .recoveryPerformed());
    }

    @Test
    void shouldHandleRecoveryWithLogPositionIfUnstableTail() throws Exception {
        // Nothing after the maxposition should ever be read, even in logtail reading because after that
        // position the log is not stable and could be truncated during recovery is running.
        // Let's destroy the tail a bit after maxposition and see that everything still works.

        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();

        generateSomeData(db);
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("My checkpoint"));
        LatestCheckpointInfo latestCheckpointInfo = checkPointer.latestCheckPointInfo();

        FlushableLogPositionAwareChannel channel = db.getDependencyResolver()
                .resolveDependency(LogFiles.class)
                .getLogFile()
                .getTransactionLogWriter()
                .getChannel();
        channel.beginChecksumForWriting();
        channel.putVersion(LATEST_KERNEL_VERSION.version());
        channel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        // something that can not be read without error
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.putChecksum();
        channel.prepareForFlush().flush();

        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);

        RecoveryCriteria recoveryCriteria =
                RecoveryCriteria.untilPosition(latestCheckpointInfo.checkpointedLogPosition());
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        assertFalse(isRecoveryRequired(databaseLayout, config, predicate));

        assertFalse(performRecovery(Recovery.contextWithNoLogTail(
                                fileSystem,
                                pageCache,
                                EMPTY,
                                config,
                                databaseLayout,
                                INSTANCE,
                                IOController.DISABLED,
                                logProvider,
                                LATEST_KERNEL_VERSION_PROVIDER)
                        .recoveryPredicate(predicate)
                        .monitors(monitors)
                        .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                        .clock(fakeClock)
                        .incompleteTransactionAction(STOP))
                .recoveryPerformed());
    }

    @Test
    void shouldRecoverWithTailScanningAndLogPositionIgnoringUnstableTail() throws Exception {
        // Nothing after the maxposition should ever be read, even in logtail reading because after that
        // position the log is not stable and could be truncated during recovery is running.
        // We write a dummy tx with a higher appendIndex/txId to catch this,
        // since DetachedLogTailAppendIndexProvider masks exceptions

        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();

        generateSomeData(db);
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("My checkpoint"));
        LatestCheckpointInfo latestCheckpointInfo = checkPointer.latestCheckPointInfo();

        // Append an extra dummy tx after maxPosition which we shouldn't read
        var txWriter = db.getDependencyResolver()
                .resolveDependency(LogFiles.class)
                .getLogFile()
                .getTransactionLogWriter();
        txWriter.getWriter()
                .writeStartEntry(LogEntryFactory.newStartEntry(
                        LATEST_KERNEL_VERSION,
                        Instant.now().toEpochMilli(),
                        latestCheckpointInfo
                                .highestObservedClosedTransactionId()
                                .id(),
                        latestCheckpointInfo.appendIndex() + 1L,
                        UNKNOWN_TX_SEQUENCE_NUMBER,
                        0,
                        new byte[0]));
        txWriter.getWriter()
                .writeCommitEntry(
                        LATEST_KERNEL_VERSION,
                        latestCheckpointInfo
                                        .highestObservedClosedTransactionId()
                                        .id()
                                + 1,
                        Instant.now().toEpochMilli());

        managementService.shutdown();

        // Delete shutdown checkpoint
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);
        // Delete custom checkpoint
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);

        RecoveryCriteria recoveryCriteria =
                RecoveryCriteria.untilPosition(latestCheckpointInfo.checkpointedLogPosition());
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);

        LogFiles logFiles = LogFilesBuilder.readableBuilder(
                        layout, fileSystem, LATEST_KERNEL_VERSION_PROVIDER, LATEST_LOG_FORMAT_PROVIDER)
                .withTailReadingMaxPosition(latestCheckpointInfo.checkpointedLogPosition())
                .build();
        LogTailMetadata tailMetadata = logFiles.getTailMetadata();
        assertThat(tailMetadata.lastBatch().appendIndex()).isEqualTo(latestCheckpointInfo.appendIndex());
        assertThat(tailMetadata.lastBatch().logPositionAfter())
                .isEqualTo(latestCheckpointInfo.checkpointedLogPosition());
        assertTrue(isRecoveryRequired(databaseLayout, config, predicate));

        assertTrue(performRecovery(context(
                                fileSystem,
                                pageCache,
                                EMPTY,
                                config,
                                databaseLayout,
                                INSTANCE,
                                IOController.DISABLED,
                                logProvider,
                                tailMetadata)
                        .recoveryPredicate(predicate)
                        .monitors(monitors)
                        .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                        .clock(fakeClock)
                        .incompleteTransactionAction(STOP))
                .recoveryPerformed());
    }

    @Test
    void shouldHandleRecoveryWithLogPositionIfUnstableTailWithoutCheckpoints() throws Exception {
        // Nothing after the maxposition should ever be read, even in logtail reading because after that
        // position the log is not stable and could be truncated during recovery is running.
        // Let's destroy the tail a bit after maxposition and see that everything still works.

        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();

        generateSomeData(db);

        FlushableLogPositionAwareChannel channel = db.getDependencyResolver()
                .resolveDependency(LogFiles.class)
                .getLogFile()
                .getTransactionLogWriter()
                .getChannel();
        LogPosition position = channel.getCurrentLogPosition();
        channel.beginChecksumForWriting();
        channel.putVersion(LATEST_KERNEL_VERSION.version());
        channel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        // something that can not be read without error
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.put((byte) 99);
        channel.putChecksum();
        channel.prepareForFlush().flush();

        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(layout, fileSystem);

        RecoveryCriteria recoveryCriteria = RecoveryCriteria.untilPosition(position);
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        assertTrue(isRecoveryRequired(databaseLayout, config, predicate));

        assertTrue(performRecovery(Recovery.contextWithNoLogTail(
                                fileSystem,
                                pageCache,
                                EMPTY,
                                config,
                                databaseLayout,
                                INSTANCE,
                                IOController.DISABLED,
                                logProvider,
                                LATEST_KERNEL_VERSION_PROVIDER)
                        .recoveryPredicate(predicate)
                        .monitors(monitors)
                        .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                        .clock(fakeClock)
                        .incompleteTransactionAction(STOP))
                .recoveryPerformed());
    }

    @Test
    void recoveryUntilPositionNotAllowedToPointBeforeLatestCheckpoint() {
        GraphDatabaseAPI db = createDatabase();

        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
        LatestCheckpointInfo latestCheckpointInfo = checkPointer.latestCheckPointInfo();
        LogPosition positionBeforeCheckpoint = latestCheckpointInfo.checkpointedLogPosition();

        generateSomeData(db);
        managementService.shutdown();

        RecoveryCriteria recoveryCriteria = RecoveryCriteria.untilPosition(positionBeforeCheckpoint);
        RecoveryPredicate predicate = recoveryCriteria.toPredicate();

        assertThrows(RuntimeException.class, () -> isRecoveryRequired(databaseLayout, defaults(), predicate));
    }

    private void prepareEmptyZeroedLogFile(Path victimFilePath) throws IOException {
        fileSystem.deleteFileOrThrow(victimFilePath);
        var nativeAccess = NativeAccessProvider.getNativeAccess();
        try (StoreChannel storeChannel = fileSystem.open(victimFilePath, Set.of(CREATE, TRUNCATE_EXISTING, WRITE))) {
            int fileDescriptor = storeChannel.getFileDescriptor();
            nativeAccess.tryPreallocateSpace(fileDescriptor, ByteUnit.mebiBytes(1));
        }
    }

    private void prepareCorruptedLogFile(Path victimFilePath) throws IOException {
        fileSystem.deleteFileOrThrow(victimFilePath);
        byte corruptionSource = (byte) (ThreadLocalRandom.current().nextBoolean() ? 7 : -7);
        try (StoreChannel storeChannel = fileSystem.open(victimFilePath, Set.of(CREATE, TRUNCATE_EXISTING, WRITE))) {

            storeChannel.writeAll(ByteBuffer.wrap(new byte[BIGGEST_HEADER]));
            storeChannel.writeAll(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, corruptionSource, 0}));
        }
    }

    private void removeLastKbFromLogFile(Path victimFilePath, LogPosition logPosition) throws IOException {
        try (StoreChannel storeChannel = fileSystem.open(victimFilePath, Set.of(WRITE))) {
            long newSize = storeChannel.size() - ByteUnit.kibiBytes(1);
            assertThat(newSize).isGreaterThan(logPosition.getByteOffset());
            storeChannel.truncate(newSize);
        }
    }

    private void prepareEmptyLogFile(Path victimFilePath) throws IOException {
        fileSystem.deleteFileOrThrow(victimFilePath);
        try (StoreChannel storeChannel = fileSystem.open(victimFilePath, Set.of(CREATE, TRUNCATE_EXISTING, WRITE))) {}
    }

    private boolean idGeneratorIsDirty(Path path, ImmutableSet<OpenOption> openOptions) throws IOException {
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fileSystem, immediate(), PageCacheTracer.NULL, "my db");
        try (IdGenerator idGenerator = idGeneratorFactory.open(
                pageCache,
                new StoreFile(path),
                TEST_NODE_TYPE,
                () -> 0L /*will not be used*/,
                10_000,
                true,
                Config.defaults(),
                CONTEXT_FACTORY,
                openOptions,
                IdSlotDistribution.SINGLE_IDS)) {
            MutableBoolean dirtyOnStartup = new MutableBoolean();
            InvocationHandler invocationHandler = (proxy, method, args) -> {
                if (method.getName().equals("dirtyOnStartup")) {
                    dirtyOnStartup.setTrue();
                }
                return null;
            };
            ReporterFactory reporterFactory = new ReporterFactory(invocationHandler);
            idGenerator.consistencyCheck(
                    reporterFactory, NULL_CONTEXT_FACTORY, Runtime.getRuntime().availableProcessors());
            return dirtyOnStartup.booleanValue();
        }
    }

    static void awaitIndexesOnline(GraphDatabaseService database) {
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(10, MINUTES);
            transaction.commit();
        }
    }

    private static void createSingleNode(GraphDatabaseService service) {
        try (Transaction transaction = service.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
    }

    private void startStopDatabase() {
        GraphDatabaseService db = createDatabase();
        db.beginTx().close();
        managementService.shutdown();
    }

    void recoverDatabase() throws Exception {
        recoverDatabase(EMPTY, ALL);
    }

    private void recoverDatabase(DatabaseTracers tracers) throws Exception {
        recoverDatabase(tracers, ALL);
    }

    private void recoverDatabase(RecoveryCriteria recoveryCriteria) throws Exception {
        recoverDatabase(EMPTY, recoveryCriteria);
    }

    private void recoverDatabase(Iterable<ExtensionFactory<?>> extensionFactories) throws Exception {
        recoverDatabase(EMPTY, ALL, extensionFactories);
    }

    void additionalConfiguration(Config config) {
        config.set(fail_on_missing_files, false);
    }

    TestDatabaseManagementServiceBuilder additionalConfiguration(TestDatabaseManagementServiceBuilder builder) {
        return builder;
    }

    private void recoverDatabase(DatabaseTracers databaseTracers, RecoveryCriteria recoveryCriteria) throws Exception {
        recoverDatabase(databaseTracers, recoveryCriteria, Iterables.cast(Services.loadAll(ExtensionFactory.class)));
    }

    private void recoverDatabase(
            DatabaseTracers databaseTracers,
            RecoveryCriteria recoveryCriteria,
            Iterable<ExtensionFactory<?>> extensionFactories)
            throws Exception {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        assertTrue(isRecoveryRequired(databaseLayout, config, databaseTracers));

        Recovery.performRecovery(Recovery.contextWithNoLogTail(
                        fileSystem,
                        pageCache,
                        databaseTracers,
                        config,
                        databaseLayout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        LATEST_KERNEL_VERSION_PROVIDER)
                .recoveryPredicate(recoveryCriteria.toPredicate())
                .monitors(monitors)
                .extensionFactories(extensionFactories)
                .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER)
                .clock(fakeClock));
        assertFalse(isRecoveryRequired(databaseLayout, config));
    }

    private boolean isRecoveryRequired(DatabaseLayout layout) throws Exception {
        Config config = Config.newBuilder().build();
        additionalConfiguration(config);
        return isRecoveryRequired(layout, config);
    }

    private boolean isRecoveryRequired(DatabaseLayout layout, Config config) throws Exception {
        return Recovery.isRecoveryRequired(fileSystem, pageCache, layout, config, Optional.empty(), INSTANCE, EMPTY);
    }

    private boolean isRecoveryRequired(DatabaseLayout layout, Config config, DatabaseTracers tracers) throws Exception {
        return Recovery.isRecoveryRequired(fileSystem, pageCache, layout, config, Optional.empty(), INSTANCE, tracers);
    }

    private boolean isRecoveryRequired(DatabaseLayout layout, Config config, RecoveryPredicate recoveryPredicate)
            throws Exception {
        return Recovery.isRecoveryRequired(
                fileSystem,
                pageCache,
                layout,
                StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout, config),
                config,
                Optional.empty(),
                INSTANCE,
                DatabaseTracers.EMPTY,
                recoveryPredicate);
    }

    private int countCheckPointsInTransactionLogs() throws IOException {
        LogFiles logFiles = buildLogFiles();
        var checkpoints = logFiles.getCheckpointFile().reachableCheckpoints();
        return checkpoints.size();
    }

    private LogFiles buildLogFiles() throws IOException {
        return LogFilesBuilder.readableBuilder(
                        databaseLayout,
                        fileSystem,
                        LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withCommandReaderFactory(StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout, null)
                        .commandReaderFactory())
                .withDatabaseTracers(EMPTY)
                .build();
    }

    private void removeTransactionLogs() throws IOException {
        LogFiles logFiles = buildLogFiles();
        for (Path logFile : fileSystem.listFiles(logFiles.logFilesDirectory())) {
            fileSystem.deleteFile(logFile);
        }
    }

    private void removeFileWithCheckpoint() throws IOException {
        LogFiles logFiles = buildLogFiles();
        fileSystem.deleteFileOrThrow(logFiles.getCheckpointFile().getCurrentFile());
    }

    private int countTransactionLogFiles() throws IOException {
        LogFiles logFiles = buildLogFiles();
        return logFiles.logFiles().length;
    }

    private int countCheckpointFiles() throws IOException {
        // Don't build active files here since that will trigger logTail reading and
        // potentially clean up empty checkpoint files
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(
                        databaseLayout.getTransactionLogsDirectory(), fileSystem)
                .build();
        return logFiles.getCheckpointFile().getMatchedFiles().length;
    }

    private static void generateSomeData(GraphDatabaseService database) {
        for (int i = 0; i < 10; i++) {
            try (Transaction transaction = database.beginTx()) {
                Node node1 = transaction.createNode();
                Node node2 = transaction.createNode();
                node1.createRelationshipTo(node2, withName("Type" + i));
                node2.setProperty("a", secure().nextAlphanumeric(TEN_KB));
                transaction.commit();
            }
        }
    }

    GraphDatabaseAPI createDatabase() {
        return createDatabase(logical_log_rotation_threshold.defaultValue());
    }

    protected GraphDatabaseAPI createDatabase(long logThreshold) {
        createBuilder(logThreshold);
        managementService = builder.build();
        return (GraphDatabaseAPI) managementService.database(databaseLayout.getDatabaseName());
    }

    private void createBuilder(long logThreshold) {
        if (builder == null) {
            logProvider = new AssertableLogProvider();
            fakeClock = Clocks.fakeClock(10, SECONDS);
            builder = new TestDatabaseManagementServiceBuilder(neo4jLayout)
                    .setConfig(preallocate_logical_logs, false)
                    .setClock(fakeClock)
                    .setInternalLogProvider(logProvider)
                    .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                    .setConfig(logical_log_rotation_threshold, logThreshold);
            builder = additionalConfiguration(builder);
        }
    }

    private void startStopDatabaseWithForcedRecovery() {
        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        forcedRecoveryManagementService.shutdown();
    }

    private DatabaseManagementService forcedRecoveryManagement() {
        TestDatabaseManagementServiceBuilder serviceBuilder =
                new TestDatabaseManagementServiceBuilder(neo4jLayout).setConfig(fail_on_missing_files, false);
        return additionalConfiguration(serviceBuilder).build();
    }

    private DatabaseManagementService dbmsWithFailOnCorruptedFalse() {
        TestDatabaseManagementServiceBuilder serviceBuilder = new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(fail_on_missing_files, false)
                .setConfig(GraphDatabaseInternalSettings.fail_on_corrupted_log_files, false);
        return additionalConfiguration(serviceBuilder).build();
    }

    private static LogMetadataProvider getMetadataProvider(GraphDatabaseAPI db) {
        return db.getDependencyResolver().resolveDependency(LogMetadataProvider.class);
    }

    private void verifyRecoveryMissingLogs() throws IOException {
        GraphDatabaseAPI restartedDatabase = createDatabase();
        try {
            LogFiles logFiles = restartedDatabase.getDependencyResolver().resolveDependency(LogFiles.class);
            CheckpointInfo checkpointInfo =
                    logFiles.getCheckpointFile().reachableCheckpoints().get(0);
            assertThat(checkpointInfo.reason()).contains("missing logs");
        } finally {
            managementService.shutdown();
        }
    }

    private void assumeRecordStorageEngine(GraphDatabaseAPI database) {
        final var storageEngine = database.getDependencyResolver().resolveDependency(StorageEngine.class);
        assumeThat(storageEngine instanceof RecordStorageEngine)
                .as("Requires the use of testAccessNeoStores, which is record format specific")
                .isTrue();
    }

    private void appendBytesToLastLogFile(Path logFilePath, LogPosition logPosition, byte[] bytesToWrite)
            throws IOException {
        try (StoreChannel storeChannel = fileSystem.write(logFilePath)) {
            storeChannel.position(logPosition.getByteOffset());
            storeChannel.writeAll(ByteBuffer.wrap(bytesToWrite));
        }
    }

    private static Path getIdFile(GraphDatabaseAPI db) {
        var idFiles = getIdFiles(db);
        return getFirstSortedOnName(idFiles);
    }

    private static Collection<Path> getIdFiles(GraphDatabaseAPI db) {
        return db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .listStorageFiles(new StorageFileSelection(false, false, true));
    }

    private static Path getStoreFile(GraphDatabaseAPI db, FileSystemAbstraction fs) {
        Set<Path> files = new HashSet<>(db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .listStorageFiles(new StorageFileSelection(true, true, false)));
        var layout = db.databaseLayout();
        files.removeAll(layout.pathForStore(CommonDatabaseStores.METADATA).allSegments(fs));
        files.removeAll(
                layout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS).allSegments(fs));
        return getFirstSortedOnName(files);
    }

    private static Path getFirstSortedOnName(Collection<Path> path) {
        return path.stream()
                .max(Comparator.comparing(p -> p.getFileName().toString())) // To be deterministic
                .orElseThrow();
    }

    interface Dependencies {
        CompositeDatabaseAvailabilityGuard globalGuard();
    }

    private static class GlobalGuardConsumerTestExtensionFactory extends ExtensionFactory<Dependencies> {
        private GlobalGuardConsumer providedConsumer;

        GlobalGuardConsumerTestExtensionFactory() {
            super("globalGuardConsumer");
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
            providedConsumer = new GlobalGuardConsumer(dependencies);
            return providedConsumer;
        }

        public GlobalGuardConsumer getProvidedGuardConsumer() {
            return providedConsumer;
        }
    }

    private static class GlobalGuardConsumer extends LifecycleAdapter {
        private final CompositeDatabaseAvailabilityGuard globalGuard;

        GlobalGuardConsumer(Dependencies dependencies) {
            globalGuard = dependencies.globalGuard();
        }
    }

    private static class CheckpointTracer extends DefaultDatabaseTracer {

        private final AtomicInteger openCounter = new AtomicInteger();

        private CheckpointTracer() {
            super(PageCacheTracer.NULL);
        }

        @Override
        public void openLogFile(Path filePath) {
            if (filePath.getFileName().toString().contains("checkpoint")) {
                openCounter.incrementAndGet();
            }
            super.openLogFile(filePath);
        }

        public int getCheckpointOpenCounter() {
            return openCounter.get();
        }
    }
}
