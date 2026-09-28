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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
import static org.neo4j.kernel.TxLogValidationUtils.assertLogHeaderExpectedVersion;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsIn;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsWithCorrectVersionInSpecificLogVersion;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.createWriteTransaction;
import static org.neo4j.test.UpgradeTestUtil.upgradeDatabase;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;
import static org.neo4j.wal.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionFailureHelper;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.WALService;

@Neo4jLayoutExtension
class TransactionLogsUpgradeIT {
    @Inject
    protected DefaultFileSystemAbstraction fileSystem;

    @Inject
    private Neo4jLayout neo4jLayout;

    protected DatabaseManagementService managementService;
    protected GraphDatabaseAPI testDb;
    protected CommandReaderFactory commandReaderFactory;
    private static final KernelVersion EXPECTED_HEADER_VERSION_LATEST_FORMAT =
            LATEST_KERNEL_VERSION.isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED)
                    ? LATEST_KERNEL_VERSION
                    : null; /* pre-envelope format doesn't include the version */

    protected TestDatabaseManagementServiceBuilder configureStartUp(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, DEFAULT_LOG_SEGMENT_SIZE * 3L);
        builder.setConfig(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, false);
        return builder;
    }

    protected KernelVersion headerVersionForStartingVersion() {
        return EXPECTED_HEADER_VERSION_LATEST_FORMAT;
    }

    protected KernelVersion startingKernelVersion() {
        return LATEST_KERNEL_VERSION;
    }

    @BeforeEach
    void setUp() {
        startDbms(this::configureStartUp);

        commandReaderFactory = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(StorageEngineFactory.class)
                .commandReaderFactory();
    }

    @AfterEach
    void tearDown() {
        shutdownDbms();
    }

    protected TestDatabaseManagementServiceBuilder configureGloriousFutureAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                .setConfig(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())
                .setConfig(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateOnKernelVersionChangeAndGetCorrectInfoInLogHeader(boolean useQueueAppender) throws Exception {
        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        createWriteTransaction(testDb);
        assertKernelVersion(testDb, startingKernelVersion());

        upgradeDatabase(managementService, testDb, startingKernelVersion(), GLORIOUS_FUTURE);

        long firstNewTransaction = testDb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getHighestGapFreeClosedTransactionId();
        WALService walService = testDb.getDependencyResolver().resolveDependency(WALService.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                INITIAL_LOG_VERSION,
                headerVersionForStartingVersion(),
                TransactionIdStore.BASE_TX_ID);
        AtomicInteger latestChecksum = new AtomicInteger();
        assertWholeTransactionsIn(
                walService.getLogFile(),
                INITIAL_LOG_VERSION,
                (startEntry) -> {},
                (commitEntry) -> latestChecksum.set(commitEntry.getChecksum()),
                commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                walService.getLogFile().getLogRangeInfo().highestVersion(),
                GLORIOUS_FUTURE,
                firstNewTransaction - 1 /* should point at the upgrade tx */,
                latestChecksum.get());

        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                walService.getLogFile(),
                INITIAL_LOG_VERSION,
                startingKernelVersion(),
                (int) (firstNewTransaction - 1 - TransactionIdStore.BASE_TX_ID),
                commandReaderFactory);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                walService.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateToNewFileWhenUpgradeTxIsLastOnStartup(boolean useQueueAppender) throws Exception {
        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        TransactionIdStore transactionIdStore =
                testDb.getDependencyResolver().resolveDependency(TransactionIdStore.class);
        long lastClosedTransactionIdBeforeUpgrade = transactionIdStore.getHighestGapFreeClosedTransactionId();

        long numNodesBefore = getNodeCount(testDb);

        // Register a handler that will make the transaction triggering the upgrade fail
        managementService.registerTransactionEventListener(
                DEFAULT_DATABASE_NAME, new InternalTransactionEventListener.Adapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                        if (data.metaData().containsKey("triggerTx")) {
                            throw TransactionFailureHelper.internalError(
                                    "Transaction log upgrade",
                                    "Failed because you asked for it",
                                    Status.Transaction.TransactionHookFailed);
                        }
                        return null;
                    }
                });

        // then upgrade dbms runtime to trigger db upgrade on next write
        upgradeDbms(managementService);

        GraphDatabaseAPI finalTestDb = testDb;
        assertThatThrownBy(() -> {
                    try (TransactionImpl tx = (TransactionImpl) finalTestDb.beginTx()) {
                        // metadata indicating we want it to fail
                        tx.setMetaData(Map.of("triggerTx", "something"));
                        tx.createNode(); // and make sure it is a write to trigger upgrade
                        tx.commit();
                    }
                })
                .isInstanceOf(TransactionFailureException.class);

        assertThat(getNodeCount(testDb))
                .as("Triggering transaction succeeded when it should fail")
                .isEqualTo(numNodesBefore);
        assertKernelVersion(testDb, GLORIOUS_FUTURE);
        LogPosition positionAfterUpgrade =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();

        // Now the upgrade transaction is our latest transaction in the log, and it is on the 'old' version
        shutdownDbms();

        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        assertKernelVersion(testDb, GLORIOUS_FUTURE);

        createWriteTransaction(testDb);

        WALService walService = testDb.getDependencyResolver().resolveDependency(WALService.class);

        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                INITIAL_LOG_VERSION,
                headerVersionForStartingVersion(),
                TransactionIdStore.BASE_TX_ID);
        AtomicInteger latestChecksum = new AtomicInteger();
        int nbrTxs = assertWholeTransactionsIn(
                walService.getLogFile(),
                INITIAL_LOG_VERSION,
                (startEntry) -> assertThat(startEntry.kernelVersion()).isEqualTo(startingKernelVersion()),
                (commitEntry) -> latestChecksum.set(commitEntry.getChecksum()),
                commandReaderFactory);
        assertThat(nbrTxs).isEqualTo((int) (lastClosedTransactionIdBeforeUpgrade - TransactionIdStore.BASE_TX_ID + 1));
        assertThat(fileSystem.getFileSize(walService.getLogFile().getLogFileForVersion(INITIAL_LOG_VERSION)))
                .isEqualTo(positionAfterUpgrade.getByteOffset());
        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                INITIAL_LOG_VERSION + 1,
                GLORIOUS_FUTURE,
                lastClosedTransactionIdBeforeUpgrade + 1,
                latestChecksum.get());
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                walService.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void makeCleanRotationOnVersionChange(boolean useQueueAppender) throws Throwable {
        int nbrTxsPerContestant = 10;
        int nbrContestants = 5;

        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        long txsBefore = testDb.getDependencyResolver()
                        .resolveDependency(TransactionIdStore.class)
                        .getLastCommittedTransactionId()
                - TransactionIdStore.BASE_TX_ID;

        Barrier.Control atLeastOneTxDoneBeforeAfter = new Barrier.Control();
        Race race = new Race();
        race.addContestant(
                () -> {
                    createWriteTransaction(testDb);
                    atLeastOneTxDoneBeforeAfter.reached();
                    createWriteTransaction(testDb);
                },
                1);
        race.addContestants(
                nbrContestants,
                () -> {
                    for (int i = 0; i < nbrTxsPerContestant; i++) {
                        createWriteTransaction(testDb);
                    }
                },
                1);
        race.addContestant(
                Race.throwing(() -> {
                    atLeastOneTxDoneBeforeAfter.await();
                    upgradeDbms(managementService);
                    atLeastOneTxDoneBeforeAfter.release();
                }),
                1);

        race.go();

        WALService walService = testDb.getDependencyResolver().resolveDependency(WALService.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                INITIAL_LOG_VERSION,
                headerVersionForStartingVersion(),
                TransactionIdStore.BASE_TX_ID);
        assertLogHeaderExpectedVersion(
                fileSystem,
                walService,
                walService.getLogFile().getLogRangeInfo().highestVersion(),
                GLORIOUS_FUTURE);

        int nbrTxsIn0 = assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                walService.getLogFile(), INITIAL_LOG_VERSION, startingKernelVersion(), commandReaderFactory);
        assertThat(nbrTxsIn0).isGreaterThanOrEqualTo(2); // At least upgrade and one before
        int nbrTxsIn1 = assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                walService.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, commandReaderFactory);
        assertThat(nbrTxsIn1).isGreaterThanOrEqualTo(1); // At least the one waiting for the barrier
        assertThat(nbrTxsIn0 + nbrTxsIn1)
                .isEqualTo(nbrContestants * nbrTxsPerContestant
                        + 2 /* the guaranteed before and after */
                        + 1 /* the version update */
                        + txsBefore);
    }

    protected long getNodeCount(GraphDatabaseAPI db) {
        try (Transaction tx = db.beginTx()) {
            return Iterables.count(tx.getAllNodes());
        }
    }

    protected void startDbms(Configuration configuration) {
        managementService = configuration
                .configure(new TestDatabaseManagementServiceBuilder(neo4jLayout)
                        .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                        .setConfig(automatic_upgrade_enabled, false))
                .build();
        testDb = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    protected void shutdownDbms() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    @FunctionalInterface
    interface Configuration {
        TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder);
    }
}
