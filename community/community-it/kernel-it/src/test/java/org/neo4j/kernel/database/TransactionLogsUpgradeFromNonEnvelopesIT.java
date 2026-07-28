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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.fs.ReadableChannel.BASE_TERM;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;
import static org.neo4j.kernel.TxLogValidationUtils.assertLogHeaderExpectedVersion;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsIn;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsWithCorrectVersionInSpecificLogVersion;
import static org.neo4j.kernel.TxLogValidationUtils.readEntryTerms;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.kernel.recovery.RecoveryHelpers.getLatestCheckpoint;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;
import static org.neo4j.test.LatestVersions.LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.createWriteTransaction;
import static org.neo4j.test.UpgradeTestUtil.upgradeDatabase;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.TxLogValidationUtils;
import org.neo4j.kernel.impl.api.tracer.DefaultDatabaseTracer;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.RecoveryHelpers;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.SkipOnSpd;

@SkipOnSpd(reason = "The transaction stream looks different in SPD")
class TransactionLogsUpgradeFromNonEnvelopesIT extends TransactionLogsUpgradeIT {

    @Override
    protected TestDatabaseManagementServiceBuilder configureStartUp(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, DEFAULT_LOG_SEGMENT_SIZE * 3L);
        builder.setConfig(
                GraphDatabaseInternalSettings.latest_kernel_version, LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES.version());
        builder.setConfig(
                GraphDatabaseInternalSettings.latest_runtime_version,
                LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES.getVersion());
        builder.setConfig(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, false);
        return builder;
    }

    @Override
    protected KernelVersion headerVersionForStartingVersion() {
        return null; /* pre-envelope format doesn't include the version */
    }

    @Override
    protected KernelVersion startingKernelVersion() {
        return LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void canFindNextLogFileIfHaveFileWithJustHeader(boolean useQueueAppender) throws Throwable {
        // There is a corner case where the upgrade transaction can trigger a rotation just after being written. And
        // then the transaction after the upgrade also triggers a rotation because it is on a new version. There will
        // then be an empty file in the middle, but it should still work.
        var defaultDatabaseTracer = testDb.getDependencyResolver().resolveDependency(DefaultDatabaseTracer.class);

        // Fill log with slightly more than we need to trigger rotation later
        while (defaultDatabaseTracer.appendedBytes() < DEFAULT_LOG_SEGMENT_SIZE * 2.2) {
            createWriteTransaction(testDb);
        }
        assertThat(defaultDatabaseTracer.numberOfLogRotations()).isEqualTo(0);
        long nodeCountBeforeTxTriggeringUpgrade = getNodeCount(testDb);
        long lastClosedTransactionIdBeforeUpgrade = testDb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getHighestGapFreeClosedTransactionId();

        shutdownDbms();
        // Set rotation so that the first transaction (upgrade) should trigger rotation.
        // We should then end up with one log file with everything including upgrade tx,
        // one log file that only contains a header and one logfile with the tx in the new version.
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, DEFAULT_LOG_SEGMENT_SIZE * 2L)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        upgradeDatabase(managementService, testDb, startingKernelVersion(), GLORIOUS_FUTURE);

        DatabaseLayout dbLayout = testDb.databaseLayout();
        var tracer = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(DefaultDatabaseTracer.class);
        assertThat(tracer.numberOfLogRotations()).isEqualTo(2);

        shutdownDbms();

        var config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())
                .build();
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(dbLayout, fileSystem, config);
        assertThat(getLatestCheckpoint(dbLayout, fileSystem, config).kernelVersion())
                .isEqualTo(startingKernelVersion());

        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        assertKernelVersion(testDb, GLORIOUS_FUTURE);
        // We managed to read all the way past the empty log file and saw the tx in the last logfile.
        assertThat(getNodeCount(testDb)).isEqualTo(nodeCountBeforeTxTriggeringUpgrade + 1);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION,
                headerVersionForStartingVersion(),
                TransactionIdStore.BASE_TX_ID);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(),
                INITIAL_LOG_VERSION,
                startingKernelVersion(),
                (int) (lastClosedTransactionIdBeforeUpgrade + 1 - TransactionIdStore.BASE_TX_ID),
                commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION + 1,
                headerVersionForStartingVersion(),
                lastClosedTransactionIdBeforeUpgrade + 1);
        assertThat(fileSystem.getFileSize(logFiles.getLogFile().getLogFileForVersion(INITIAL_LOG_VERSION + 1)))
                .isEqualTo(LogFormat.fromKernelVersion(startingKernelVersion()).getHeaderSize());
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 1, startingKernelVersion(), 0, commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION + 2,
                GLORIOUS_FUTURE,
                lastClosedTransactionIdBeforeUpgrade + 1);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 2, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @Test
    void shouldReadOverFormatSwitch() throws Exception {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest);

        createWriteTransaction(testDb);
        assertKernelVersion(testDb, startingKernelVersion());

        upgradeDatabase(managementService, testDb, startingKernelVersion(), GLORIOUS_FUTURE);
        createWriteTransaction(testDb);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);

        assertThat(assertWholeTransactionsIn(
                        logFiles.getLogFile(),
                        INITIAL_LOG_VERSION,
                        (startEntry) -> {},
                        (commitEntry) -> {},
                        commandReaderFactory,
                        ReaderLogVersionBridge.forFile(logFiles.getLogFile())))
                .isBetween(5, 6); // One extra token tx on record
    }

    @Test
    void shouldMaintainValidTermOnFormatSwitch() throws Exception {
        // Confirm that pre-envelope term information reports consistently as BASE_TERM
        LogFiles logFilesOld = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        LogFile logFileOld = logFilesOld.getLogFile();
        LogMetadataProvider metadataProviderOld = logFilesOld.logMetadataProvider();
        long oldTerm = metadataProviderOld.getCurrentTerm();
        assertThat(oldTerm).isEqualTo(BASE_TERM);
        LogHeader oldLogHeader = logFileOld.extractHeader(logFileOld.getCurrentLogVersion());
        assertThat(oldLogHeader.getLogFormatVersion().usesSegments()).isFalse();
        assertThat(oldLogHeader.getLastTerm()).isEqualTo(oldTerm);
        shutdownDbms();

        // upgrade onto envelopes
        startDbms(this::configureGloriousFutureAsLatest);

        upgradeDatabase(managementService, testDb, startingKernelVersion(), GLORIOUS_FUTURE);
        createWriteTransaction(testDb);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        LogFile logFile = logFiles.getLogFile();
        LogMetadataProvider metadataProvider = logFiles.logMetadataProvider();
        long term = metadataProvider.getCurrentTerm();
        assertThat(term).isEqualTo(BASE_TERM);
        LogHeader logHeader = logFile.extractHeader(logFile.getCurrentLogVersion());
        assertThat(logHeader.getLogFormatVersion().usesSegments()).isTrue();
        assertThat(logHeader.getLastTerm()).isEqualTo(term);

        List<TxLogValidationUtils.EntryTerm> entryTerms = readEntryTerms(logFile, logFile.getCurrentLogVersion());
        assertThat(entryTerms)
                .isNotEmpty()
                .allSatisfy(entry -> assertThat(entry.term()).isEqualTo(BASE_TERM));
    }
}
