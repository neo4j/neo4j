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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.LatestVersions.LATEST_RUNTIME_VERSION;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.v522.DetachedCheckpointLogEntrySerializerV5_22;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.storageengine.api.TransactionIdStore;

class EnvelopedRecoveryCorruptedTransactionLogIT extends RecoveryCorruptedTransactionLogIT {
    // TODO MERGELOG turn into no-op when default format
    @Override
    protected Map<Setting<?>, Object> additionalConfig() {
        return Map.of(
                GraphDatabaseInternalSettings.latest_kernel_version,
                VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED.version(),
                GraphDatabaseInternalSettings.latest_runtime_version,
                LATEST_RUNTIME_VERSION.kernelVersion().isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED)
                        ? LATEST_RUNTIME_VERSION.getVersion()
                        : DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion(),
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create,
                true);
    }

    @Override
    protected KernelVersion kernelVersion() {
        return VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
    }

    @Override
    protected int checkpointRecordSize() {
        return DetachedCheckpointLogEntrySerializerV5_22.checkPointRecordSizeDependingOnVersion(true);
    }

    @Override
    protected int minimumBytesToConsiderARecordBroken() {
        // Anything the length of an envelope header or less is just considered a broken
        // last entry by default and will not be considered corrupted (just last broken entry which is recoverable).
        return LogEnvelopeHeader.HEADER_SIZE + 1;
    }

    @Test
    void allowRecoveryOnHeaderEndingInPreviousChecksum() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        LogPosition logOffsetBeforeTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        generateTransaction(database);
        long numberOfClosedTransactions = getTransactionIdStore(database).getHighestGapFreeClosedTransactionId()
                - lastClosedTransactionBeforeStart;
        LogPosition logOffsetAfterTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        generateTransaction(database);

        // Zero out everything after first part of header of the second transaction.
        // Will break it in the middle of the previousChecksum field
        managementService.shutdown();
        long offset = logOffsetAfterTestTransactions.getByteOffset()
                + (LogEnvelopeHeader.HEADER_SIZE
                        - (Long.BYTES + Byte.BYTES) // fields after checksum
                        - (Short.BYTES)); // most of checksum
        try (StoreChannel storeChannel = fileSystem.write(
                logFiles.getLogFile().getLogFileForVersion(logFiles.getLogFile().getCurrentLogVersion()))) {
            storeChannel.position(offset);
            storeChannel.writeAll(ByteBuffers.allocate(2000, ByteOrder.LITTLE_ENDIAN, INSTANCE));
        }
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        LogAssertions.assertThat(logProvider)
                .containsMessages("Recovery required from position LogPosition{logVersion=0, byteOffset="
                        + logOffsetBeforeTestTransactions.getByteOffset() + "}");
    }

    @Test
    void recoveryCanSkipOverNonTxContent() throws IOException {
        // This test is not testing a corrupted tx, just that it can read over entries in the
        // log that are not kernel transactions.
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        generateTransaction(database);

        // Write a non kernel entry
        FlushableLogPositionAwareChannel channel = database.getDependencyResolver()
                .resolveDependency(LogFiles.class)
                .getLogFile()
                .getTransactionLogWriter()
                .getChannel();
        channel.beginChecksumForWriting();
        channel.putVersion(VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED.version());
        channel.putContentType((byte) 5); // Non-kernel type
        channel.put(new byte[] {1, 2, 3, 4, 5}, 5);
        channel.putChecksum();

        // Fudge append index - move one forward to not make channel sad on next tx.
        LogMetadataProvider logMetadataProvider =
                database.getDependencyResolver().resolveDependency(LogMetadataProvider.class);
        logMetadataProvider.nextAppendIndex();

        generateTransaction(database);
        long numberOfClosedTransactions = getTransactionIdStore(database).getHighestGapFreeClosedTransactionId()
                - lastClosedTransactionBeforeStart;

        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        // The non kernel content should have been seen as a tx
        assertEquals(numberOfClosedTransactions + 1, recoveryMonitor.getNumberOfRecoveredTransactions());
    }

    private static Stream<Arguments> provideStoreIdAndMode() {
        StoreId badStoreId = StoreId.generateNew("block", "block", 1, 2);
        return Stream.of(
                Arguments.of(StoreId.UNKNOWN, false, false),
                Arguments.of(StoreId.UNKNOWN, true, false),
                Arguments.of(badStoreId, false, false),
                Arguments.of(badStoreId, true, false));
    }

    @Disabled("TODO MERGELOG: Enable when Merged Log functionality is out of transitory state.")
    @ParameterizedTest()
    @MethodSource("provideStoreIdAndMode")
    void checkStoreIdValidationLogic(StoreId storeId, boolean mergeLog, boolean shouldStart) throws IOException {
        Map<Setting<?>, Object> extraConfig = Map.of(
                GraphDatabaseInternalSettings.latest_kernel_version,
                KernelVersion.GLORIOUS_FUTURE.version(),
                GraphDatabaseInternalSettings.latest_runtime_version,
                DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion(),
                GraphDatabaseInternalSettings.merged_log,
                mergeLog,
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create,
                true);
        Path currentLogPath;
        try (var managementService = databaseFactory.setConfig(extraConfig).build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            generateTransaction(database);
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency(LogFiles.class);
            LogFile logFile = logFiles.getLogFile();
            currentLogPath = logFile.getLogFileForVersion(logFile.getCurrentLogVersion());
        }
        setLogHeaderToStoreId(currentLogPath, storeId);

        try (var managementService = databaseFactory.setConfig(extraConfig).build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            DatabaseStateService<?> dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            Optional<Throwable> possibleFailure = dbStateService.causeOfFailure(db.databaseId());
            assertEquals(shouldStart, possibleFailure.isEmpty());
            possibleFailure.ifPresent(
                    throwable -> assertThat(throwable.getCause())
                            .hasMessageContaining(
                                    "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify 'internal.dbms.tx_log.fail_on_corrupted_log_files=false'"));
        }
    }

    private void setLogHeaderToStoreId(Path currentLogPath, StoreId storeId) throws IOException {
        try (var storeChannel = fileSystem.write(currentLogPath)) {
            LogHeader currentHeader = LogHeaderReader.readLogHeader(storeChannel, true, currentLogPath, INSTANCE);
            LogHeader newHeader = currentHeader
                    .getLogFormatVersion()
                    .newHeader(
                            currentHeader.getLogVersion(),
                            currentHeader.getLastAppendIndex(),
                            currentHeader.getLastTerm(),
                            StoreIdentifier.newStoreIdentifier(storeId),
                            currentHeader.getSegmentBlockSize(),
                            currentHeader.getPreviousLogFileChecksum(),
                            currentHeader.getKernelVersion(),
                            UNSPECIFIED_CREATION_TIME);
            storeChannel.position(0L);
            LogFormat.writeLogHeader(storeChannel, newHeader, INSTANCE);
        }
    }
}
