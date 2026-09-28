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
package org.neo4j.wal.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_log_buffer_size;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.MathUtil.roundUp;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.wal.files.LogFilesBuilder.logFilesBasedOnlyBuilder;
import static org.neo4j.wal.files.LogFilesBuilder.writeableBuilder;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogSegments;

@Neo4jLayoutExtension
class LogFilesBuilderTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    private Path storeDirectory;

    @BeforeEach
    void setUp() {
        storeDirectory = testDirectory.homePath();
    }

    @Test
    void buildFilesBasedContext() {
        TransactionLogFilesContext context = logFilesBasedOnlyBuilder(storeDirectory, fileSystem)
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .buildContext();
        assertEquals(fileSystem, context.fileSystem());
    }

    @Test
    void buildDefaultContext() {
        LogFilesBuilder builder = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS);
        TransactionLogFilesContext context = builder.buildContext();
        TransactionLogFilesOverrides overrides = builder.buildOverrides();
        TransactionLogFilesProviders providers =
                new TransactionLogFilesProviders(mock(LogMetadataProvider.class), overrides);
        assertEquals(fileSystem, context.fileSystem());
        assertNotNull(context.commandReaderFactory());
        assertEquals(
                roundUp(ByteUnit.mebiBytes(256), context.envelopeSegmentBlockSizeBytes()),
                context.rotationThreshold().get());
        assertEquals(1, providers.appendIndex());
        assertEquals(2, providers.getLogVersionRepository().getCurrentLogVersion());
    }

    @Test
    void guaranteeMinimumTwoSegmentsForRotation() {
        LogFilesBuilder builder = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withConfig(Config.defaults(logical_log_rotation_threshold, kibiBytes(128)));
        TransactionLogFilesContext context = builder.buildContext();
        TransactionLogFilesOverrides overrides = builder.buildOverrides();
        TransactionLogFilesProviders providers =
                new TransactionLogFilesProviders(mock(LogMetadataProvider.class), overrides);
        assertEquals(fileSystem, context.fileSystem());
        assertNotNull(context.commandReaderFactory());
        assertEquals(
                context.envelopeSegmentBlockSizeBytes() * 2L,
                context.rotationThreshold().get());
        assertEquals(1, providers.appendIndex());
        assertEquals(2, providers.getLogVersionRepository().getCurrentLogVersion());
    }

    @Test
    void guaranteeMinimumTwoSegmentsForCheckpointRotation() {
        TransactionLogFilesContext context = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withConfig(Config.defaults(checkpoint_logical_log_rotation_threshold, kibiBytes(128)))
                .buildContext();
        assertEquals(context.envelopeSegmentBlockSizeBytes() * 2L, context.checkpointRotationThreshold());
    }

    @Test
    void keepConfigWhenBiggerThanTwoSegmentsForCheckpointRotation() {
        TransactionLogFilesContext context = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withConfig(Config.defaults(
                        checkpoint_logical_log_rotation_threshold, LogSegments.DEFAULT_LOG_SEGMENT_SIZE * 4L))
                .buildContext();
        assertEquals(context.envelopeSegmentBlockSizeBytes() * 4L, context.checkpointRotationThreshold());
    }

    @Test
    void buildContextWithRotationThreshold() {
        LogFilesBuilder builder = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withConfig(Config.defaults(transaction_log_buffer_size, ByteUnit.mebiBytes(1)));
        TransactionLogFilesContext context = builder.buildContext();
        TransactionLogFilesOverrides overrides = builder.buildOverrides();
        TransactionLogFilesProviders providers =
                new TransactionLogFilesProviders(mock(LogMetadataProvider.class), overrides);
        assertEquals(fileSystem, context.fileSystem());
        assertNotNull(context.commandReaderFactory());
        assertEquals(ByteUnit.mebiBytes(1), context.rotationThreshold().get());
        assertEquals(1, providers.appendIndex());
        assertEquals(2, providers.getLogVersionRepository().getCurrentLogVersion());
    }

    @Test
    void buildDefaultContextWithDependencies() {
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository(2);
        DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance());
        Dependencies dependencies = dependenciesOf(databaseHealth);

        LogFilesBuilder builder = writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withDependencies(dependencies)
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withLogVersionRepository(logVersionRepository);
        TransactionLogFilesContext context = builder.buildContext();
        TransactionLogFilesOverrides overrides = builder.buildOverrides();
        TransactionLogFilesProviders providers =
                new TransactionLogFilesProviders(mock(LogMetadataProvider.class), overrides);

        assertEquals(fileSystem, context.fileSystem());
        assertNotNull(context.commandReaderFactory());
        assertEquals(
                roundUp(ByteUnit.mebiBytes(256), context.envelopeSegmentBlockSizeBytes()),
                context.rotationThreshold().get());
        assertEquals(databaseHealth, context.databaseHealth());
        assertEquals(1, providers.appendIndex());
        assertEquals(2, providers.getLogVersionRepository().getCurrentLogVersion());
    }

    @Test
    void buildContextWithCustomAbsoluteLogFilesLocations() throws Throwable {
        Path customLogDirectory = testDirectory.directory("absoluteCustomLogDirectory");
        Config config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, customLogDirectory.toAbsolutePath())
                .build();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        WALService walService = writeableBuilder(
                        DatabaseLayout.of(config),
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withStoreId(storeId)
                .build();
        walService.init();
        walService.start();

        assertEquals(
                customLogDirectory.resolve(databaseLayout.getDatabaseName()),
                walService.getLogFile().getLogRangeInfo().highestFile().getParent());
        walService.shutdown();
    }

    @Test
    void buildWithCustomLogFileVersionTracker() throws Throwable {
        final var tracker = mock(LogFileVersionTracker.class);

        final var logDirectory = testDirectory.directory("logs");
        final var config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, logDirectory.toAbsolutePath())
                .set(logical_log_rotation_threshold, kibiBytes(256))
                .build();

        final var logFiles = writeableBuilder(
                        DatabaseLayout.of(config),
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withLogFileVersionTracker(tracker)
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withStoreId(new StoreId(1, 2, "engine-1", "format-1", 3, 4))
                .withConfig(config)
                .build();
        try {
            logFiles.init();
            logFiles.start();

            final var logFile = logFiles.getLogFile();
            logFile.rotate();

            LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
            final var lowestLogVersion = logRangeInfo.lowestVersion();
            final var logPosition =
                    new LogPosition(lowestLogVersion, fileSystem.getFileSize(logRangeInfo.lowestFile()));
            logFile.delete(lowestLogVersion);

            verify(tracker).logDeleted(eq(lowestLogVersion));
            verify(tracker).logCompleted(eq(logPosition));
        } finally {
            logFiles.stop();
            logFiles.shutdown();
        }
    }
}
