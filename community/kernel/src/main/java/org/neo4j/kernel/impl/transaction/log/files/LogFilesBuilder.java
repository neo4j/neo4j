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
package org.neo4j.kernel.impl.transaction.log.files;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_log_buffer_size;
import static org.neo4j.internal.helpers.MathUtil.roundUp;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.RecoveryOutcome;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Transactional log files facade class builder.
 * Depending from required abilities user can choose what kind of facade instance is required: from fully functional
 * to simplified that can operate only based on available log files without accessing stores and other external
 * components.
 * <br/>
 * Builder allow to configure any dependency explicitly and will use default value if that exist otherwise.
 * More specific dependencies always take precedence over more generic.
 * <br/>
 * For example: provided rotation threshold will
 * be used in precedence of value that can be specified in provided config.
 */
public class LogFilesBuilder {
    private StorageEngineFactory storageEngineFactory;
    private CommandReaderFactory commandReaderFactory;
    private DatabaseLayout databaseLayout;
    private Path logsDirectory;
    private Config config;
    private Long rotationThreshold;
    private InternalLogProvider logProvider = NullLogProvider.getInstance();
    private DependencyResolver dependencies;
    private FileSystemAbstraction fileSystem;
    private LogVersionRepository logVersionRepository;
    private LogFileVersionTracker logFileVersionTracker;
    private TransactionIdStore transactionIdStore;
    private AppendIndexProvider appendIndexProvider;
    private IntSupplier lastCommittedChecksumProvider;
    private boolean fileBasedOperationsOnly;
    private DatabaseTracers databaseTracers = DatabaseTracers.EMPTY;
    private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
    private DatabaseHealth databaseHealth;
    private Clock clock;
    private Monitors monitors;
    private StoreId storeId;
    private KernelVersionProvider emptyLogskernelVersionProvider = KernelVersionProvider.THROWING_PROVIDER;
    private KernelVersionProvider kernelVersionProvider = null;
    private LogFormatVersionProvider emptyLogsLogFormatProvider = LogFormatVersionProvider.THROWING_PROVIDER;
    private LogFormatVersionProvider logFormatVersionProvider = null;
    private LogTailMetadata externalLogTail;
    private int envelopeSegmentBlockSizeBytes = LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
    private int bufferSizeBytes;
    private boolean readOnlyLogs;
    private boolean noInit;
    private boolean turnOffPreallocation;
    private LogPosition tailReadingMaxPosition = LogPosition.UNSPECIFIED;
    private RecoveryOutcome recoveryOutcome = RecoveryOutcome.EMPTY_OUTCOME;

    private LogFilesBuilder() {}

    /**
     * Builder for fully functional transactional log files.
     * Log files will be able to access store and external components information, perform rotations, etc.
     * @param databaseLayout database directory
     * @param fileSystem log files filesystem
     * @param emptyLogsKernelVersionProvider provider of the kernel version to use for empty log files.
     */
    public static LogFilesBuilder writeableBuilder(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            KernelVersionProvider emptyLogsKernelVersionProvider,
            LogFormatVersionProvider emptyLogsLogFormatVersionProvider) {
        LogFilesBuilder filesBuilder = new LogFilesBuilder();
        filesBuilder.databaseLayout = databaseLayout;
        filesBuilder.fileSystem = fileSystem;
        filesBuilder.emptyLogskernelVersionProvider = emptyLogsKernelVersionProvider;
        filesBuilder.emptyLogsLogFormatProvider = emptyLogsLogFormatVersionProvider;
        return filesBuilder;
    }

    public static LogFilesBuilder readableBuilder(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            KernelVersionProvider emptyLogsKernelVersionProvider,
            LogFormatVersionProvider emptyLogsLogFormatVersionProvider) {
        LogFilesBuilder builder = writeableBuilder(
                databaseLayout, fileSystem, emptyLogsKernelVersionProvider, emptyLogsLogFormatVersionProvider);
        builder.turnOffPreallocation = true;
        builder.readOnlyLogs = true;
        builder.noInit = true;
        return builder;
    }

    /**
     * Build log files that will be able to perform only operations on a log files directly.
     * Any operation that will require access to a store or other parts of runtime will fail.
     * Should be mainly used only for testing purposes or when only file based operations will be performed
     * @param logsDirectory log files directory
     * @param fileSystem file system
     */
    public static LogFilesBuilder logFilesBasedOnlyBuilder(Path logsDirectory, FileSystemAbstraction fileSystem) {
        LogFilesBuilder builder = new LogFilesBuilder();
        builder.logsDirectory = logsDirectory;
        builder.databaseLayout = DatabaseLayout.ofFlat(logsDirectory);
        builder.fileSystem = fileSystem;
        builder.fileBasedOperationsOnly = true;
        builder.readOnlyLogs = true;
        builder.turnOffPreallocation = true;
        builder.noInit = true;
        return builder;
    }

    public LogFilesBuilder withLogVersionRepository(LogVersionRepository logVersionRepository) {
        this.logVersionRepository = logVersionRepository;
        return this;
    }

    public LogFilesBuilder withLogFileVersionTracker(LogFileVersionTracker logFileVersionTracker) {
        this.logFileVersionTracker = logFileVersionTracker;
        return this;
    }

    public LogFilesBuilder withTransactionIdStore(TransactionIdStore transactionIdStore) {
        this.transactionIdStore = transactionIdStore;
        return this;
    }

    public LogFilesBuilder withExternalLogTailMetadata(LogTailMetadata logTailMetadata) {
        this.externalLogTail = logTailMetadata;
        return this;
    }

    public LogFilesBuilder withLogProvider(InternalLogProvider logProvider) {
        this.logProvider = logProvider;
        return this;
    }

    public LogFilesBuilder withLastCommittedChecksumProvider(IntSupplier checksumProvider) {
        this.lastCommittedChecksumProvider = checksumProvider;
        return this;
    }

    public LogFilesBuilder withConfig(Config config) {
        this.config = config;
        return this;
    }

    public LogFilesBuilder withMonitors(Monitors monitors) {
        this.monitors = monitors;
        return this;
    }

    public LogFilesBuilder withRotationThreshold(long rotationThreshold) {
        this.rotationThreshold = rotationThreshold;
        return this;
    }

    // NOTE that only the parts that make up TransactionLogFilesContext will look in dependencies.
    // Any other overrides need to call their individual method.
    public LogFilesBuilder withDependencies(DependencyResolver dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public LogFilesBuilder withDatabaseTracers(DatabaseTracers databaseTracers) {
        this.databaseTracers = databaseTracers;
        return this;
    }

    public LogFilesBuilder withMemoryTracker(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        return this;
    }

    public LogFilesBuilder withStoreId(StoreId storeId) {
        this.storeId = storeId;
        return this;
    }

    public LogFilesBuilder withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public LogFilesBuilder withDatabaseHealth(DatabaseHealth databaseHealth) {
        this.databaseHealth = databaseHealth;
        return this;
    }

    public LogFilesBuilder withStorageEngineFactory(StorageEngineFactory storageEngineFactory) {
        this.storageEngineFactory = storageEngineFactory;
        return this;
    }

    public LogFilesBuilder withCommandReaderFactory(CommandReaderFactory commandReaderFactory) {
        this.commandReaderFactory = commandReaderFactory;
        return this;
    }

    public LogFilesBuilder withAppendIndexProvider(AppendIndexProvider appendIndexProvider) {
        this.appendIndexProvider = appendIndexProvider;
        return this;
    }

    public LogFilesBuilder withLogsDirectory(Path logsDirectory) {
        this.logsDirectory = logsDirectory;
        return this;
    }

    public LogFilesBuilder withEnvelopeSegmentBlockSizeBytes(int envelopeSegmentBlockSizeBytes) {
        this.envelopeSegmentBlockSizeBytes = envelopeSegmentBlockSizeBytes;
        return this;
    }

    public LogFilesBuilder withBufferSizeBytes(int overrideBufferSizeBytes) {
        this.bufferSizeBytes = overrideBufferSizeBytes;
        return this;
    }

    public LogFilesBuilder withKernelVersionProvider(KernelVersionProvider kernelVersionProvider) {
        this.kernelVersionProvider = kernelVersionProvider;
        return this;
    }

    public LogFilesBuilder withLogFormatVersionProvider(LogFormatVersionProvider logFormatVersionProvider) {
        this.logFormatVersionProvider = logFormatVersionProvider;
        return this;
    }

    public LogFilesBuilder withInitializeProviders() {
        this.noInit = false;
        return this;
    }

    public LogFilesBuilder withNoInit() {
        this.noInit = true;
        return this;
    }

    public LogFilesBuilder withNoPreallocation() {
        this.turnOffPreallocation = true;
        return this;
    }

    public LogFilesBuilder withRecoveryOutcome(RecoveryOutcome recoveryOutcome) {
        this.recoveryOutcome = recoveryOutcome;
        return this;
    }

    /**
     * If the logfiles have a moving tail and should only be evaluated up to a specific position.
     */
    public LogFilesBuilder withTailReadingMaxPosition(LogPosition maxPosition) {
        this.tailReadingMaxPosition = maxPosition;
        return this;
    }

    public LogFiles build() throws IOException {
        TransactionLogFilesContext filesContext = buildContext();
        TransactionLogFilesOverrides overrides = buildOverrides();

        Path logsDirectory = getLogsDirectory();
        filesContext.fileSystem().mkdirs(logsDirectory);
        return new TransactionLogFiles(logsDirectory, filesContext, overrides);
    }

    private Path getLogsDirectory() {
        return requireNonNullElseGet(logsDirectory, () -> databaseLayout.getTransactionLogsDirectory());
    }

    TransactionLogFilesOverrides buildOverrides() {
        return new TransactionLogFilesOverrides(
                logVersionRepository,
                transactionIdStore,
                appendIndexProvider,
                lastCommittedChecksumProvider,
                databaseLayout,
                logFormatVersionProvider,
                kernelVersionProvider,
                externalLogTail,
                noInit,
                noInit || fileBasedOperationsOnly || readOnlyLogs,
                tailReadingMaxPosition);
    }

    TransactionLogFilesContext buildContext() {
        if (config == null) {
            config = Config.defaults();
        }
        requireNonNull(fileSystem);
        Supplier<StoreId> storeIdSupplier = getStoreId();
        LogFileVersionTracker versionTracker = getLogFileVersionTracker();

        // Register listener for rotation threshold
        AtomicLong rotationThreshold = getRotationThresholdAndRegisterForUpdates();
        long checkpointThreshold = getCheckpointRotationThreshold();
        AtomicBoolean tryPreallocateTransactionLogs = getTryToPreallocateTransactionLogs();
        var nativeAccess = getNativeAccess();
        var monitors = getMonitors();
        var health = getDatabaseHealth();
        var clock = getClock();

        return new TransactionLogFilesContext(
                rotationThreshold,
                checkpointThreshold,
                tryPreallocateTransactionLogs,
                commandReaderFactory(),
                versionTracker,
                fileSystem,
                logProvider,
                databaseTracers,
                storeIdSupplier,
                nativeAccess,
                memoryTracker,
                monitors,
                config.get(fail_on_corrupted_log_files),
                health,
                emptyLogskernelVersionProvider,
                emptyLogsLogFormatProvider,
                clock,
                databaseLayout.getDatabaseName(),
                config,
                new BinarySupportedKernelVersions(config),
                readOnlyLogs,
                envelopeSegmentBlockSizeBytes,
                getBufferSizeBytes(),
                recoveryOutcome);
    }

    private CommandReaderFactory commandReaderFactory() {
        if (commandReaderFactory != null) {
            return commandReaderFactory;
        }
        if (fileBasedOperationsOnly) {
            return CommandReaderFactory.NO_COMMANDS;
        }
        return storageEngineFactory().commandReaderFactory();
    }

    private StorageEngineFactory storageEngineFactory() {
        if (storageEngineFactory == null) {
            storageEngineFactory = StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                    .orElseThrow();
        }
        return storageEngineFactory;
    }

    private Clock getClock() {
        if (clock != null) {
            return clock;
        }
        return Clock.systemUTC();
    }

    private DatabaseHealth getDatabaseHealth() {
        if (databaseHealth != null) {
            return databaseHealth;
        }
        if (dependencies == null) {
            return new DatabaseHealth(HealthEventGenerator.NO_OP, logProvider.getLog(DatabaseHealth.class));
        }
        return dependencies
                .resolveOptionalDependency(DatabaseHealth.class)
                .orElse(new DatabaseHealth(HealthEventGenerator.NO_OP, logProvider.getLog(DatabaseHealth.class)));
    }

    private Monitors getMonitors() {
        if (monitors == null) {
            return new Monitors();
        }
        return monitors;
    }

    private NativeAccess getNativeAccess() {
        if (dependencies == null) {
            return NativeAccessProvider.getNativeAccess();
        }
        return dependencies
                .resolveOptionalDependency(NativeAccess.class)
                .orElseGet(NativeAccessProvider::getNativeAccess);
    }

    private int getBufferSizeBytes() {
        if (bufferSizeBytes == 0) {
            return (int) roundUpToEnvelopeSegment(config.get(transaction_log_buffer_size));
        }
        return (int) roundUpToEnvelopeSegment(bufferSizeBytes);
    }

    private AtomicLong getRotationThresholdAndRegisterForUpdates() {
        if (rotationThreshold != null) {
            return new AtomicLong(guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(rotationThreshold)));
        }
        AtomicLong configThreshold = new AtomicLong(
                guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(config.get(logical_log_rotation_threshold))));
        config.addListener(
                logical_log_rotation_threshold,
                (prev, update) -> configThreshold.set(guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(update))));
        return configThreshold;
    }

    private long getCheckpointRotationThreshold() {
        return guaranteeAtLeastTwoSegments(
                roundUpToEnvelopeSegment(config.get(checkpoint_logical_log_rotation_threshold)));
    }

    private long roundUpToEnvelopeSegment(long bytes) {
        return roundUp(bytes, envelopeSegmentBlockSizeBytes);
    }

    private long guaranteeAtLeastTwoSegments(long rotationThreshold) {
        return Math.max(rotationThreshold, envelopeSegmentBlockSizeBytes * 2L);
    }

    private AtomicBoolean getTryToPreallocateTransactionLogs() {
        if (turnOffPreallocation) {
            return new AtomicBoolean(false);
        }
        AtomicBoolean tryToPreallocate = new AtomicBoolean(config.get(preallocate_logical_logs));
        config.addListener(preallocate_logical_logs, (prev, update) -> {
            String logMessage = "Updating " + preallocate_logical_logs.name() + " from " + prev + " to " + update;
            logProvider.getLog(LogFiles.class).debug(logMessage);
            tryToPreallocate.set(update);
        });
        return tryToPreallocate;
    }

    private LogFileVersionTracker getLogFileVersionTracker() {
        if (logFileVersionTracker != null) {
            return logFileVersionTracker;
        }
        if (dependencies == null) {
            return LogFileVersionTracker.NO_OP;
        }
        return dependencies
                .resolveOptionalDependency(LogFileVersionTracker.class)
                .orElse(LogFileVersionTracker.NO_OP);
    }

    private Supplier<StoreId> getStoreId() {
        if (storeId != null) {
            return () -> storeId;
        }
        if (fileBasedOperationsOnly) {
            return () -> {
                throw new UnsupportedOperationException("Current version of log files can't perform any "
                        + "operation that require availability of store id. Please build full version of log files "
                        + "to be able to use them.");
            };
        }
        return () -> resolveDependency(StoreIdProvider.class).getStoreId();
    }

    private <T> T resolveDependency(Class<T> clazz) {
        return dependencies.resolveDependency(clazz);
    }
}
