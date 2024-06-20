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
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.ReadOnlyLogVersionRepository;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
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
    private boolean readOnlyStores;
    private StorageEngineFactory storageEngineFactory;
    private CommandReaderFactory commandReaderFactory;
    private DatabaseLayout databaseLayout;
    private Path logsDirectory;
    private Config config;
    private Long rotationThreshold;
    private Long checkpointRotationThreshold;
    private InternalLogProvider logProvider = NullLogProvider.getInstance();
    private DependencyResolver dependencies;
    private FileSystemAbstraction fileSystem;
    private LogVersionRepository logVersionRepository;
    private LogFileVersionTracker logFileVersionTracker;
    private TransactionIdStore transactionIdStore;
    private AppendIndexProvider appendIndexProvider;
    private LongSupplier lastCommittedTransactionIdSupplier;
    private ThrowingSupplier<LogPosition, IOException> lastClosedPositionSupplier;
    private boolean fileBasedOperationsOnly;
    private DatabaseTracers databaseTracers = DatabaseTracers.EMPTY;
    private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
    private DatabaseHealth databaseHealth;
    private Clock clock;
    private Monitors monitors;
    private StoreId storeId;
    private NativeAccess nativeAccess;
    private KernelVersionProvider kernelVersionProvider;
    private LogTailMetadata externalLogTail;
    private int envelopeSegmentBlockSizeBytes = LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
    private int bufferSizeBytes;
    private boolean readOnlyLogs;

    private LogFilesBuilder() {}

    /**
     * Builder for fully functional transactional log files.
     * Log files will be able to access store and external components information, perform rotations, etc.
     * @param databaseLayout database directory
     * @param fileSystem log files filesystem
     * @param kernelVersionProvider provider of the kernel version to use for transactions and checkpoints.
     *                              Make sure that this is a provider that will listen to upgrade transactions
     *                              so the version is updated when needed.
     */
    public static LogFilesBuilder builder(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            KernelVersionProvider kernelVersionProvider) {
        LogFilesBuilder filesBuilder = new LogFilesBuilder();
        filesBuilder.databaseLayout = databaseLayout;
        filesBuilder.fileSystem = fileSystem;
        filesBuilder.kernelVersionProvider = kernelVersionProvider;
        return filesBuilder;
    }

    public static LogFilesBuilder builder(DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem) {
        LogFilesBuilder filesBuilder = new LogFilesBuilder();
        filesBuilder.databaseLayout = databaseLayout;
        filesBuilder.fileSystem = fileSystem;
        return filesBuilder;
    }

    /**
     * Build log files that can access and operate only on active set of log files without ability to
     * rotate and create any new one. Appending to current log file still possible.
     * Store and external components access available in read only mode.
     *
     * @param databaseLayout store directory
     * @param fileSystem log file system
     * @param kernelVersionProvider provider of the kernel version to use for transactions and checkpoints.
     *                              Make sure that this is a provider that will listen to upgrade transactions
     *                              so the version is updated when needed.
     */
    public static LogFilesBuilder activeFilesBuilder(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            KernelVersionProvider kernelVersionProvider) {
        LogFilesBuilder builder = builder(databaseLayout, fileSystem, kernelVersionProvider);
        builder.readOnlyStores = true;
        return builder;
    }

    public static LogFilesBuilder readOnlyBuilder(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            KernelVersionProvider kernelVersionProvider) {
        LogFilesBuilder builder = new LogFilesBuilder();
        builder.databaseLayout = databaseLayout;
        builder.fileSystem = fileSystem;
        builder.kernelVersionProvider = kernelVersionProvider;
        builder.readOnlyStores = true;
        builder.readOnlyLogs = true;
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
        return builder;
    }

    public LogFilesBuilder withLastClosedTransactionPositionSupplier(
            ThrowingSupplier<LogPosition, IOException> lastClosedPositionSupplier) {
        this.lastClosedPositionSupplier = lastClosedPositionSupplier;
        return this;
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

    public LogFilesBuilder withLastCommittedTransactionIdSupplier(LongSupplier transactionIdSupplier) {
        this.lastCommittedTransactionIdSupplier = transactionIdSupplier;
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

    public LogFilesBuilder withCheckpointRotationThreshold(long checkpointRotationThreshold) {
        this.checkpointRotationThreshold = checkpointRotationThreshold;
        return this;
    }

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

    public LogFilesBuilder withNativeAccess(NativeAccess nativeAccess) {
        this.nativeAccess = nativeAccess;
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

    public LogFiles build() throws IOException {
        TransactionLogFilesContext filesContext = buildContext();
        Path logsDirectory = getLogsDirectory();
        filesContext.getFileSystem().mkdirs(logsDirectory);
        return new TransactionLogFiles(logsDirectory, filesContext);
    }

    private Path getLogsDirectory() {
        return requireNonNullElseGet(logsDirectory, () -> databaseLayout.getTransactionLogsDirectory());
    }

    TransactionLogFilesContext buildContext() {
        if (config == null) {
            config = Config.defaults();
        }
        if (kernelVersionProvider == null) {
            kernelVersionProvider = () -> KernelVersion.getLatestVersion(config);
        }
        requireNonNull(fileSystem);
        Supplier<StoreId> storeIdSupplier = getStoreId();
        LogVersionRepositoryProvider logVersionRepositorySupplier = getLogVersionRepositoryProvider();
        LogFileVersionTracker versionTracker = getLogFileVersionTracker();

        LastAppendIndexProvider availableAppendIndexProvider = availableAppendIndexProvider();
        LastAppendIndexLogFilesProvider lastAppendIndexLogFilesProvider =
                lastAppendIndexLogFilesProvider(availableAppendIndexProvider);
        var appendIndexProvider = lastAppendIndexProvider(availableAppendIndexProvider);
        LastCommittedTransactionIdProvider lastCommittedIdSupplier = lastCommittedIdProvider();
        LastClosedPositionProvider lastClosedTransactionPositionProvider = closePositionProvider();

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
                lastAppendIndexLogFilesProvider,
                appendIndexProvider,
                lastCommittedIdSupplier,
                lastClosedTransactionPositionProvider,
                logVersionRepositorySupplier,
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
                kernelVersionProvider,
                clock,
                databaseLayout.getDatabaseName(),
                config,
                externalLogTail,
                new BinarySupportedKernelVersions(config),
                readOnlyLogs,
                envelopeSegmentBlockSizeBytes,
                getBufferSizeBytes());
    }

    private LastAppendIndexProvider availableAppendIndexProvider() {
        if (appendIndexProvider != null) {
            return appendIndexProvider::getLastAppendIndex;
        }
        if (dependencies != null && dependencies.containsDependency(MetadataProvider.class)) {
            MetadataProvider metadataProvider = resolveDependency(MetadataProvider.class);
            return metadataProvider::getLastAppendIndex;
        }
        return null;
    }

    private LastAppendIndexProvider lastAppendIndexProvider(LastAppendIndexProvider availableProvider) {
        if (availableProvider != null) {
            return availableProvider;
        }
        return () -> {
            throw new UnsupportedOperationException(
                    "Unable to provide last append index in this mode. Please build full version of log files to be able to use real append index provider.");
        };
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
        if (dependencies != null) {
            return dependencies.resolveDependency(DatabaseHealth.class);
        }
        return new DatabaseHealth(HealthEventGenerator.NO_OP, logProvider.getLog(DatabaseHealth.class));
    }

    private Monitors getMonitors() {
        if (monitors == null) {
            return new Monitors();
        }
        return monitors;
    }

    private NativeAccess getNativeAccess() {
        if (nativeAccess != null) {
            return nativeAccess;
        }
        if (dependencies != null && dependencies.containsDependency(NativeAccess.class)) {
            return dependencies.resolveDependency(NativeAccess.class);
        }
        return NativeAccessProvider.getNativeAccess();
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
        if (readOnlyStores) {
            return new AtomicLong(roundUpToEnvelopeSegment(Long.MAX_VALUE - envelopeSegmentBlockSizeBytes));
        }
        AtomicLong configThreshold = new AtomicLong(
                guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(config.get(logical_log_rotation_threshold))));
        config.addListener(
                logical_log_rotation_threshold,
                (prev, update) -> configThreshold.set(guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(update))));
        return configThreshold;
    }

    private long getCheckpointRotationThreshold() {
        if (checkpointRotationThreshold != null) {
            return guaranteeAtLeastTwoSegments(roundUpToEnvelopeSegment(checkpointRotationThreshold));
        }
        if (readOnlyLogs) {
            return roundUpToEnvelopeSegment(Long.MAX_VALUE - envelopeSegmentBlockSizeBytes);
        }
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
        if (readOnlyStores) {
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

    private LogVersionRepositoryProvider getLogVersionRepositoryProvider() {
        if (logVersionRepository != null) {
            return any -> logVersionRepository;
        }
        if (fileBasedOperationsOnly) {
            return any -> {
                throw new UnsupportedOperationException(
                        "Current version of log files can't perform any "
                                + "operation that require availability of log version repository. Please build full version of log files to be able to use them.");
            };
        }
        if (readOnlyStores) {
            requireNonNull(databaseLayout, "Store directory is required.");
            return new ReadOnlyLogVersionRepositoryProvider();
        } else {
            requireNonNull(
                    dependencies,
                    LogVersionRepository.class.getSimpleName() + " is required. "
                            + "Please provide an instance or a dependencies where it can be found.");
            return new SupplierLogVersionRepositoryProvider(dependencies.provideDependency(LogVersionRepository.class));
        }
    }

    private LogFileVersionTracker getLogFileVersionTracker() {
        if (logFileVersionTracker != null) {
            return logFileVersionTracker;
        }

        if (dependencies != null && dependencies.containsDependency(LogFileVersionTracker.class)) {
            return resolveDependency(LogFileVersionTracker.class);
        }

        return LogFileVersionTracker.NO_OP;
    }

    private LastAppendIndexLogFilesProvider lastAppendIndexLogFilesProvider(
            LastAppendIndexProvider lastAppendIndexProvider) {
        if (lastAppendIndexProvider != null) {
            return new LongSupplierLastAppendIndexLogFilesProvider(lastAppendIndexProvider::lastAppendIndex);
        }
        if (fileBasedOperationsOnly) {
            return any -> {
                throw new UnsupportedOperationException("Current version of log files can't perform any "
                        + "operation that require availability of append index provider. Please build full version of log files "
                        + "to be able to use them.");
            };
        }
        if (readOnlyStores) {
            requireNonNull(databaseLayout, "Store directory is required.");
            return new ReadOnlyLastAppendIndexLogFilesProvider();
        } else {
            return any -> {
                throw new UnsupportedOperationException("Current version of log files can't perform any "
                        + "operation that require availability of append index provider. Please build full version of log files "
                        + "to be able to use them.");
            };
        }
    }

    private LastCommittedTransactionIdProvider lastCommittedIdProvider() {
        if (lastCommittedTransactionIdSupplier != null) {
            return new LongSupplierLastCommittedTransactionIdProvider(lastCommittedTransactionIdSupplier);
        }
        if (transactionIdStore != null) {
            return new LongSupplierLastCommittedTransactionIdProvider(
                    transactionIdStore::getLastCommittedTransactionId);
        }
        if (fileBasedOperationsOnly) {
            return any -> {
                throw new UnsupportedOperationException("Current version of log files can't perform any "
                        + "operation that require availability of transaction id store. Please build full version of log files "
                        + "to be able to use them.");
            };
        }
        if (readOnlyStores) {
            requireNonNull(databaseLayout, "Store directory is required.");
            return new ReadOnlyLastCommittedTransactionIdProvider();
        } else {
            requireNonNull(
                    dependencies,
                    TransactionIdStore.class.getSimpleName() + " is required. "
                            + "Please provide an instance or a dependencies where it can be found.");
            return new LongSupplierLastCommittedTransactionIdProvider(
                    () -> resolveDependency(TransactionIdStore.class).getLastCommittedTransactionId());
        }
    }

    private LastClosedPositionProvider closePositionProvider() {
        if (lastClosedPositionSupplier != null) {
            return any -> lastClosedPositionSupplier.get();
        }
        if (transactionIdStore != null) {
            return any -> transactionIdStore.getLastClosedTransaction().logPosition();
        }
        if (fileBasedOperationsOnly) {
            return any -> {
                throw new UnsupportedOperationException("Current version of log files can't perform any "
                        + "operation that require availability of transaction id store. Please build full version of log files "
                        + "to be able to use them.");
            };
        }
        if (readOnlyStores) {
            requireNonNull(databaseLayout, "Store directory is required.");
            return logFiles -> logFiles.getTailMetadata().getLastTransactionLogPosition();
        } else {
            requireNonNull(
                    dependencies,
                    TransactionIdStore.class.getSimpleName() + " is required. "
                            + "Please provide an instance or a dependencies where it can be found.");
            return any -> resolveDependency(TransactionIdStore.class)
                    .getLastClosedTransaction()
                    .logPosition();
        }
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

    private static class LongSupplierLastCommittedTransactionIdProvider implements LastCommittedTransactionIdProvider {
        private final LongSupplier idSupplier;

        LongSupplierLastCommittedTransactionIdProvider(LongSupplier idSupplier) {
            this.idSupplier = idSupplier;
        }

        @Override
        public long getLastCommittedTransactionId(LogFiles logFiles) {
            return idSupplier.getAsLong();
        }
    }

    private static class ReadOnlyLastCommittedTransactionIdProvider implements LastCommittedTransactionIdProvider {
        @Override
        public long getLastCommittedTransactionId(LogFiles logFiles) {
            return logFiles.getTailMetadata().getLastCommittedTransaction().id();
        }
    }

    private static class LongSupplierLastAppendIndexLogFilesProvider implements LastAppendIndexLogFilesProvider {
        private final LongSupplier idSupplier;

        LongSupplierLastAppendIndexLogFilesProvider(LongSupplier idSupplier) {
            this.idSupplier = idSupplier;
        }

        @Override
        public long getLastAppendIndex(LogFiles logFiles) {
            return idSupplier.getAsLong();
        }
    }

    private static class ReadOnlyLastAppendIndexLogFilesProvider implements LastAppendIndexLogFilesProvider {
        @Override
        public long getLastAppendIndex(LogFiles logFiles) {
            return logFiles.getTailMetadata().getLastCheckpointedAppendIndex();
        }
    }

    private static class SupplierLogVersionRepositoryProvider implements LogVersionRepositoryProvider {
        private final Supplier<LogVersionRepository> supplier;

        SupplierLogVersionRepositoryProvider(Supplier<LogVersionRepository> supplier) {
            this.supplier = supplier;
        }

        @Override
        public LogVersionRepository logVersionRepository(LogFiles logFiles) {
            return supplier.get();
        }
    }

    private static class ReadOnlyLogVersionRepositoryProvider implements LogVersionRepositoryProvider {
        @Override
        public LogVersionRepository logVersionRepository(LogFiles logFiles) {
            return new ReadOnlyLogVersionRepository(logFiles.getTailMetadata());
        }
    }
}
