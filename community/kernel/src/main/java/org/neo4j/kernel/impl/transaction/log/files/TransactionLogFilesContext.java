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

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;

public class TransactionLogFilesContext {
    private final AtomicLong rotationThreshold;
    private final long checkpointRotationThreshold;
    private final AtomicBoolean tryPreallocateTransactionLogs;
    private final CommandReaderFactory commandReaderFactory;
    private final LastCommittedTransactionIdProvider lastCommittedTransactionIdSupplier;
    private final LastAppendIndexProvider lastAppendIndexProvider;
    private final LongSupplier committingTransactionIdSupplier;
    private final LastClosedPositionProvider lastClosedPositionProvider;
    private final LogVersionRepositoryProvider logVersionRepositoryProvider;
    private final LogFileVersionTracker versionTracker;
    private final FileSystemAbstraction fileSystem;
    private final InternalLogProvider logProvider;
    private final DatabaseTracers databaseTracers;
    private final NativeAccess nativeAccess;
    private final MemoryTracker memoryTracker;
    private final Monitors monitors;
    private final boolean failOnCorruptedLogFiles;
    private final Supplier<StoreId> storeId;
    private final DatabaseHealth databaseHealth;
    private final KernelVersionProvider kernelVersionProvider;
    private final Clock clock;
    private final String databaseName;
    private final Config config;
    private final LogTailMetadata externalTailInfo;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final boolean readOnly;
    private final int envelopeSegmentBlockSizeBytes;
    private final int bufferSizeBytes;

    public TransactionLogFilesContext(
            AtomicLong rotationThreshold,
            long checkpointRotationThreshold,
            AtomicBoolean tryPreallocateTransactionLogs,
            CommandReaderFactory commandReaderFactory,
            LastCommittedTransactionIdProvider lastCommittedTransactionIdSupplier,
            LastAppendIndexProvider lastAppendIndexProvider,
            LongSupplier committingTransactionIdSupplier,
            LastClosedPositionProvider lastClosedPositionProvider,
            LogVersionRepositoryProvider logVersionRepositoryProvider,
            LogFileVersionTracker versionTracker,
            FileSystemAbstraction fileSystem,
            InternalLogProvider logProvider,
            DatabaseTracers databaseTracers,
            Supplier<StoreId> storeId,
            NativeAccess nativeAccess,
            MemoryTracker memoryTracker,
            Monitors monitors,
            boolean failOnCorruptedLogFiles,
            DatabaseHealth databaseHealth,
            KernelVersionProvider kernelVersionProvider,
            Clock clock,
            String databaseName,
            Config config,
            LogTailMetadata externalTailInfo,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            boolean readOnly,
            int envelopeSegmentBlockSizeBytes,
            int bufferSizeBytes) {
        this.rotationThreshold = rotationThreshold;
        this.checkpointRotationThreshold = checkpointRotationThreshold;
        this.tryPreallocateTransactionLogs = tryPreallocateTransactionLogs;
        this.commandReaderFactory = commandReaderFactory;
        this.lastCommittedTransactionIdSupplier = lastCommittedTransactionIdSupplier;
        this.lastAppendIndexProvider = lastAppendIndexProvider;
        this.committingTransactionIdSupplier = committingTransactionIdSupplier;
        this.lastClosedPositionProvider = lastClosedPositionProvider;
        this.logVersionRepositoryProvider = logVersionRepositoryProvider;
        this.versionTracker = versionTracker;
        this.fileSystem = fileSystem;
        this.logProvider = logProvider;
        this.databaseTracers = databaseTracers;
        this.storeId = storeId;
        this.nativeAccess = nativeAccess;
        this.memoryTracker = memoryTracker;
        this.monitors = monitors;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.databaseHealth = databaseHealth;
        this.kernelVersionProvider = kernelVersionProvider;
        this.clock = clock;
        this.databaseName = databaseName;
        this.config = config;
        this.externalTailInfo = externalTailInfo;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.readOnly = readOnly;
        this.envelopeSegmentBlockSizeBytes = envelopeSegmentBlockSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
    }

    AtomicLong getRotationThreshold() {
        return rotationThreshold;
    }

    public long getCheckpointRotationThreshold() {
        return checkpointRotationThreshold;
    }

    public CommandReaderFactory getCommandReaderFactory() {
        return commandReaderFactory;
    }

    public LogVersionRepositoryProvider getLogVersionRepositoryProvider() {
        return logVersionRepositoryProvider;
    }

    public LogFileVersionTracker getLogFileVersionTracker() {
        return versionTracker;
    }

    public LastCommittedTransactionIdProvider getLastCommittedTransactionIdProvider() {
        return lastCommittedTransactionIdSupplier;
    }

    public long committingTransactionId() {
        return committingTransactionIdSupplier.getAsLong();
    }

    public long appendIndex() {
        return lastAppendIndexProvider.lastAppendIndex();
    }

    LastClosedPositionProvider getLastClosedTransactionPositionProvider() {
        return lastClosedPositionProvider;
    }

    public FileSystemAbstraction getFileSystem() {
        return fileSystem;
    }

    public InternalLogProvider getLogProvider() {
        return logProvider;
    }

    AtomicBoolean getTryPreallocateTransactionLogs() {
        return tryPreallocateTransactionLogs;
    }

    public NativeAccess getNativeAccess() {
        return nativeAccess;
    }

    public DatabaseTracers getDatabaseTracers() {
        return databaseTracers;
    }

    public StoreId getStoreId() {
        return storeId.get();
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public Monitors getMonitors() {
        return monitors;
    }

    public boolean isFailOnCorruptedLogFiles() {
        return failOnCorruptedLogFiles;
    }

    public DatabaseHealth getDatabaseHealth() {
        return databaseHealth;
    }

    public KernelVersionProvider getKernelVersionProvider() {
        return kernelVersionProvider;
    }

    public Clock getClock() {
        return clock;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Config getConfig() {
        return config;
    }

    public LogTailMetadata getExternalTailInfo() {
        return externalTailInfo;
    }

    public BinarySupportedKernelVersions getBinarySupportedKernelVersions() {
        return binarySupportedKernelVersions;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getEnvelopeSegmentBlockSizeBytes() {
        return envelopeSegmentBlockSizeBytes;
    }

    public int getBufferSizeBytes() {
        return bufferSizeBytes;
    }
}
