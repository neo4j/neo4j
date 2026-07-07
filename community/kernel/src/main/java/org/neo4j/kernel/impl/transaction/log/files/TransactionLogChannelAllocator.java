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

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.dynamic_read_only_failover;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.StoreIdentifier;

public class TransactionLogChannelAllocator {
    private final TransactionLogFilesContext logFilesContext;
    private final FileSystemAbstraction fileSystem;
    private final SequentialFileNameHelper fileHelper;
    private final LogHeaderCache logHeaderCache;
    private final ChannelNativeAccessor nativeChannelAccessor;
    private final DatabaseTracer databaseTracer;
    private final AtomicLong rotationThreshold;

    public TransactionLogChannelAllocator(
            TransactionLogFilesContext logFilesContext,
            SequentialFileNameHelper fileHelper,
            LogHeaderCache logHeaderCache,
            AtomicLong rotationThreshold) {
        this.logFilesContext = logFilesContext;
        this.fileSystem = logFilesContext.fileSystem();
        this.databaseTracer = logFilesContext.databaseTracers().getDatabaseTracer();
        this.fileHelper = fileHelper;
        this.logHeaderCache = logHeaderCache;
        this.nativeChannelAccessor = new StoreChannelNativeAccessor(
                logFilesContext.fileSystem(),
                logFilesContext.nativeAccess(),
                logFilesContext.logProvider(),
                new TransactionLogOutOfDiskHandler(logFilesContext));
        this.rotationThreshold = rotationThreshold;
    }

    public PhysicalLogVersionedStoreChannel createLogChannel(
            long version,
            long lastAppendIndex,
            int previousLogFileChecksum,
            KernelVersionProvider kernelVersionProvider,
            LogFormatVersionProvider logFormatProvider)
            throws IOException {
        AllocatedFile allocatedFile = allocateFile(version);
        var storeChannel = allocatedFile.storeChannel();
        var logFile = allocatedFile.path();
        LogHeader header = maybeInitLogHeader(
                version,
                lastAppendIndex,
                previousLogFileChecksum,
                kernelVersionProvider,
                logFormatProvider,
                storeChannel,
                logFile);
        assert header.getLogVersion() == version;
        logHeaderCache.putHeader(version, header);

        storeChannel.position(header.getStartPosition().getByteOffset());

        return new PhysicalLogVersionedStoreChannel(
                storeChannel, version, header.getLogFormatVersion(), logFile, nativeChannelAccessor, databaseTracer);
    }

    public void initializeLogFile(
            long version,
            long lastAppendIndex,
            int previousLogFileChecksum,
            KernelVersionProvider kernelVersionProvider,
            LogFormatVersionProvider logFormatVersionProvider)
            throws IOException {
        var allocatedFile = allocateFile(version);
        try (StoreChannel storeChannel = allocatedFile.storeChannel()) {
            maybeInitLogHeader(
                    version,
                    lastAppendIndex,
                    previousLogFileChecksum,
                    kernelVersionProvider,
                    logFormatVersionProvider,
                    storeChannel,
                    allocatedFile.path());
        }
    }

    private LogHeader maybeInitLogHeader(
            long version,
            long lastAppendIndex,
            int previousLogFileChecksum,
            KernelVersionProvider kernelVersionProvider,
            LogFormatVersionProvider logFormatProvider,
            StoreChannel storeChannel,
            Path logFile)
            throws IOException {
        LogHeader header = readLogHeader(storeChannel, false, logFile, logFilesContext.memoryTracker());
        if (header == null) {
            try (LogFileCreateEvent createEvent = databaseTracer.createLogFile()) {
                // we always write file header from the beginning of the file
                storeChannel.position(0);
                KernelVersion kernelVersion = kernelVersionProvider.kernelVersion();
                header = logFormatProvider
                        .getCurrentLogFormat()
                        .newHeader(
                                version,
                                lastAppendIndex,
                                LogHeader.UNKNOWN_TERM,
                                StoreIdentifier.newStoreIdentifier(
                                        logFilesContext.storeId().get()),
                                logFilesContext.envelopeSegmentBlockSizeBytes(),
                                previousLogFileChecksum,
                                kernelVersion,
                                logFilesContext.clock().millis());
                writeLogHeader(storeChannel, header, logFilesContext.memoryTracker());
                createEvent.fileCreated(header.getStartPosition().getByteOffset());
            }
        }
        return header;
    }

    public PhysicalLogVersionedStoreChannel createLogChannelExistingVersion(long version) throws IOException {
        AllocatedFile allocatedFile = allocateExistingFile(version);
        var storeChannel = allocatedFile.storeChannel();
        var logFile = allocatedFile.path();
        LogHeader header = readLogHeader(storeChannel, true, logFile, logFilesContext.memoryTracker());
        if (header == null) {
            // Either there was nothing at all, or we read one byte and saw that it was a preallocated file.
            throw new IncompleteLogHeaderException(allocatedFile.path, (int) storeChannel.position(), -1);
        }
        assert header.getLogVersion() == version;
        logHeaderCache.putHeader(version, header);

        storeChannel.position(header.getStartPosition().getByteOffset());

        return new PhysicalLogVersionedStoreChannel(
                storeChannel, version, header.getLogFormatVersion(), logFile, nativeChannelAccessor, databaseTracer);
    }

    public PhysicalLogVersionedStoreChannel openLogChannel(long version) throws IOException {
        return openLogChannel(version, false);
    }

    public PhysicalLogVersionedStoreChannel openLogChannel(long version, boolean raw) throws IOException {
        Path fileToOpen = fileHelper.getFileForVersion(version);

        if (!fileSystem.fileExists(fileToOpen)) {
            throw new NoSuchFileException(fileToOpen.toAbsolutePath().toString());
        }
        databaseTracer.openLogFile(fileToOpen);

        StoreChannel rawChannel = null;
        try {
            rawChannel = fileSystem.read(fileToOpen);
            LogHeader header = readLogHeader(rawChannel, true, fileToOpen, logFilesContext.memoryTracker());
            if (header == null) {
                throw new IncompleteLogHeaderException(fileToOpen, 0, Long.BYTES);
            }
            if (header.getLogVersion() != version) {
                throw new IllegalStateException(format(
                        "Unexpected log file header. Expected header version: %d, actual header: %s", version, header));
            }
            var versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                    rawChannel,
                    version,
                    header.getLogFormatVersion(),
                    fileToOpen,
                    nativeChannelAccessor,
                    databaseTracer,
                    raw);
            if (!raw) {
                nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(rawChannel, fileToOpen);
            }
            return versionedStoreChannel;
        } catch (NoSuchFileException cause) {
            throw (NoSuchFileException)
                    new NoSuchFileException(fileToOpen.toAbsolutePath().toString()).initCause(cause);
        } catch (Throwable unexpectedError) {
            if (rawChannel != null) {
                // If we managed to open the file before failing, then close the channel
                try {
                    rawChannel.close();
                } catch (IOException e) {
                    unexpectedError.addSuppressed(e);
                }
            }
            throw unexpectedError;
        }
    }

    public LogHeader readLogHeaderForVersion(long version) throws IOException {
        Path fileToOpen = fileHelper.getFileForVersion(version);

        if (!fileSystem.fileExists(fileToOpen)) {
            throw new NoSuchFileException(fileToOpen.toAbsolutePath().toString());
        }

        try (StoreChannel read = fileSystem.read(fileToOpen)) {
            return readLogHeader(read, true, fileToOpen, logFilesContext.memoryTracker());
        }
    }

    private AllocatedFile allocateFile(long version) throws IOException {
        Path file = fileHelper.getFileForVersion(version);
        boolean fileExist = fileSystem.fileExists(file);
        StoreChannel storeChannel = fileSystem.write(file);
        if (fileExist) {
            nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(storeChannel, file);
        } else if (logFilesContext.tryPreallocateTransactionLogs().get()) {
            nativeChannelAccessor.preallocateSpace(storeChannel, rotationThreshold.get(), file);
        }
        return new AllocatedFile(file, storeChannel);
    }

    private AllocatedFile allocateExistingFile(long version) throws IOException {
        Path file = fileHelper.getFileForVersion(version);
        boolean fileExist = fileSystem.fileExists(file);
        if (!fileExist) {
            throw new NoSuchFileException(file.toAbsolutePath().toString());
        }
        StoreChannel storeChannel = fileSystem.write(file);
        nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(storeChannel, file);
        return new AllocatedFile(file, storeChannel);
    }

    private record AllocatedFile(Path path, StoreChannel storeChannel) {}

    private static class TransactionLogOutOfDiskHandler implements ChannelNativeAccessor.OutOfDiskHandler {
        private final InternalLog log;
        private final Config config;
        private final String databaseName;

        public TransactionLogOutOfDiskHandler(TransactionLogFilesContext logFilesContext) {
            this.log = logFilesContext.logProvider().getLog(getClass());
            this.config = logFilesContext.config();
            this.databaseName = logFilesContext.databaseName();
        }

        @Override
        public void handle(String error) {
            log.error(
                    "Warning! System is running out of disk space. Failed to preallocate file since disk does not have enough space left. "
                            + "Please provision more space to avoid that. Allocation failure details: " + error);
            if (config.get(dynamic_read_only_failover)) {
                log.error("Switching database to read only mode.");
                markDatabaseReadOnly();
            } else {
                log.error(
                        "Dynamic switchover to read-only mode is disabled. The database will continue execution in the current mode.");
            }
        }

        private void markDatabaseReadOnly() {
            Set<String> readOnlyDatabases = new HashSet<>(config.get(read_only_databases));
            readOnlyDatabases.add(databaseName);
            config.setDynamic(read_only_databases, readOnlyDatabases, "Dynamic failover to read-only mode.");
        }
    }
}
