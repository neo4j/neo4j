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
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogFileCreateEvent;

public class TransactionLogChannelAllocator {
    private final TransactionLogFilesContext logFilesContext;
    private final FileSystemAbstraction fileSystem;
    private final TransactionLogFilesHelper fileHelper;
    private final LogHeaderCache logHeaderCache;
    private final ChannelNativeAccessor nativeChannelAccessor;
    private final DatabaseTracer databaseTracer;

    public TransactionLogChannelAllocator(
            TransactionLogFilesContext logFilesContext,
            TransactionLogFilesHelper fileHelper,
            LogHeaderCache logHeaderCache,
            ChannelNativeAccessor nativeChannelAccessor) {
        this.logFilesContext = logFilesContext;
        this.fileSystem = logFilesContext.getFileSystem();
        this.databaseTracer = logFilesContext.getDatabaseTracers().getDatabaseTracer();
        this.fileHelper = fileHelper;
        this.logHeaderCache = logHeaderCache;
        this.nativeChannelAccessor = nativeChannelAccessor;
    }

    public PhysicalLogVersionedStoreChannel createLogChannel(
            long version,
            long lastAppendIndex,
            int previousLogFileChecksum,
            KernelVersionProvider kernelVersionProvider)
            throws IOException {
        AllocatedFile allocatedFile = allocateFile(version);
        var storeChannel = allocatedFile.storeChannel();
        var logFile = allocatedFile.path();
        LogHeader header = readLogHeader(storeChannel, false, logFile, logFilesContext.getMemoryTracker());
        if (header == null) {
            try (LogFileCreateEvent ignored = databaseTracer.createLogFile()) {
                // we always write file header from the beginning of the file
                storeChannel.position(0);
                KernelVersion kernelVersion = kernelVersionProvider.kernelVersion();
                header = LogFormat.fromKernelVersion(kernelVersion)
                        .newHeader(
                                version,
                                lastAppendIndex,
                                logFilesContext.getStoreId(),
                                logFilesContext.getEnvelopeSegmentBlockSizeBytes(),
                                previousLogFileChecksum,
                                kernelVersion);
                writeLogHeader(storeChannel, header, logFilesContext.getMemoryTracker());
            }
        }
        assert header.getLogVersion() == version;
        logHeaderCache.putHeader(version, header);

        storeChannel.position(header.getStartPosition().getByteOffset());

        return new PhysicalLogVersionedStoreChannel(
                storeChannel, version, header.getLogFormatVersion(), logFile, nativeChannelAccessor, databaseTracer);
    }

    public PhysicalLogVersionedStoreChannel createLogChannelExistingVersion(long version) throws IOException {
        AllocatedFile allocatedFile = allocateExistingFile(version);
        var storeChannel = allocatedFile.storeChannel();
        var logFile = allocatedFile.path();
        LogHeader header = readLogHeader(storeChannel, true, logFile, logFilesContext.getMemoryTracker());
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
        Path fileToOpen = fileHelper.getLogFileForVersion(version);

        if (!fileSystem.fileExists(fileToOpen)) {
            throw new NoSuchFileException(fileToOpen.toAbsolutePath().toString());
        }
        databaseTracer.openLogFile(fileToOpen);

        StoreChannel rawChannel = null;
        try {
            rawChannel = fileSystem.read(fileToOpen);
            LogHeader header = readLogHeader(rawChannel, true, fileToOpen, logFilesContext.getMemoryTracker());
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
                nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(rawChannel, version);
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
        Path fileToOpen = fileHelper.getLogFileForVersion(version);

        if (!fileSystem.fileExists(fileToOpen)) {
            throw new NoSuchFileException(fileToOpen.toAbsolutePath().toString());
        }

        try (StoreChannel read = fileSystem.read(fileToOpen)) {
            return readLogHeader(read, true, fileToOpen, logFilesContext.getMemoryTracker());
        }
    }

    private AllocatedFile allocateFile(long version) throws IOException {
        Path file = fileHelper.getLogFileForVersion(version);
        boolean fileExist = fileSystem.fileExists(file);
        StoreChannel storeChannel = fileSystem.write(file);
        if (fileExist) {
            nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(storeChannel, version);
        } else if (logFilesContext.getTryPreallocateTransactionLogs().get()) {
            nativeChannelAccessor.preallocateSpace(storeChannel, version);
        }
        return new AllocatedFile(file, storeChannel);
    }

    private AllocatedFile allocateExistingFile(long version) throws IOException {
        Path file = fileHelper.getLogFileForVersion(version);
        boolean fileExist = fileSystem.fileExists(file);
        if (!fileExist) {
            throw new NoSuchFileException(file.toAbsolutePath().toString());
        }
        StoreChannel storeChannel = fileSystem.write(file);
        nativeChannelAccessor.adviseSequentialAccessAndKeepInCache(storeChannel, version);
        return new AllocatedFile(file, storeChannel);
    }

    private record AllocatedFile(Path path, StoreChannel storeChannel) {}
}
