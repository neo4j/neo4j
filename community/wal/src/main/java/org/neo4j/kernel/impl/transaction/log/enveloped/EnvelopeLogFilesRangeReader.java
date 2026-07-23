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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class EnvelopeLogFilesRangeReader implements EnvelopeLogRangeReader {
    private static final int MAX_RETRIES = 2;

    private final EnvelopedLogFiles envelopedLogFiles;
    private final InternalLog log;

    public EnvelopeLogFilesRangeReader(EnvelopedLogFiles envelopedLogFiles, InternalLogProvider logProvider) {
        this.envelopedLogFiles = envelopedLogFiles;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public StoreChannelsForTransfer storeChannels(long fromIndex, long desiredToIndex) throws IOException {
        return channels(fromIndex, desiredToIndex, false);
    }

    @Override
    public StoreChannelsForTransfer entryStreamChannels(long fromIndex, long desiredToIndex) throws IOException {
        return channels(fromIndex, desiredToIndex, true);
    }

    private StoreChannelsForTransfer channels(long fromIndex, long desiredToIndex, boolean entryStream)
            throws IOException {
        return retryOnConcurrentDeletion(() -> {
            long toIndex = capToIndex(desiredToIndex);
            long availableFromIndex = availableFromIndex();
            if (fromIndex == -1) {
                return channelsFor(availableFromIndex, toIndex, entryStream);
            }
            if (fromIndex < availableFromIndex) {
                return StoreChannelsForTransfer.nothingToTransfer(availableFromIndex, fromIndex);
            }
            return channelsFor(fromIndex, toIndex, entryStream);
        });
    }

    private StoreChannelsForTransfer channelsFor(long fromIndex, long toIndex, boolean entryStream) throws IOException {
        if (toIndex < fromIndex) {
            return StoreChannelsForTransfer.nothingToTransfer(fromIndex, toIndex);
        }
        return entryStream
                ? envelopedLogFiles.entryStreamChannels(fromIndex, toIndex)
                : envelopedLogFiles.storeChannels(fromIndex, toIndex);
    }

    /** Subclasses bound the range end by their own visibility marker (commit index, flushed index, ...). */
    protected long capToIndex(long desiredToIndex) {
        return desiredToIndex;
    }

    protected long availableFromIndex() throws IOException {
        var logFilesMetadata = envelopedLogFiles.logFilesMetadata(false);
        logFilesMetadata.next();
        return logFilesMetadata.get().logHeader().getLastAppendIndex() + 1;
    }

    private StoreChannelsForTransfer retryOnConcurrentDeletion(
            ThrowingSupplier<StoreChannelsForTransfer, IOException> supplier) throws IOException {
        IOException lastException = null;
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                return supplier.get();
            } catch (NoSuchFileException | AccessDeniedException e) {
                // A log file has been (or is being) deleted concurrently with us calculating the availableFromIndex.
                // POSIX surfaces this as NoSuchFileException; Windows surfaces an in-progress deletion as
                // AccessDeniedException, because a file marked for deletion cannot be opened until the delete
                // completes. Both are the same logical race and are handled the same way: retry.
                lastException = e;
            }
        }
        log.warn(
                "Failed to obtain store channels for transfer after " + (MAX_RETRIES + 1) + " attempts", lastException);
        throw lastException;
    }

    @Override
    public long term(long index) throws IOException {
        if (index == -1) {
            return -1;
        }
        try (var readChannel = envelopedLogFiles.openReadChannel(index)) {
            if (readChannel != null) {
                readChannel.goToEntry(index);
                return readChannel.currentTerm();
            }
        }
        var logFilesMetadata = envelopedLogFiles.logFilesMetadata();
        logFilesMetadata.next();
        var logFileMetadata = logFilesMetadata.get();
        if (logFileMetadata.logHeader().getLastAppendIndex() == index) {
            return logFileMetadata.logHeader().getLastTerm();
        }

        // This should be unreachable under normal conditions: if we have index+1 (which is what we are about to send),
        // then both the index and its corresponding term must be available in the log header.
        // If we reach this point, it likely indicates a log pruning operation occurred concurrently with the catchup
        // process.
        throw new IOException("No log file found for index " + index + " unable to determine the term for the entry");
    }

    @Override
    public long highestReadableIndex() {
        return envelopedLogFiles.currentWriteChannel().currentIndex();
    }
}
