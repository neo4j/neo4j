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
import java.nio.ByteOrder;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotateEvents;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

public class EnvelopedLogFiles implements EnvelopeReadChannelProvider, AutoCloseable {
    public static final int MINIMUM_SEGMENTS = 2;
    private final int segmentBlockSize;
    private final int writerBufferedBlocks;
    private final MemoryTracker memoryTracker;
    private final LogRotation logRotation;
    private final LogTracers logTracers;
    private final LogsRepository logsRepository;
    private final EnvelopedLogHeaderCache logHeaderCache;
    private final long maxFileSize;
    private final LogHeaderFactory logHeaderFactory;
    private final LogFilesPruner logFilesPruner;
    private final ChannelNativeAccessor channelNativeAccessor;
    private final InternalLog log;
    private final InternalLogProvider logProvider;
    private LogChannelContext<StoreChannel> currentWriteChannel;
    private EnvelopeWriteChannel appendingChannel;

    public EnvelopedLogFiles(
            LogsRepository logsRepository,
            LogHeaderFactory logHeaderFactory,
            int segmentBlockSize,
            int writerBufferedBlocks,
            int totalSegments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            ChannelNativeAccessor channelNativeAccessor,
            InternalLogProvider logProvider,
            LogTracers logTracers) {
        if (totalSegments < MINIMUM_SEGMENTS) {
            throw new IllegalArgumentException(
                    String.format("Must have at least %d segments. Got %d", MINIMUM_SEGMENTS, totalSegments));
        }
        this.channelNativeAccessor = channelNativeAccessor;
        this.logHeaderFactory = logHeaderFactory;
        this.logsRepository = logsRepository;
        this.logHeaderCache = new EnvelopedLogHeaderCache();
        this.segmentBlockSize = segmentBlockSize;
        this.writerBufferedBlocks = writerBufferedBlocks;
        this.memoryTracker = memoryTracker;
        this.maxFileSize = totalSegments * (long) segmentBlockSize;
        this.logRotation = new EnvelopedLogRotation(this, maxFileSize);
        this.logFilesPruner = new LogFilesPruner(logsRepository, pruneThreshold);
        this.logProvider = logProvider;
        this.log = logProvider.getLog(EnvelopedLogFiles.class);
        this.logTracers = logTracers;
    }

    public EnvelopedLogFiles(
            LogsRepository logsRepository,
            LogHeaderFactory logHeaderFactory,
            int segmentBlockSize,
            int writerBufferedBlocks,
            int totalSegments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            ChannelNativeAccessor channelNativeAccessor,
            InternalLogProvider logProvider) {
        this(
                logsRepository,
                logHeaderFactory,
                segmentBlockSize,
                writerBufferedBlocks,
                totalSegments,
                memoryTracker,
                pruneThreshold,
                channelNativeAccessor,
                logProvider,
                LogTracers.NULL); // TODO MERGELOGS LogTracers currently only used in tests
    }

    public EnvelopeWriteChannel currentWriteChannel() {
        if (appendingChannel == null) {
            throw new IllegalStateException("Writer channel has not been initialised");
        }
        return appendingChannel;
    }

    public void setPruneThreshold(LogPruneThreshold threshold) {
        logFilesPruner.setThreshold(threshold);
    }

    @Override
    public EnvelopeReadChannel openReadChannel() throws IOException {
        var longRange = logsRepository.logVersionsRange();
        if (longRange.isEmpty()) {
            throw new IllegalStateException("No log files found " + logsRepository);
        }
        return envelopedReadChannel(logsRepository.openReadChannel(longRange.from()), false);
    }

    @Override
    public EnvelopeReadChannel openReadChannel(long entryIndex) throws IOException {
        long fileVersion = getFileVersion(entryIndex);
        if (fileVersion != -1) {
            return envelopedReadChannel(logsRepository.openReadChannel(fileVersion), false);
        }
        return null;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    private long getFileVersion(long entryIndex) throws IOException {
        if (logHeaderCache.isEmpty()) {
            var longRange = logsRepository.logVersionsRange();
            if (longRange.isEmpty()) {
                return -1;
            }
            var logFileBinarySearch = new LogFileBinarySearch(
                    logsRepository, longRange.from(), longRange.to() - longRange.from() + 1, memoryTracker);
            return LogBinarySearch.binarySearch(logFileBinarySearch, entryIndex);
        }
        return LogBinarySearch.binarySearch(logHeaderCache.binarySearchReader(), entryIndex);
    }

    public long initialise() throws IOException {
        clearCache();
        logsRepository.initialise();
        return recoverLogTail(0, true);
    }

    public void populateCache() throws IOException {
        try (var metadata = new LogFilesMetadata(logsRepository)) {
            while (metadata.next()) {
                var logHeader = metadata.get().logHeader();
                logHeaderCache.cache(logHeader);
            }
        }
        log.info("Populated log header cache");
    }

    public void clearCache() {
        logHeaderCache.clear();
        log.info("Log header cache cleared");
    }

    /**
     * Can be used to reset the write channel to the end of the log, for example, after all the
     * log files have been replaced by a store copy. This method should not be used if there's a
     * chance that the log is corrupt.
     */
    public long resetWriteChannelToLatestIndex() throws IOException {
        return recoverLogTail(logsRepository.latestVersion(), false);
    }

    private long recoverLogTail(long fromVersion, boolean populateCache) throws IOException {
        var tailChecker = new EnvelopedLogTailChecker(
                logsRepository,
                logFileVersion -> envelopedReadChannel(logsRepository.openReadChannel(logFileVersion), false),
                memoryTracker,
                logProvider);
        var tailInfo = tailChecker.checkEnvelopedLogTail(fromVersion);
        if (tailInfo.createInitial()) {
            log.info("No previous enveloped raft log files found. Creating new log file. " + tailInfo);
            long startVersion = tailInfo.lastValidatedPosition().getLogVersion();
            deleteLogFilesFrom(startVersion);
            if (populateCache) {
                populateCache();
            }
            var logChannelCtx = createNewStoreChannel(
                    startVersion,
                    logHeaderFactory.createLogHeader(
                            startVersion,
                            tailInfo.lastValidAppendIndex(),
                            tailInfo.lastValidChecksum(),
                            segmentBlockSize,
                            tailInfo.lastValidTerm()));
            updateState(
                    logChannelCtx,
                    tailInfo.lastValidChecksum(),
                    tailInfo.lastValidAppendIndex(),
                    tailInfo.lastValidTerm());
        } else if (tailInfo.brokenLastEntry()) {
            log.warn("Last raft log entry incomplete. Truncating log file to end of last complete entry. " + tailInfo);

            logHeaderFactory.setStoreIdentifier(tailInfo.storeIdentifier());

            long lastValidVersion = tailInfo.lastValidatedPosition().getLogVersion();
            var logChannelCtx = openWriteChannel(
                    lastValidVersion, tailInfo.lastValidatedPosition().getByteOffset());
            updateState(
                    logChannelCtx,
                    tailInfo.lastValidChecksum(),
                    tailInfo.lastValidAppendIndex(),
                    tailInfo.lastValidTerm());
            deleteLogFilesFrom(lastValidVersion + 1L);
            // truncateToPosition may create a new file, so populate the cache first
            if (populateCache) {
                populateCache();
            }
            appendingChannel.truncateToPosition(
                    tailInfo.lastValidatedPosition().getByteOffset(),
                    tailInfo.lastValidChecksum(),
                    tailInfo.lastValidAppendIndex(),
                    tailInfo.lastValidTerm());
            if (tailInfo.segmentOffset() > 0) {
                appendingChannel.insertStartOffset(tailInfo.segmentOffset());
            }
            appendingChannel.prepareForFlush().flush();
        } else {
            log.info("Reopen previous enveloped raft log file. " + tailInfo);

            logHeaderFactory.setStoreIdentifier(tailInfo.storeIdentifier());

            // stop updateState throwing if for some reason we call initialise twice
            if (appendingChannel != null) {
                appendingChannel.close();
                appendingChannel = null;
            }
            if (populateCache) {
                populateCache();
            }
            var logChannelCtx = openWriteChannel(
                    tailInfo.lastValidatedPosition().getLogVersion(),
                    tailInfo.lastValidatedPosition().getByteOffset());
            updateState(
                    logChannelCtx,
                    tailInfo.lastValidChecksum(),
                    tailInfo.lastValidAppendIndex(),
                    tailInfo.lastValidTerm());
        }
        return tailInfo.lastValidAppendIndex();
    }

    private void closeCurrentWriteChannel() throws IOException {
        if (appendingChannel != null) {
            appendingChannel.close();
            appendingChannel = null;
        }
        if (currentWriteChannel != null) {
            currentWriteChannel.channel().close();
            currentWriteChannel = null;
        }
    }

    public void updateWriteChannel(long index, int checksum, long prevIndex, long prevTerm) throws IOException {
        LogPosition position;
        try (var reader = this.openReadChannel(index)) {
            while (reader.entryIndex() != index) {
                reader.goToNextEntry();
            }

            position = reader.goToEndOfEntry();
        }

        this.closeCurrentWriteChannel();
        this.currentWriteChannel = this.openWriteChannel(position.getLogVersion(), position.getByteOffset());
        this.appendingChannel = this.envelopedWriteChannel(currentWriteChannel, checksum, prevIndex, prevTerm);
    }

    /**
     * Truncates the envelope log files.
     *
     * @param fromIndex the index to truncate from (inclusive)
     * @return the current write channel after the truncate.
     * @throws IllegalArgumentException if fromIndex is negative, higher than current index or if it has been pruned
     */
    public EnvelopeWriteChannel truncate(long fromIndex) throws IOException {
        if (fromIndex < 0) {
            throw new IllegalArgumentException("Negative values is not allowed " + fromIndex);
        }
        long lastAppendedIndex = appendingChannel == null ? -1 : appendingChannel.currentIndex();
        if (appendingChannel != null && fromIndex > lastAppendedIndex) {
            throw new IllegalArgumentException(
                    "Cannot truncate at index " + fromIndex + " when last appended index is " + lastAppendedIndex);
        }

        long position;
        long version;
        int prevChecksum;
        long prevTerm;
        int offset;
        try (var readChannel = openReadChannel(fromIndex)) {
            if (readChannel == null) {
                throw new IllegalArgumentException(fromIndex + " has been pruned");
            }
            version = readChannel.getLogVersion();
            prevTerm = readChannel.logHeader().getLastTerm();
            position = readChannel.position();
            prevChecksum = readChannel.logHeader().getPreviousLogFileChecksum();
            boolean envelopesRead = true;
            while (readChannel.entryIndex() < fromIndex) {
                // don't read values from the channel until it has definitely read an envelope header
                if (!envelopesRead) {
                    prevChecksum = readChannel.getChecksum();
                    prevTerm = readChannel.currentTerm();
                    version = readChannel.getLogVersion();
                }
                position = readChannel.goToNextEnvelope();
                envelopesRead = false;
            }
            offset = readChannel.getSegmentOffset(position);
        }
        if (currentWriteChannel.version() != version) {
            appendingChannel = null;
            currentWriteChannel.channel().close();
            currentWriteChannel = null;
            deleteLogFilesFrom(version + 1);
            currentWriteChannel = openWriteChannel(version, position);
            appendingChannel = envelopedWriteChannel(currentWriteChannel, -1, Integer.MAX_VALUE, prevTerm);
        } else {
            // delete any (empty) trailing files before trimming
            // so that we don't create a broken checksum chain due to the stale headers
            deleteLogFilesFrom(version + 1);
        }
        appendingChannel.truncateToPosition(position, prevChecksum, fromIndex - 1, prevTerm);
        if (offset > 0) {
            appendingChannel.insertStartOffset(offset);
        }
        appendingChannel.prepareForFlush().flush();
        return appendingChannel;
    }

    /**
     * Truncates the envelope log files after the last written entry.
     * No written entries are truncated, this should be called only
     * when we want to free the pre-allocated data in the log.
     */
    public void forceRotate() throws IOException {
        appendingChannel.prepareForFlush().flush();
        appendingChannel.truncateToPosition(
                appendingChannel.position(),
                appendingChannel.currentChecksum(),
                appendingChannel.currentIndex(),
                appendingChannel.currentTerm());
    }

    /**
     * Skip in the log to the desired index. This will delete all log files and create a new empty file with a header
     * pointing to the provided index and checksum.
     * <p>
     * This method only makes sense if the log is distributed as it is effectively creating a new log starting at
     * some higher entry. Henece only the "other log" can provide the correct context for the initial state of
     * the new log.
     *
     * @param index    the index to skip to. This will be the log header index and the next written entry will be index+1
     * @param checksum the checksum for the provided index.
     * @param offset   the offset for the provided index.
     * @param term     the term for the provided index
     */
    public void skip(long index, int checksum, int offset, long term) throws IOException {
        if (index > appendingChannel.currentIndex()) {
            var prunedVersion = logsRepository.logVersionsRange().to();
            deleteLogFilesTo(prunedVersion);
            long nextVersion = prunedVersion + 1;
            var newStoreChannel = createNewStoreChannel(
                    nextVersion,
                    logHeaderFactory.createLogHeader(nextVersion, index, checksum, segmentBlockSize, term));
            updateState(newStoreChannel, checksum, index, term);
            if (offset > 0) {
                currentWriteChannel().insertStartOffset(offset);
                currentWriteChannel().prepareForFlush().flush();
            }
        }
    }

    /**
     * Prunes the envelope files. Only entire files can be removed by prune.
     *
     * @param index the desired index to prune up to (exclusive)
     * @return the highest index that was removed after the prune event, or -1 if there is nothing to prune
     */
    public long prune(long index) throws IOException {
        long fileVersion = getFileVersion(index);
        if (fileVersion == -1) {
            return -1; // index already pruned
        }
        long versionToPrune = fileVersion - 1;
        if (!logsRepository.logVersionsRange().isWithinRange(versionToPrune)) {
            return -1;
        }
        var envelopeWriteChannel = currentWriteChannel();
        var prunedVersion = logFilesPruner.pruneUpTo(versionToPrune, envelopeWriteChannel.currentIndex());
        if (prunedVersion == -1) {
            return -1;
        }
        logHeaderCache.deleteTo(prunedVersion);
        assert !logsRepository.isEmpty();
        var logFilesMetadata = logFilesMetadata();
        logFilesMetadata.next();
        return logFilesMetadata.get().logHeader().getLastAppendIndex();
    }

    private void rotateCurrentFile(long lastAppendIndex, int checksum, long lastTerm) throws IOException {
        if (appendingChannel == null) {
            throw new IllegalStateException("Cannot rotate if not initialised");
        } else {
            var nextVersion = currentWriteChannel.version() + 1;
            appendingChannel.prepareForFlush().flush();
            currentWriteChannel.channel().truncate(currentWriteChannel.channel().position());
            // create new channel after truncate, because when rotate is called within
            // EnvelopedWriteChannel.truncateToPosition
            // it may be that the envelopes being trimmed contradict the checksum put into the new header
            // leading to false checksum chain violations if the process dies here
            var newStoreChannel = createNewStoreChannel(
                    nextVersion,
                    logHeaderFactory.createLogHeader(
                            nextVersion, lastAppendIndex, checksum, segmentBlockSize, lastTerm));
            updateState(newStoreChannel, checksum, lastAppendIndex, lastTerm);
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrentWriteChannel();
    }

    @VisibleForTesting
    void deleteLogFilesFrom(long version) throws IOException {
        logHeaderCache.deleteFrom(version);
        logsRepository.deleteLogFilesFrom(version);
    }

    @VisibleForTesting
    void deleteLogFilesTo(long version) throws IOException {
        logHeaderCache.deleteTo(version);
        logsRepository.deleteLogFilesTo(version);
    }

    /**
     * Recalibrates the write channel state after directPutAll operations have detached it from its state.
     * This method reads through the entries from the last segment boundary to validate the checksum chain.
     * If the last entry is incomplete, this method will fail.
     *
     * @throws InvalidLogEnvelopeReadException if incomplete entries are found at the end of the log
     * @throws IOException if an I/O error occurs during recovery
     */
    public void recalibrateWriteChannel() throws IOException {
        if (appendingChannel == null) {
            throw new IllegalStateException("Writer channel has not been initialised");
        }

        long currentPosition = appendingChannel.position();

        // If we're at the start (just the header), no recovery needed
        if (currentPosition <= segmentBlockSize) {
            return;
        }

        // Calculate the segment boundary to start reading from
        // If we're precisely aligned with a segment boundary, we need to start from the previous segment
        // to ensure we have entries to read for state recovery
        long segmentBoundary = (currentPosition / segmentBlockSize) * segmentBlockSize;
        if (segmentBoundary == currentPosition) {
            // We're aligned with a segment boundary, go back one segment
            segmentBoundary -= segmentBlockSize;
        }

        // Open a read channel on the current log file version directly
        long currentVersion = currentWriteChannel.version();
        log.info(
                "Recalibrating write-channel checksum-chain mirrors from on-disk tail (fileVersion=%d, segmentBoundary=%d, currentPosition=%d).",
                currentVersion, segmentBoundary, currentPosition);
        try (var readChannel = envelopedReadChannel(logsRepository.openReadChannel(currentVersion), false)) {
            // Position the read channel at the segment boundary
            readChannel.position(segmentBoundary);

            // Read through all entries to validate the checksum chain and recover state
            while (true) {
                // Try to read the next entry (complete entry, not just envelope)
                try {
                    readChannel.goToNextEntry();
                } catch (ReadPastEndException ignored) {
                    break;
                }
            }
            long lastValidPosition = readChannel.position();
            long recoveredIndex = readChannel.entryIndex();
            int recoveredChecksum = readChannel.getChecksum();
            long recoveredTerm = readChannel.currentTerm();

            // Check if there's incomplete data after the last valid position
            if (lastValidPosition < currentPosition) {
                // We have data written beyond the last valid entry - this is incomplete
                throw new InvalidLogEnvelopeReadException(
                        "Incomplete entry found at the end of the log. Last valid position: " + lastValidPosition
                                + ", current write position: " + currentPosition);
            }
            // Update the write channel's state with the recovered values
            appendingChannel.recoverState(recoveredChecksum, recoveredIndex, recoveredTerm);
        }
    }

    @VisibleForTesting
    public LogPosition currentWriteLogPosition() throws IOException {
        return new LogPosition(currentWriteChannel.version(), appendingChannel.position());
    }

    /**
     * Truncates to the last safe entry position when log pulling fails unexpectedly.
     * Finds the last complete entry that ends at or before the last segment boundary
     * and truncates to that entry.
     *
     * @param knownSafeIndex the position to start searching from, 0 if there is none
     * @throws IOException if an I/O error occurs during truncation
     */
    public void truncateToLastSafeEntry(long knownSafeIndex) throws IOException {
        closeCurrentWriteChannel();
        recoverLogTail(getFileVersion(knownSafeIndex), false);
    }

    private void updateState(
            LogChannelContext<StoreChannel> logChannelCtx, int checksumAtPosition, long prevIndex, long prevTerm)
            throws IOException {
        if (appendingChannel == null) {
            appendingChannel = envelopedWriteChannel(logChannelCtx, checksumAtPosition, prevIndex, prevTerm);
        } else {
            appendingChannel.prepareForFlush().flush();
            currentWriteChannel.channel().flush();
            if (prevIndex != appendingChannel.currentIndex()) {
                appendingChannel = envelopedWriteChannel(logChannelCtx, checksumAtPosition, prevIndex, prevTerm);
            } else {
                appendingChannel.setChannel(logChannelCtx.channel());
            }
            currentWriteChannel.channel().close();
        }
        currentWriteChannel = logChannelCtx;
    }

    private LogChannelContext<StoreChannel> createNewStoreChannel(long version, LogHeader logHeader)
            throws IOException {
        var logChannelCtx = logsRepository.createWriteChannel(version);
        channelNativeAccessor.preallocateSpace(logChannelCtx.channel(), maxFileSize, logChannelCtx.path());
        logChannelCtx.channel().position(0);
        LogFormat.writeLogHeader(logChannelCtx.channel(), logHeader, memoryTracker);
        logChannelCtx.channel().flush(); // ensure header and metadata is flushed to disk
        logChannelCtx.channel().position(segmentBlockSize);
        logHeaderCache.cache(logHeader);
        return logChannelCtx;
    }

    private LogChannelContext<StoreChannel> openWriteChannel(long version, long position) throws IOException {
        var logChannelCtx = logsRepository.openWriteChannel(version);
        position = position == 0 ? segmentBlockSize : position;
        logChannelCtx.channel().position(position);
        return logChannelCtx;
    }

    private EnvelopeWriteChannel envelopedWriteChannel(
            LogChannelContext<StoreChannel> logChannelCtx, int checksumAtPosition, long prevIndex, long prevTerm)
            throws IOException {
        return new EnvelopeWriteChannel(
                logChannelCtx.channel(),
                scoopedBuffer(),
                segmentBlockSize,
                checksumAtPosition,
                prevIndex,
                prevTerm,
                logTracers,
                logRotation);
    }

    private HeapScopedBuffer scoopedBuffer() {
        return new HeapScopedBuffer(writerBufferedBlocks * segmentBlockSize, ByteOrder.LITTLE_ENDIAN, memoryTracker);
    }

    public EnvelopeReadChannel envelopedReadChannel(
            LogChannelContext<StoreChannel> logChannelCtx, boolean keepChannelOpenOnClose) throws IOException {
        LogHeader logHeader = readLogHeader(logChannelCtx);
        LogVersionedStoreChannel logVersionedChannel = logVersionedChannel(logChannelCtx, logHeader);
        if (keepChannelOpenOnClose) {
            logVersionedChannel = new UnclosableChannel(logVersionedChannel);
        }
        return new EnvelopeReadChannel(
                logVersionedChannel,
                logHeader.getSegmentBlockSize(),
                new EnvelopedLogVersionBridge(this),
                memoryTracker,
                false);
    }

    private LogHeader readLogHeader(LogChannelContext<StoreChannel> logChannelCtx) throws IOException {
        LogHeader header =
                LogHeaderReader.readLogHeader(logChannelCtx.channel(), true, logChannelCtx.path(), memoryTracker);
        if (header == null) {
            long position = logChannelCtx.channel().position();
            logChannelCtx.close();
            // Either there was nothing at all, or we read one byte and saw that it was a preallocated file.
            throw new IncompleteLogHeaderException(logChannelCtx.path(), (int) position, -1);
        }
        return header;
    }

    private PhysicalLogVersionedStoreChannel logVersionedChannel(
            LogChannelContext<StoreChannel> logChannelCtx, LogHeader header) throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                logChannelCtx.channel(),
                header.getLogVersion(),
                header.getLogFormatVersion(),
                logChannelCtx.path(),
                channelNativeAccessor,
                logTracers);
    }

    private LogVersionedStoreChannel safeOpenChannel(long version) throws IOException {
        var longRange = logsRepository.logVersionsRange();
        if (longRange.isWithinRange(version)) {
            LogChannelContext<StoreChannel> logChannelCtx = logsRepository.openReadChannel(version);
            LogHeader logHeader = readLogHeader(logChannelCtx);
            return logVersionedChannel(logChannelCtx, logHeader);
        }
        return null;
    }

    public LogFilesMetadata logFilesMetadata() throws IOException {
        return logFilesMetadata(false);
    }

    public LogFilesMetadata logFilesMetadata(boolean reversed) throws IOException {
        return new LogFilesMetadata(logsRepository, logHeaderCache, reversed);
    }

    public void remove() throws IOException {
        close();
        clearCache();
        logsRepository.deleteLogFilesFrom(0);
    }

    public StoreChannelsForTransfer storeChannels(long fromIndex, long toIndex) throws IOException {
        if (toIndex < fromIndex) {
            throw new IllegalArgumentException(
                    String.format("From index %d is higher than to index %d", fromIndex, toIndex));
        }

        long toFileVersion = getFileVersion(toIndex + 1);
        if (toFileVersion == -1) {
            throw new NoSuchFileException(
                    "No log files containing toIndex " + toIndex + " found because they have been pruned.");
        }

        var storeChannels = new ArrayList<StoreChannel>();
        try {
            long toPosition;
            var toChannelCtx = logsRepository.openReadChannel(toFileVersion);
            storeChannels.add(toChannelCtx.channel());

            try (var envToChannel = envelopedReadChannel(toChannelCtx, true)) {
                if (envToChannel.logHeader().getLastAppendIndex() == toIndex) {
                    toPosition = envToChannel.alignWithStartEntry();
                } else {
                    envToChannel.goToEntry(toIndex);
                    toPosition = envToChannel.goToEndOfEntry().getByteOffset();
                }

                // If fromIndex is in the same file, we position the channel and we're done with the logic
                if (envToChannel.logHeader().getLastAppendIndex() < fromIndex) {
                    var position = envToChannel.goToEntry(fromIndex);
                    toChannelCtx.channel().position(position);
                    return new StoreChannelsForTransfer(
                            storeChannels, toPosition, fromIndex, toIndex, segmentBlockSize);
                }
                // Otherwise, toChannel starts at its log header's data start
                toChannelCtx
                        .channel()
                        .position(envToChannel.logHeader().getStartPosition().getByteOffset());
            }

            // Find and position the start channel
            var fromVersion = getFileVersion(fromIndex);
            if (fromVersion == -1) {
                throw new NoSuchFileException("No log file found for from index " + fromIndex);
            }

            var fromChannelCtx = logsRepository.openReadChannel(fromVersion);
            storeChannels.addFirst(fromChannelCtx.channel());
            try (var envFromChannel = envelopedReadChannel(fromChannelCtx, true)) {
                fromChannelCtx.channel().position(envFromChannel.goToEntry(fromIndex));
            }

            // this means the last file is position to the start point and unnecessary to send
            if (toPosition == toChannelCtx.channel().position()) {
                toChannelCtx.close();
                storeChannels.remove(toChannelCtx.channel());
                toFileVersion--;
                if (toFileVersion == fromVersion) {
                    toPosition = fromChannelCtx.channel().size();
                } else {
                    toChannelCtx = logsRepository.openReadChannel(toFileVersion);
                    storeChannels.add(toChannelCtx.channel());
                    toPosition = toChannelCtx.channel().size();
                    toChannelCtx.channel().position(segmentBlockSize);
                }
            }

            // Fill in the gaps between fromVersion and toVersion
            for (var v = toFileVersion - 1; v > fromVersion; v--) {
                var midChannel = logsRepository.openReadChannel(v).channel();
                if (midChannel.size() <= segmentBlockSize) {
                    // less than should not be possible. In any case. Files may be empty due to truncation events.
                    // There is no reason to include these for transfer.
                    continue;
                }
                storeChannels.add(1, midChannel); // Maintain order: [from, ..., to]
                midChannel.position(segmentBlockSize);
            }

            return new StoreChannelsForTransfer(storeChannels, toPosition, fromIndex, toIndex, segmentBlockSize);
        } catch (Exception e) {
            IOUtils.closeAllSilently(storeChannels);
            throw e;
        }
    }

    private static class EnvelopedLogRotation implements LogRotation {
        private final EnvelopedLogFiles envelopedLogFiles;
        private final long maxFileSize;

        EnvelopedLogRotation(EnvelopedLogFiles envelopedLogFiles, long maxFileSize) {
            this.envelopedLogFiles = envelopedLogFiles;
            this.maxFileSize = maxFileSize;
        }

        @Override
        public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public boolean locklessBatchedRotateLogIfNeeded(
                LogRotateEvents logRotateEvents,
                long lastAppendIndex,
                KernelVersion kernelVersion,
                int checksum,
                LogFormat logFormat) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            return rotateLogIfNeeded(logRotateEvents);
        }

        @Override
        public boolean locklessRotateLogIfNeeded(
                LogRotateEvents logRotateEvents, KernelVersion kernelVersion, boolean force) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public void rotateLogFile(LogRotateEvents logRotateEvents) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public void locklessRotateLogFile(
                LogRotateEvents logRotateEvents, long lastAppendIndex, int previousChecksum, long lastTerm)
                throws IOException {
            try (var event = logRotateEvents.beginLogRotate()) {
                envelopedLogFiles.rotateCurrentFile(lastAppendIndex, previousChecksum, lastTerm);
                event.rotationCompleted(0); // TODO add clock
            }
        }

        @Override
        public void locklessRotateLogFile(
                LogRotateEvents logRotateEvents,
                KernelVersion kernelVersion,
                long lastAppendIndex,
                int previousChecksum) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public void locklessRotateLogFile(
                LogRotateEvents logRotateEvents,
                KernelVersion kernelVersion,
                long lastAppendIndex,
                int previousChecksum,
                LogFormat logFormat) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public long rotationSize() {
            return maxFileSize;
        }
    }

    private static class EnvelopedLogVersionBridge implements LogVersionBridge {
        private final EnvelopedLogFiles envelopedLogFiles;

        public EnvelopedLogVersionBridge(EnvelopedLogFiles envelopedLogFiles) {

            this.envelopedLogFiles = envelopedLogFiles;
        }

        @Override
        public LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) throws IOException {
            var nextChannel = envelopedLogFiles.safeOpenChannel(channel.getLogVersion() + 1);
            if (nextChannel != null) {
                channel.close();
                return nextChannel;
            }
            return channel;
        }
    }
}
