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
package org.neo4j.wal.enveloped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.LogTracers;
import org.neo4j.wal.LogVersionBridge;
import org.neo4j.wal.LogVersionedStoreChannel;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.UnclosableChannel;
import org.neo4j.wal.entry.IncompleteLogHeaderException;
import org.neo4j.wal.entry.LogEnvelopeHeader;
import org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.entry.LogHeaderReader;
import org.neo4j.wal.pruning.LogPruneThreshold;
import org.neo4j.wal.rotation.LogRotateEvents;
import org.neo4j.wal.rotation.LogRotation;

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
                logTracers,
                new EnvelopedLogHeaderCacheImpl());
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
            InternalLogProvider logProvider,
            LogTracers logTracers,
            EnvelopedLogHeaderCache logHeaderCache) {
        if (totalSegments < MINIMUM_SEGMENTS) {
            throw new IllegalArgumentException(
                    String.format("Must have at least %d segments. Got %d", MINIMUM_SEGMENTS, totalSegments));
        }
        this.channelNativeAccessor = channelNativeAccessor;
        this.logHeaderFactory = logHeaderFactory;
        this.logsRepository = logsRepository;
        this.logHeaderCache = logHeaderCache;
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
                LogTracers.NULL, // TODO MERGELOGS LogTracers currently only used in tests
                new EnvelopedLogHeaderCacheImpl());
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
        logsRepository.deleteTempFileIfPresent();
        return recoverLogTail(0, true);
    }

    public void populateCache() throws IOException {
        try (var metadata = new LogFilesMetadata(logsRepository)) {
            while (metadata.next()) {
                var logHeader = metadata.get().logHeader();
                logHeaderCache.cache(logHeader);
            }
        }
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
    public long resetWriteChannelToLatestIndex(boolean allowStoreIdentifierChange) throws IOException {
        if (allowStoreIdentifierChange) {
            // TODO MERGELOG: It would be nicer if we didn't have to make the header factory mutable. The alternative
            //                is to create a completely different EnvelopedLogFiles object when there's a need to
            //                replace the store with a different one. However, this requires replacing references to
            //                EnvelopedLogFiles with some sort of holder/manager in many places, so is left to a
            //                followup.
            logHeaderFactory.allowStoreIdentifierChange();
        }
        long latestVersion = logsRepository.latestVersion();
        return latestVersion >= 0 ? recoverLogTail(latestVersion, false) : -1;
    }

    private long recoverLogTail(long fromVersion, boolean populateCache) throws IOException {
        var tailChecker = new EnvelopedLogTailChecker(
                logsRepository,
                logFileVersion -> envelopedReadChannel(logsRepository.openReadChannel(logFileVersion), false),
                memoryTracker,
                logProvider);
        var tailInfo = tailChecker.checkEnvelopedLogTail(fromVersion);
        if (tailInfo.kernelVersion() != null) {
            logHeaderFactory.setVersion(tailInfo.kernelVersion());
        }
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
            logHeaderFactory.setVersion(readChannel.logHeader().getKernelVersion());
            while (readChannel.entryIndex() < fromIndex) {
                // don't read values from the channel until it has definitely read an envelope header
                if (!envelopesRead) {
                    prevChecksum = readChannel.getChecksum();
                    prevTerm = readChannel.getTerm();
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
        var tail = readFlushedTailState();
        if (tail == null) {
            return;
        }
        log.info(
                "Recalibrating write-channel checksum-chain mirrors from on-disk tail: %s (fileVersion=%d, position=%d)",
                tail, currentWriteChannel.version(), appendingChannel.position());
        appendingChannel.recoverState(tail.checksum(), tail.index(), tail.term());
    }

    /**
     * Verifies that the write channel's tracked state matches the on-disk tail, without mutating it — the
     * read-only twin of {@link #recalibrateWriteChannel()}. Raw appends mirror channel state from the shipped
     * envelopes they parse; a divergence here means that mirroring broke, and the next rotation or local append
     * would seed from bad state.
     *
     * @throws IllegalStateException if the tracked state does not match the on-disk tail
     * @throws InvalidLogEnvelopeReadException if incomplete entries are found at the end of the log
     * @throws IOException if an I/O error occurs while reading the tail
     */
    public void verifyWriteChannelCalibration() throws IOException {
        var tail = readFlushedTailState();
        if (tail == null) {
            return;
        }
        if (tail.checksum() != appendingChannel.currentChecksum()
                || tail.index() != appendingChannel.currentIndex()
                || tail.term() != appendingChannel.currentTerm()) {
            var tracked = new TailState(
                    appendingChannel.currentIndex(),
                    appendingChannel.currentChecksum(),
                    appendingChannel.currentTerm());
            throw new IllegalStateException(
                    ("The write channel's tracked state has diverged from the on-disk tail (fileVersion=%d, "
                                    + "position=%d): %s vs tracked %s")
                            .formatted(currentWriteChannel.version(), appendingChannel.position(), tail, tracked));
        }
    }

    private record TailState(long index, int checksum, long term) {}

    /**
     * Reads the last complete entry's state from the flushed on-disk tail, scanning from the last segment
     * boundary, or {@code null} when the file holds no entry to compare against: nothing beyond the header
     * segment, or only the START_OFFSET filler a truncation leaves behind.
     *
     * @throws InvalidLogEnvelopeReadException if incomplete entries are found at the end of the log
     */
    private TailState readFlushedTailState() throws IOException {
        if (appendingChannel == null) {
            throw new IllegalStateException("Writer channel has not been initialised");
        }

        long currentPosition = appendingChannel.position();
        if (currentPosition <= segmentBlockSize) {
            return null;
        }

        // Start reading from the last segment boundary; when precisely aligned on one, go back a whole segment
        // to ensure there are entries to read.
        long segmentBoundary = (currentPosition / segmentBlockSize) * segmentBlockSize;
        if (segmentBoundary == currentPosition) {
            segmentBoundary -= segmentBlockSize;
        }

        long currentVersion = currentWriteChannel.version();
        try (var readChannel = envelopedReadChannel(logsRepository.openReadChannel(currentVersion), false)) {
            readChannel.position(segmentBoundary);

            // Read through all entries to validate the checksum chain
            while (true) {
                try {
                    readChannel.goToNextEntry();
                } catch (ReadPastEndException ignored) {
                    break;
                }
            }
            long lastValidPosition = readChannel.position();
            if (lastValidPosition < currentPosition) {
                throw new InvalidLogEnvelopeReadException(
                        "Incomplete entry found at the end of the log (fileVersion=" + currentVersion
                                + "). Last valid position: " + lastValidPosition
                                + ", current write position: " + currentPosition);
            }
            long tailIndex = readChannel.entryIndex();
            if (tailIndex == LogEnvelopeHeader.UNSPECIFIED_INDEX) {
                // A truncation opens its file with only the START_OFFSET filler, and the leader-transition hook can
                // run before any append refills it. The filler carries neither index nor term, so comparing against
                // its sentinels would fail a channel that legitimately tracks (fromIndex - 1, prevTerm).
                return null;
            }
            return new TailState(tailIndex, readChannel.getChecksum(), readChannel.getTerm());
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
        logsRepository.deleteTempFileIfPresent(); // discard a temp file left over from a crash
        var tempPath = logsRepository.tempPath();
        var logChannelCtx = logsRepository.createWriteChannelAt(tempPath, version);
        channelNativeAccessor.preallocateSpace(logChannelCtx.channel(), maxFileSize, tempPath);
        logChannelCtx.channel().position(0);
        LogFormat.writeLogHeader(logChannelCtx.channel(), logHeader, memoryTracker);
        logChannelCtx.channel().force(true); // persist header and file size before the file becomes visible
        logsRepository.publishTempFile(version); // atomic rename so a reader never sees a header-less file
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
        LogVersionedStoreChannel logVersionedChannel = logVersionedChannel(logChannelCtx, logHeader, true);
        if (keepChannelOpenOnClose) {
            logVersionedChannel = new UnclosableChannel(logVersionedChannel);
        }
        return new EnvelopeReadChannel(
                logVersionedChannel,
                logHeader.getSegmentBlockSize(),
                keepChannelOpenOnClose
                        ? new KeepChannelOpenEnvelopedLogVersionBridge(this)
                        : new EnvelopedLogVersionBridge(this),
                memoryTracker,
                true);
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
            LogChannelContext<StoreChannel> logChannelCtx, LogHeader header, boolean skipCacheAdvice)
            throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                logChannelCtx.channel(),
                header.getLogVersion(),
                header.getLogFormatVersion(),
                logChannelCtx.path(),
                channelNativeAccessor,
                logTracers,
                skipCacheAdvice);
    }

    private LogVersionedStoreChannel safeOpenChannel(long version, boolean skipCacheAdvice) throws IOException {
        var longRange = logsRepository.logVersionsRange();
        if (longRange.isWithinRange(version)) {
            LogChannelContext<StoreChannel> logChannelCtx = logsRepository.openReadChannel(version);
            LogHeader logHeader = readLogHeader(logChannelCtx);
            return logVersionedChannel(logChannelCtx, logHeader, skipCacheAdvice);
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

    /** Channels over the pure entry stream, per the {@link EnvelopeLogRangeReader#entryStreamChannels} contract. */
    public StoreChannelsForTransfer entryStreamChannels(long fromIndex, long toIndex) throws IOException {
        if (toIndex < fromIndex) {
            throw new IllegalArgumentException(
                    String.format("From index %d is higher than to index %d", fromIndex, toIndex));
        }

        long toFileVersion = getFileVersion(toIndex);
        if (toFileVersion == -1) {
            throw new NoSuchFileException(
                    "No log files containing toIndex " + toIndex + " found because they have been pruned.");
        }

        var storeChannels = new ArrayList<StoreChannel>();
        try {
            long toPosition;
            try (var envToChannel = envelopedReadChannel(logsRepository.openReadChannel(toFileVersion), true)) {
                envToChannel.goToEntry(toIndex);
                toPosition = envToChannel.goToEndOfEntry().getByteOffset();
                toFileVersion = envToChannel.getLogVersion();
                var channelToAdd = ensureClosable(envToChannel.channel());
                storeChannels.add(channelToAdd);
                var logHeader = envToChannel.logHeader();

                // If fromIndex is in the same file, we position the channel and we're done with the logic
                if (logHeader.getLastAppendIndex() < fromIndex) {
                    var position = envToChannel.goToEntry(fromIndex);
                    channelToAdd.position(position);
                    return new StoreChannelsForTransfer(
                            storeChannels, toPosition, fromIndex, toIndex, segmentBlockSize);
                }
                // Otherwise, toChannel starts at its log header's data start
                channelToAdd.position(logHeader.getStartPosition().getByteOffset());
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

            // Fill in the gaps between fromVersion and toVersion
            for (var v = toFileVersion - 1; v > fromVersion; v--) {
                var midChannel = logsRepository.openReadChannel(v).channel();
                if (midChannel.size() <= segmentBlockSize) {
                    // less than should not be possible. In any case. Files may be empty due to truncation events.
                    // There is no reason to include these for transfer.
                    midChannel.close();
                    continue;
                }
                storeChannels.add(1, midChannel); // Maintain order: [from, ..., to]
                midChannel.position(segmentBlockSize);
            }

            // The first channel is at an entry already (goToEntry lands past any START_OFFSET filler);
            // follow-on channels sit at their file's first data segment and may need the skip.
            for (var i = 1; i < storeChannels.size(); i++) {
                skipLeadingStartOffset(storeChannels.get(i));
            }

            return new StoreChannelsForTransfer(storeChannels, toPosition, fromIndex, toIndex, segmentBlockSize);
        } catch (Exception e) {
            IOUtils.closeAllSilently(storeChannels);
            throw e;
        }
    }

    /**
     * A file created by truncation opens its first data segment with a START_OFFSET filler standing in for the
     * bytes that remained in the predecessor's last segment. It is local layout, not entry data, so the entry
     * stream must start past it; its size then reappears as the channel's intra-segment offset (the filler is
     * always smaller than a segment, so the skip never crosses a segment boundary).
     */
    private static void skipLeadingStartOffset(StoreChannel channel) throws IOException {
        long position = channel.position();
        var header = ByteBuffer.allocate(LogEnvelopeHeader.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        while (header.hasRemaining() && channel.read(header) >= 0) {
            // EOF before a full header just means no filler — a file this short cannot start with one
        }
        if (header.hasRemaining()
                || header.get(LogEnvelopeHeader.OFFSET_ENVELOPE_TYPE) != EnvelopeType.START_OFFSET.typeValue) {
            channel.position(position);
            return;
        }
        channel.position(
                position + LogEnvelopeHeader.HEADER_SIZE + header.getInt(LogEnvelopeHeader.OFFSET_PAYLOAD_LENGTH));
    }

    public LogHeaderFactory headerFactory() {
        return this.logHeaderFactory;
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
                long lastTerm,
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
                long lastTerm,
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
            var nextChannel = envelopedLogFiles.safeOpenChannel(channel.getLogVersion() + 1, raw);
            if (nextChannel != null) {
                channel.close();
                return nextChannel;
            }
            return channel;
        }
    }

    private static class KeepChannelOpenEnvelopedLogVersionBridge implements LogVersionBridge {
        private final EnvelopedLogFiles envelopedLogFiles;

        public KeepChannelOpenEnvelopedLogVersionBridge(EnvelopedLogFiles envelopedLogFiles) {
            this.envelopedLogFiles = envelopedLogFiles;
        }

        @Override
        public LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) throws IOException {
            var nextChannel = envelopedLogFiles.safeOpenChannel(channel.getLogVersion() + 1, raw);
            if (nextChannel != null) {
                ((UnclosableChannel) channel).toClosable().close();
                return new UnclosableChannel(nextChannel);
            }
            return channel;
        }
    }

    private static LogVersionedStoreChannel ensureClosable(LogVersionedStoreChannel channel) {
        if (channel instanceof UnclosableChannel unclosableChannel) {
            return unclosableChannel.toClosable();
        }
        return channel;
    }
}
