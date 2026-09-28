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
package org.neo4j.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.wal.enveloped.EnvelopeWriteChannel;
import org.neo4j.wal.enveloped.InvalidLogEnvelopeReadException;
import org.neo4j.wal.files.LogVersionVisitor;
import org.neo4j.wal.rotation.LogRotation;

/**
 * Sees a log file as bytes, including taking care of rotation of the file into optimal chunks.
 */
public interface LogFile extends VersionedFile, RotatableFile {
    @FunctionalInterface
    interface LogFileVisitor {
        boolean visit(ReadableLogPositionAwareChannel channel) throws IOException;
    }

    /**
     * @return transaction writer capable to append transaction representation into transaction log
     */
    LogWriter getTransactionLogWriter();

    /**
     * @return transaction log rotation
     */
    LogRotation getLogRotation();

    /**
     * Opens a {@link ReadableLogChannel reader} at the desired {@link LogPosition}, capable of reading log entries
     * from that position and onwards, through physical log versions.
     *
     * @param position {@link LogPosition} to position the returned reader at.
     * @return {@link ReadableChannel} capable of reading log data, starting from {@link LogPosition position}.
     * @throws IOException on I/O error.
     */
    ReadableLogChannel getReader(LogPosition position) throws IOException;

    /**
     * Opens a {@link ReadableLogChannel reader} at the desired {@link LogPosition}, capable of reading log entries
     * from that position and onwards, through physical log versions.
     * In comparison with alternative {{@link #getReader(LogPosition)}} reader acquired as result of this call will not gonna
     * perform any calls to pre-load, offload file and potentially some other optimisations.
     *
     * @param position {@link LogPosition} to position the returned reader at.
     * @return {@link ReadableChannel} capable of reading log data, starting from {@link LogPosition position}.
     * @throws IOException on I/O error.
     */
    ReadableLogChannel getRawReader(LogPosition position) throws IOException;

    void accept(LogFileVisitor visitor, LogPosition startingFromPosition) throws IOException;

    NativeScopedBuffer createScopedBuffer();

    PhysicalLogVersionedStoreChannel openForVersion(long version, boolean raw) throws IOException;

    PhysicalLogVersionedStoreChannel createLogChannelForVersion(
            long version,
            LongSupplier lastAppendIndexSupplier,
            KernelVersionProvider kernelVersionProvider,
            int previousLogFileChecksum,
            LogFormatVersionProvider logFormatVersionProvider,
            LogTermProvider logTermProvider)
            throws IOException;

    PhysicalLogVersionedStoreChannel createLogChannelForExistingVersion(long version) throws IOException;

    boolean versionExists(long version);

    boolean hasAnyEntries(long version);

    void accept(LogVersionVisitor visitor);

    void accept(LogHeaderVisitor visitor) throws IOException;

    void combine(Path additionalSource, long overLappingAppendIndex) throws IOException;

    boolean forceAfterAppend(LogForceEvents logForceEvents) throws IOException;

    void locklessForce(LogForceEvents logAppendEvents) throws IOException;

    void flush() throws IOException;

    /**
     * This utility method creates initial empty log file when no transaction log files are discovered during recovery
     * Implementations should work without extra preparations, i.e. starting LifeCycle`d component
     */
    void initializeMissingLogFile() throws IOException;

    void truncate() throws IOException;

    void truncate(LogPosition position) throws IOException;

    LogPosition append(
            ByteBuffer byteBuffer,
            OptionalLong appendIndex,
            Optional<Byte> kernelVersionByte,
            int checksum,
            long offset,
            Optional<Byte> logFormatVersion)
            throws IOException;

    /**
     * Register map of externally exposed readers. Key is log version number. Value is log reader.
     * @param internalChannels map of readers
     */
    void registerExternalReaders(LongObjectMap<StoreChannel> internalChannels);

    /**
     * Unregister externally exposed reader. If version is not registered or channel is not registered it will be just ignored
     * @param version version of log file to unregister reader
     * @param channel reader to unregister
     */
    void unregisterExternalReader(long version, StoreChannel channel);

    /**
     * Mass termination (unregistration and closing ) of externally exposed readers.
     * All registered readers up to (inclusive) specified version will be closed and unregistered.
     * @param maxDeletedVersion version up to terminate external readers.
     */
    void terminateExternalReaders(long maxDeletedVersion);

    /**
     * Delete the log file at the specified version, or throw an error if it does not exist.
     * @param version version of log file to delete
     * @throws IOException on I/O error.
     */
    void delete(Long version) throws IOException;

    /**
     * These two are really only for error handling with the rawTxPull protocol - see implementation for more details.
     */
    record PositionWithPrevAppendIndex(LogPosition position, long prevAppendIndexAtPosition) {}

    PositionWithPrevAppendIndex findSafeTruncationPointInPreviousFile(PositionWithPrevAppendIndex basePosition);

    long getLastEntryAppendIndexInLogFiles();

    long getMaxFileSize();

    EnvelopeWriteChannel currentWriteChannel();

    /**
     * Recalibrates the write channel state after directPutAll operations have detached it from its state.
     * This method reads through the entries from the last segment boundary to validate the checksum chain.
     * If the last entry is incomplete, this method will fail.
     *
     * @throws InvalidLogEnvelopeReadException if incomplete entries are found at the end of the log
     * @throws IOException if an I/O error occurs during recovery
     */
    void recalibrateWriteChannel() throws IOException;

    /**
     * Truncates the envelope log files after the last written entry.
     * No written entries are truncated, this should be called only
     * when we want to free the pre-allocated data in the log.
     */
    void forceRotate() throws IOException;

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
    void skip(long index, int checksum, int offset, long term) throws IOException;

    void updateWriteChannel(long index, int checksum, long index1, long term) throws IOException;

    /**
     * Truncates to the last safe entry position when log pulling fails unexpectedly.
     * Finds the last complete entry that ends at or before the last segment boundary
     * and truncates to that entry.
     *
     * @param knownSafeIndex the position to start searching from, 0 if there is none
     * @throws IOException if an I/O error occurs during truncation
     */
    void truncateToLastSafeEntry(long knownSafeIndex) throws IOException;

    /**
     * Can be used to reset the write channel to the end of the log, for example, after all the
     * log files have been replaced by a store copy. This method should not be used if there's a
     * chance that the log is corrupt.
     *
     * @return
     */
    long resetWriteChannelToLatestIndex(boolean allowStoreIdentifierChange) throws IOException;

    LogPosition currentWriteLogPosition() throws IOException;

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
    void verifyWriteChannelCalibration() throws IOException;
}
