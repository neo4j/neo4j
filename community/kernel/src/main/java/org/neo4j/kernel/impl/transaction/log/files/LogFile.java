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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvents;

/**
 * Sees a log file as bytes, including taking care of rotation of the file into optimal chunks.
 */
public interface LogFile extends RotatableFile {
    @FunctionalInterface
    interface LogFileVisitor {
        boolean visit(ReadableLogPositionAwareChannel channel) throws IOException;
    }

    /**
     * @return transaction writer capable to append transaction representation into transaction log
     */
    TransactionLogWriter getTransactionLogWriter();

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

    /**
     * Opens a {@link ReadableLogChannel reader} at the desired {@link LogPosition}, capable of reading log entries
     * from that position and onwards, with the given {@link LogVersionBridge}.
     *
     * @param position {@link LogPosition} to position the returned reader at.
     * @param logVersionBridge {@link LogVersionBridge} how to bridge log versions.
     * @return {@link ReadableChannel} capable of reading log data, starting from {@link LogPosition position}.
     * @throws IOException on I/O error.
     */
    ReadableLogChannel getReader(LogPosition position, LogVersionBridge logVersionBridge) throws IOException;

    void accept(LogFileVisitor visitor, LogPosition startingFromPosition) throws IOException;

    TransactionLogFileInformation getLogFileInformation();

    PhysicalLogVersionedStoreChannel openForVersion(long version) throws IOException;

    PhysicalLogVersionedStoreChannel openForVersion(long version, boolean raw) throws IOException;

    PhysicalLogVersionedStoreChannel createLogChannelForVersion(
            long version,
            LongSupplier lastApplyIndexSupplier,
            KernelVersionProvider kernelVersionProvider,
            int previousLogFileChecksum)
            throws IOException;

    PhysicalLogVersionedStoreChannel createLogChannelForExistingVersion(long version) throws IOException;

    long getLogVersion(Path file);

    Path getLogFileForVersion(long version);

    Path getHighestLogFile();

    long getHighestLogVersion();

    long getCurrentLogVersion();

    long getLowestLogVersion();

    LogHeader extractHeader(long version) throws IOException;

    boolean versionExists(long version);

    boolean hasAnyEntries(long version);

    void accept(LogVersionVisitor visitor);

    void accept(LogHeaderVisitor visitor) throws IOException;

    Path[] getMatchedFiles() throws IOException;

    void combine(Path additionalSource) throws IOException;

    boolean forceAfterAppend(LogForceEvents logForceEvents) throws IOException;

    void locklessForce(LogForceEvents logAppendEvents) throws IOException;

    void flush() throws IOException;

    void truncate() throws IOException;

    void truncate(LogPosition position) throws IOException;

    LogPosition append(
            ByteBuffer byteBuffer,
            OptionalLong appendIndex,
            Optional<Byte> kernelVersionByte,
            int checksum,
            long offset)
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
}
