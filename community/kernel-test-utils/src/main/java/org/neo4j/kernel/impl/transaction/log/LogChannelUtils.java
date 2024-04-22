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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.StoreId;

public final class LogChannelUtils {

    private LogChannelUtils() {}

    /**
     * Get a writable channel to a log file for a given {@link KernelVersion}.
     * <p>
     * If segments are applicable, it will be {@link LogSegments#DEFAULT_LOG_SEGMENT_SIZE} in size.
     *
     * @param fs filesystem where the file lives.
     * @param path path to the log file.
     * @param kernelVersion targeted kernel version.
     * @return a writable channel.
     * @throws IOException on I/O error.
     */
    public static FlushableLogPositionAwareChannel getWriteChannel(
            FileSystemAbstraction fs, Path path, KernelVersion kernelVersion) throws IOException {
        var storeChannel = fs.open(path, Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        var logHeader = getLogHeader(kernelVersion);
        writeLogHeader(storeChannel, logHeader, INSTANCE);
        var logChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, logHeader, path, ChannelNativeAccessor.EMPTY_ACCESSOR, DatabaseTracer.NULL);
        return new PhysicalFlushableLogPositionAwareChannel(logChannel, logHeader, INSTANCE);
    }

    /**
     * Get a readable channel to a log file for a given {@link KernelVersion}.
     * <p>
     * If segments are applicable, it will be {@link LogSegments#DEFAULT_LOG_SEGMENT_SIZE} in size.
     *
     * @param fs filesystem where the file lives.
     * @param path path to the log file.
     * @param kernelVersion targeted kernel version.
     * @return a readable channel.
     * @throws IOException on I/O error.
     */
    public static ReadableLogPositionAwareChannel getReadChannel(
            FileSystemAbstraction fs, Path path, KernelVersion kernelVersion) throws IOException {
        var storeChannel = fs.open(path, Set.of(StandardOpenOption.READ));
        var logHeader = LogHeaderReader.readLogHeader(storeChannel, true, path, INSTANCE);
        validateKernelVersion(kernelVersion, logHeader);
        var logChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, logHeader, path, ChannelNativeAccessor.EMPTY_ACCESSOR, DatabaseTracer.NULL);

        return ReadAheadUtils.newChannel(logChannel, logHeader, INSTANCE);
    }

    private static void validateKernelVersion(KernelVersion kernelVersion, LogHeader logHeader) {
        if (logHeader.getLogFormatVersion().getVersionByte()
                != LogFormat.fromKernelVersion(kernelVersion).getVersionByte()) {
            throw new AssertionError("Expected log format version " + logHeader.getLogFormatVersion() + " but got "
                    + LogFormat.fromKernelVersion(kernelVersion).getVersionByte());
        }
    }

    private static LogHeader getLogHeader(KernelVersion kernelVersion) {
        LogFormat logFormat = LogFormat.fromKernelVersion(kernelVersion);
        return logFormat.newHeader(
                0, 0, BASE_APPEND_INDEX, StoreId.UNKNOWN, DEFAULT_LOG_SEGMENT_SIZE, BASE_TX_CHECKSUM, kernelVersion);
    }
}
