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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.LOG_VERSION_BITS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.LOG_VERSION_MASK;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

public final class LogHeaderReader {
    private LogHeaderReader() {}

    public static LogHeader readLogHeader(FileSystemAbstraction fileSystem, Path file, MemoryTracker memoryTracker)
            throws IOException {
        return readLogHeader(fileSystem, file, true, memoryTracker);
    }

    public static LogHeader readLogHeader(
            FileSystemAbstraction fileSystem, Path file, boolean strict, MemoryTracker memoryTracker)
            throws IOException {
        try (StoreChannel channel = fileSystem.read(file)) {
            return readLogHeader(channel, strict, file, memoryTracker);
        }
    }

    /**
     * Reads the header of a log.
     *
     * @param channel {@link ReadableByteChannel} to read from, typically a channel over a file containing the data.
     * @param strict if {@code true} then will fail with {@link IncompleteLogHeaderException} on incomplete
     * header, i.e. if there's not enough data in the channel to even read the header. If {@code false} then
     * the return value will instead be {@code null}.
     * @param additionalErrorInformation when in {@code strict} mode the exception can be
     * amended with information about which file the channel represents, if any. Purely for better forensics
     * ability.
     * @param memoryTracker memory tracker
     * @return {@link LogHeader} containing the log header data from the {@code channel}.
     * @throws IOException if unable to read from {@code channel}
     * @throws IncompleteLogHeaderException if {@code strict} and not enough data could be read
     */
    public static LogHeader readLogHeader(
            ReadableByteChannel channel, boolean strict, Path additionalErrorInformation, MemoryTracker memoryTracker)
            throws IOException {
        // log header has big-endian byte order
        try (var scopedBuffer = new NativeScopedBuffer(LogFormat.BIGGEST_HEADER, ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            return readLogHeader(buffer, channel, strict, additionalErrorInformation);
        }
    }

    private static LogHeader readLogHeader(
            ByteBuffer buffer,
            ReadableByteChannel channel,
            boolean strict,
            Path fileForAdditionalErrorInformationOrNull)
            throws IOException {
        buffer.clear();
        buffer.limit(LogFormat.BIGGEST_HEADER);
        channel.read(buffer);
        buffer.flip();

        LogHeader logHeader = LogFormat.parseHeader(buffer, strict, fileForAdditionalErrorInformationOrNull);

        if (logHeader != null) {
            if (channel instanceof SeekableByteChannel seekableByteChannel) {
                seekableByteChannel.position(logHeader.getStartPosition().getByteOffset());
            }
        }

        return logHeader;
    }

    static long decodeLogVersion(long encLogVersion) {
        return encLogVersion & LOG_VERSION_MASK;
    }

    static byte decodeLogFormatVersion(long encLogVersion) {
        return (byte) ((encLogVersion >> LOG_VERSION_BITS) & 0xFF);
    }
}
