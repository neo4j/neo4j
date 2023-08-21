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

import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V8;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V9;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreIdSerialization;

/**
 * Write the transaction log file header.
 * <br>
 * Current format is
 * <pre>
 *  log version    7 bytes
 *  log format     1 bytes
 *  last committed 8 bytes
 *  store id       64 bytes
 *  segment block  4 bytes
 *  reserved       44 bytes
 * </pre>
 *
 * Byte order is big-endian
 */
public final class LogHeaderWriter {
    static final long LOG_VERSION_BITS = 56;
    static final long LOG_VERSION_MASK = 0x00FF_FFFF_FFFF_FFFFL;

    private LogHeaderWriter() {}

    public static void writeLogHeader(StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        if (logHeader.getLogFormatVersion() >= V9.getVersionByte()) {
            writeHeader9(channel, logHeader, memoryTracker);
        } else { // 5.0
            writeHeader8(channel, logHeader, memoryTracker);
        }
        channel.flush();
    }

    private static void writeHeader8(StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        try (var scopedBuffer = new NativeScopedBuffer(V8.getHeaderSize(), ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            putHeader(buffer, logHeader);

            buffer.flip();
            channel.writeAll(buffer);
        }
    }

    private static void writeHeader9(StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        try (var scopedBuffer =
                new NativeScopedBuffer(logHeader.getSegmentBlockSize(), ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            putHeader(buffer, logHeader);
            buffer.position(logHeader.getSegmentBlockSize()); // First segments is reserved
            buffer.flip();
            channel.writeAll(buffer);
        }
    }

    public static void putHeader(ByteBuffer buffer, LogHeader logHeader) throws IOException {
        ByteOrder originalOrder = buffer.order();
        try {
            // Log header uses big endian
            buffer.order(ByteOrder.BIG_ENDIAN);

            buffer.putLong(encodeLogVersion(logHeader.getLogVersion(), logHeader.getLogFormatVersion()));
            buffer.putLong(logHeader.getLastCommittedTxId());
            StoreIdSerialization.serializeWithFixedSize(logHeader.getStoreId(), buffer);

            if (logHeader.getLogFormatVersion() >= V9.getVersionByte()) {
                buffer.putInt(logHeader.getSegmentBlockSize());
                buffer.putInt(logHeader.getPreviousLogFileChecksum());
            } else {
                buffer.putLong(0 /* reserved */);
            }

            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
        } finally {
            buffer.order(originalOrder);
        }
    }

    public static long encodeLogVersion(long logVersion, long formatVersion) {
        return (logVersion & LOG_VERSION_MASK) | (formatVersion << LOG_VERSION_BITS);
    }
}
