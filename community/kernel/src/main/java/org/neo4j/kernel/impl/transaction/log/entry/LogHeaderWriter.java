/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

import java.io.IOException;
import java.nio.ByteOrder;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreIdSerialization;

/**
 * Write the transaction log file header.
 *
 * Current format is
 * <pre>
 *  log version    7 bytes
 *  log format     1 bytes
 *  last committed 8 bytes
 *  store id       64 bytes
 *  reserved       48 bytes
 * </pre>
 *
 * Byte order is big-endian
 */
public class LogHeaderWriter {
    static final long LOG_VERSION_BITS = 56;
    static final long LOG_VERSION_MASK = 0x00FF_FFFF_FFFF_FFFFL;

    private LogHeaderWriter() {}

    public static void writeLogHeader(StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        try (var scopedBuffer =
                new HeapScopedBuffer(CURRENT_FORMAT_LOG_HEADER_SIZE, ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            buffer.putLong(encodeLogVersion(logHeader.getLogVersion(), logHeader.getLogFormatVersion()));
            buffer.putLong(logHeader.getLastCommittedTxId());
            StoreIdSerialization.serializeWithFixedSize(logHeader.getStoreId(), buffer);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.putLong(0 /* reserved */);
            buffer.flip();
            channel.writeAll(buffer);
        }
    }

    public static long encodeLogVersion(long logVersion, long formatVersion) {
        return (logVersion & LOG_VERSION_MASK) | (formatVersion << LOG_VERSION_BITS);
    }
}
