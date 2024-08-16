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
package org.neo4j.kernel.impl.transaction.log.entry.v520;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_START;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class ChunkStartLogEntrySerializerV5_20 extends LogEntrySerializer<LogEntryChunkStartV5_20> {
    public ChunkStartLogEntrySerializerV5_20() {
        super(LogEntryTypeCodes.CHUNK_START);
    }

    @Override
    public LogEntryChunkStartV5_20 parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long timeWritten = channel.getLong();
        long chunkId = channel.getLong();
        long previousBatchAppendIndex = channel.getLong();
        long appendIndex = channel.getLong();
        return new LogEntryChunkStartV5_20(version, timeWritten, chunkId, appendIndex, previousBatchAppendIndex);
    }

    @Override
    public int write(WritableChannel channel, LogEntryChunkStartV5_20 logEntry) throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), CHUNK_START, channel);
        channel.putLong(logEntry.getTimeWritten())
                .putLong(logEntry.getChunkId())
                .putLong(logEntry.getPreviousBatchAppendIndex())
                .putLong(logEntry.getAppendIndex());
        return NO_RETURN_VALUE;
    }
}
