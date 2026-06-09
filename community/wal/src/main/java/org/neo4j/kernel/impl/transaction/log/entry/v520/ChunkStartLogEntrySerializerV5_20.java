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
import org.neo4j.kernel.impl.transaction.log.entry.BadLogEntryException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class ChunkStartLogEntrySerializerV5_20 extends LogEntrySerializer<LogEntryChunkStart> {
    public ChunkStartLogEntrySerializerV5_20() {
        super(LogEntryTypeCodes.CHUNK_START);
    }

    @Override
    public LogEntryChunkStart parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        long timeWritten = channel.getLong();
        long chunkId = channel.getLong();
        long previousBatchAppendIndex = channel.getLong();
        long appendIndex = channel.getAppendIndex();
        long transactionSequenceNumber = channel.getLong();
        int additionalHeaderLength = channel.getInt();
        if (additionalHeaderLength > LogEntryStart.MAX_ADDITIONAL_HEADER_SIZE) {
            throw new BadLogEntryException("Additional header length limit(" + LogEntryStart.MAX_ADDITIONAL_HEADER_SIZE
                    + ") exceeded. Parsed length is " + additionalHeaderLength);
        }
        byte[] additionalHeader = new byte[additionalHeaderLength];
        channel.get(additionalHeader, additionalHeaderLength);
        return new LogEntryChunkStart(
                version,
                timeWritten,
                chunkId,
                appendIndex,
                previousBatchAppendIndex,
                transactionSequenceNumber,
                additionalHeader);
    }

    @Override
    public int write(WritableChannel channel, LogEntryChunkStart logEntry) throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), CHUNK_START, channel);
        byte[] additionalHeaderData = logEntry.getAdditionalHeader();
        channel.putLong(logEntry.getTimeWritten())
                .putLong(logEntry.getChunkId())
                .putLong(logEntry.getPreviousBatchAppendIndex())
                .putAppendIndex(logEntry.getAppendIndex())
                .putLong(logEntry.getTransactionSequenceNumber());
        channel.putInt(additionalHeaderData.length).put(additionalHeaderData, additionalHeaderData.length);
        return NO_RETURN_VALUE;
    }
}
