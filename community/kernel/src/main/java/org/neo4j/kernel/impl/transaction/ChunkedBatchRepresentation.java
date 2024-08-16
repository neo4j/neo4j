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
package org.neo4j.kernel.impl.transaction;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_CHUNK_NUMBER;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.common.Subject;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStartV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public record ChunkedBatchRepresentation(
        LogEntryChunkStart chunkStart, CommandBatch commandBatch, LogEntryChunkEnd chunkEnd)
        implements CommittedCommandBatchRepresentation {

    public static ChunkedBatchRepresentation createChunkRepresentation(
            LogEntry start, List<StorageCommand> commands, LogEntry end) {
        LogEntryChunkStart logEntryChunkStart = createChunkStart(start);
        LogEntryChunkEnd logEntryChunkEnd = createChunkEnd(end, logEntryChunkStart);
        ChunkMetadata chunkMetadata = new ChunkMetadata(
                start instanceof LogEntryStart,
                end instanceof LogEntryCommit,
                false,
                logEntryChunkStart.getPreviousBatchAppendIndex(),
                logEntryChunkStart.getChunkId(),
                new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                new MutableLong(logEntryChunkStart.getAppendIndex()),
                logEntryChunkStart.getTimeWritten(),
                -1,
                logEntryChunkStart.getTimeWritten(),
                -1,
                logEntryChunkStart.kernelVersion(),
                Subject.AUTH_DISABLED);
        return new ChunkedBatchRepresentation(
                logEntryChunkStart, new ChunkedCommandBatch(commands, chunkMetadata), logEntryChunkEnd);
    }

    @Override
    public int serialize(LogEntryWriter<? extends WritableChannel> writer) throws IOException {
        KernelVersion kernelVersion = chunkStart.kernelVersion();
        writer.writeChunkStartEntry(
                kernelVersion,
                chunkStart.getTimeWritten(),
                chunkStart.getChunkId(),
                chunkStart.getAppendIndex(),
                chunkStart.getPreviousBatchAppendIndex());
        writer.serialize(commandBatch);
        return writer.writeChunkEndEntry(kernelVersion, chunkEnd.getTransactionId(), chunkEnd.getChunkId());
    }

    @Override
    public long appendIndex() {
        return chunkStart.getAppendIndex();
    }

    @Override
    public int checksum() {
        return chunkEnd.getChecksum();
    }

    @Override
    public long timeWritten() {
        return chunkStart.getTimeWritten();
    }

    @Override
    public long txId() {
        return chunkEnd.getTransactionId();
    }

    @Override
    public boolean isRollback() {
        return false;
    }

    @Override
    public long previousBatchAppendIndex() {
        return chunkStart.getPreviousBatchAppendIndex();
    }

    private static LogEntryChunkStart createChunkStart(LogEntry start) {
        if (start instanceof LogEntryChunkStart chunkStart) {
            return chunkStart;
        } else if (start instanceof LogEntryStart entryStart) {
            return new LogEntryChunkStartV5_20(
                    entryStart.kernelVersion(),
                    entryStart.getTimeWritten(),
                    BASE_CHUNK_NUMBER,
                    entryStart.getAppendIndex(),
                    UNKNOWN_APPEND_INDEX);
        } else {
            throw new IllegalArgumentException("Was expecting start record. Actual entry: " + start);
        }
    }

    private static LogEntryChunkEnd createChunkEnd(LogEntry end, LogEntryChunkStart chunkStart) {
        if (end instanceof LogEntryChunkEnd endChunk) {
            return endChunk;
        } else if (end instanceof LogEntryCommit commit) {
            return new LogEntryChunkEnd(
                    chunkStart.kernelVersion(), commit.getTxId(), chunkStart.getChunkId(), commit.getChecksum());
        } else {
            throw new IllegalArgumentException("Was expecting end record. Actual entry: " + end);
        }
    }
}
