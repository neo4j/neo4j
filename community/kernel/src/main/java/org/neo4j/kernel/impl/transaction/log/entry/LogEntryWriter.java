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

import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newChunkStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newRollbackEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_END;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_ROLLBACK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkEnd;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.util.VisibleForTesting;

public class LogEntryWriter<T extends WritableChannel> {
    protected final T channel;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private LogEntrySerializationSet logEntrySerializationSet;
    private KernelVersion currentVersion;

    public LogEntryWriter(T channel, BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this.channel = channel;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
    }

    public void writeStartEntry(
            KernelVersion kernelVersion,
            long timeWritten,
            long latestCommittedTxWhenStarted,
            long appendIndex,
            long transactionSequenceNumber,
            int previousChecksum,
            byte[] additionalHeaderData)
            throws IOException {
        updateSerializationSet(kernelVersion);

        logEntrySerializationSet
                .select(TX_START)
                .write(
                        channel,
                        newStartEntry(
                                kernelVersion,
                                timeWritten,
                                latestCommittedTxWhenStarted,
                                appendIndex,
                                transactionSequenceNumber,
                                previousChecksum,
                                additionalHeaderData));
    }

    public void writeStartEntry(LogEntryStart startEntry) throws IOException {
        updateSerializationSet(startEntry.kernelVersion());

        logEntrySerializationSet.select(TX_START).write(channel, startEntry);
    }

    public void writeChunkStartEntry(
            KernelVersion kernelVersion,
            long timeWritten,
            long chunkId,
            long appendIndex,
            long previousBatchAppendIndex,
            long transactionSequenceNumber,
            byte[] additionalHeader)
            throws IOException {
        updateSerializationSet(kernelVersion);

        logEntrySerializationSet
                .select(CHUNK_START)
                .write(
                        channel,
                        newChunkStartEntry(
                                kernelVersion,
                                timeWritten,
                                chunkId,
                                appendIndex,
                                previousBatchAppendIndex,
                                transactionSequenceNumber,
                                additionalHeader));
    }

    public int writeChunkEndEntry(KernelVersion kernelVersion, long transactionId, long chunkId) throws IOException {
        updateSerializationSet(kernelVersion);

        return logEntrySerializationSet
                .select(CHUNK_END)
                .write(channel, new LogEntryChunkEnd(kernelVersion, transactionId, chunkId, 0));
    }

    public int writeRollbackEntry(
            KernelVersion kernelVersion,
            long transactionId,
            long appendIndex,
            long chunkId,
            long timeWritten,
            long transactionSequenceNumber,
            long lastBatchAppendIndex)
            throws IOException {
        updateSerializationSet(kernelVersion);

        return logEntrySerializationSet
                .select(TX_ROLLBACK)
                .write(
                        channel,
                        newRollbackEntry(
                                kernelVersion,
                                transactionId,
                                appendIndex,
                                chunkId,
                                timeWritten,
                                transactionSequenceNumber,
                                lastBatchAppendIndex));
    }

    public int writeCommitEntry(KernelVersion kernelVersion, long transactionId, long timeWritten) throws IOException {
        updateSerializationSet(kernelVersion);

        return logEntrySerializationSet
                .select(TX_COMMIT)
                .write(channel, newCommitEntry(kernelVersion, transactionId, timeWritten, 0));
    }

    public void serialize(CommandBatch batch) throws IOException {
        serialize(batch, batch.kernelVersion());
    }

    public void serialize(CommittedCommandBatchRepresentation commandBatch) throws IOException {
        commandBatch.serialize(this);
    }

    public void serialize(Iterable<StorageCommand> commands, KernelVersion kernelVersion) throws IOException {
        updateSerializationSet(kernelVersion);
        logEntrySerializationSet.serialize(channel, commands, kernelVersion);
    }

    public void serialize(StorageCommand command, KernelVersion kernelVersion) throws IOException {
        updateSerializationSet(kernelVersion);
        LogEntrySerializer.writeLogEntryHeader(kernelVersion, COMMAND, channel);
        assert command.kernelVersion() == kernelVersion
                : "Command serialization KernelVersion %s does not match tx KernelVersion %s"
                        .formatted(command.kernelVersion(), kernelVersion);
        command.serialize(channel);
    }

    public void writeBatchStart(
            KernelVersion kernelVersion,
            CommandBatch batch,
            long chunkId,
            long appendIndex,
            int previousChecksum,
            long previousBatchAppendIndex,
            long transactionSequenceNumber)
            throws IOException {
        if (batch.isFirst()) {
            writeStartEntry(
                    kernelVersion,
                    batch.getTimeStarted(),
                    batch.getLatestCommittedTxWhenStarted(),
                    appendIndex,
                    transactionSequenceNumber,
                    previousChecksum,
                    encodeLogIndex(batch.consensusIndex()));
        } else {
            writeChunkStartEntry(
                    kernelVersion,
                    batch.getTimeCommitted(),
                    chunkId,
                    appendIndex,
                    previousBatchAppendIndex,
                    transactionSequenceNumber,
                    encodeLogIndex(batch.consensusIndex()));
        }
    }

    public int writeBatchEnd(KernelVersion kernelVersion, CommandBatch batch, long transactionId, long chunkId)
            throws IOException {
        if (batch.isLast()) {
            return writeCommitEntry(kernelVersion, transactionId, batch.getTimeCommitted());
        } else {
            return writeChunkEndEntry(kernelVersion, transactionId, chunkId);
        }
    }

    @VisibleForTesting
    public T getChannel() {
        return channel;
    }

    private void updateSerializationSet(KernelVersion version) {
        if (version != currentVersion) {
            logEntrySerializationSet = serializationSet(version, binarySupportedKernelVersions);
            currentVersion = version;
        }
    }
}
