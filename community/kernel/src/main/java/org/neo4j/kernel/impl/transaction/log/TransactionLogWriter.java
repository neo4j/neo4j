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

import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.util.VisibleForTesting;

public class TransactionLogWriter {
    private final FlushableLogPositionAwareChannel channel;
    private final LogEntryWriter<FlushableLogPositionAwareChannel> writer;
    private final KernelVersionProvider versionProvider;

    public TransactionLogWriter(
            FlushableLogPositionAwareChannel channel,
            KernelVersionProvider versionProvider,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this(channel, new LogEntryWriter<>(channel, binarySupportedKernelVersions), versionProvider);
    }

    @VisibleForTesting
    public TransactionLogWriter(
            FlushableLogPositionAwareChannel channel,
            LogEntryWriter<FlushableLogPositionAwareChannel> writer,
            KernelVersionProvider versionProvider) {
        this.channel = channel;
        this.writer = writer;
        this.versionProvider = versionProvider;
    }

    /*
    * It's possible to append two types of transactions: complete or chunked.
    * Complete transactions are transactions that come with all desired changes in one command batch.
    * This kind of transaction is stored in logs as a sequence of commands like:
    *   start entry -> transactional content -> commit entry
    * Chunked transactions are stored in logs in different chunks where each chunk contains only part of the transaction.
    * This kind of transaction is stored in logs as a sequence of commands like:
        start entry -> chunk start > chunk content -> chunk end -> chunk start > chunk content -> chunk end -> commit entry
    * </p>
    * An important difference is chunk start/chunk end command pairs. They present only in chunked transactions.
    * The first chunk of chunked transactions comes also with a start entry.
    * The last chunk in the chunked transaction comes with a commit entry at the end.
    */
    public int append(
            CommandBatch batch,
            long transactionId,
            long chunkId,
            int previousChecksum,
            LogPosition previousBatchPosition)
            throws IOException {
        KernelVersion kernelVersion = batch.kernelVersion();
        if (kernelVersion == null) {
            kernelVersion = versionProvider.kernelVersion();
        }
        if (batch.isRollback()) {
            return writer.writeRollbackEntry(kernelVersion, transactionId, batch.getTimeCommitted());
        }

        if (batch.isFirst()) {
            writer.writeStartEntry(
                    kernelVersion,
                    batch.getTimeStarted(),
                    batch.getLatestCommittedTxWhenStarted(),
                    previousChecksum,
                    encodeLogIndex(batch.consensusIndex()));
        } else {
            writer.writeChunkStartEntry(kernelVersion, batch.getTimeCommitted(), chunkId, previousBatchPosition);
        }

        // Write all the commands to the log channel
        writer.serialize(batch, kernelVersion);

        if (batch.isLast()) {
            return writer.writeCommitEntry(kernelVersion, transactionId, batch.getTimeCommitted());
        } else {
            return writer.writeChunkEndEntry(kernelVersion, transactionId, chunkId);
        }
    }

    public int append(CommittedCommandBatch commandBatch) throws IOException {
        return commandBatch.serialize(writer);
    }

    public LogPosition getCurrentPosition() throws IOException {
        return channel.getCurrentLogPosition();
    }

    public void resetAppendedBytesCounter() {
        channel.resetAppendedBytesCounter();
    }

    public long getAppendedBytes() {
        return channel.getAppendedBytes();
    }

    public LogPositionMarker getCurrentPosition(LogPositionMarker logPositionMarker) throws IOException {
        return channel.getCurrentLogPosition(logPositionMarker);
    }

    @VisibleForTesting
    public FlushableLogPositionAwareChannel getChannel() {
        return channel;
    }

    public int append(ByteBuffer byteBuffer) throws IOException {
        return channel.write(byteBuffer);
    }

    @VisibleForTesting
    public LogEntryWriter<FlushableLogPositionAwareChannel> getWriter() {
        return writer;
    }
}
