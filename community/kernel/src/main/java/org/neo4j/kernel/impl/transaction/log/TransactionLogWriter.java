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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.util.VisibleForTesting;

public class TransactionLogWriter {
    private final FlushablePositionAwareChecksumChannel channel;
    private final LogEntryWriterFactory logEntryWriterFactory;

    public TransactionLogWriter(
            FlushablePositionAwareChecksumChannel channel, LogEntryWriterFactory logEntryWriterFactory) {
        this.channel = channel;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    /**
     * Append a transaction to the transaction log file
     * @return checksum of the transaction
     */
    public int append(CommandBatch batch, long transactionId, long chunkId, int previousChecksum) throws IOException {
        var writer = logEntryWriterFactory.createEntryWriter(channel, batch.version());
        if (batch.isFirst()) {
            writer.writeStartEntry(
                    batch.getTimeStarted(),
                    batch.getLatestCommittedTxWhenStarted(),
                    previousChecksum,
                    batch.additionalHeader());
        }

        // Write all the commands to the log channel
        writer.serialize(batch);

        // TODO: tx envelops will allow this not to return -1 for non commit entries
        // Write commit record
        return batch.isLast() ? writer.writeCommitEntry(transactionId, batch.getTimeCommitted()) : BASE_TX_CHECKSUM;
    }

    public LogPosition getCurrentPosition() throws IOException {
        return channel.getCurrentPosition();
    }

    public LogPositionMarker getCurrentPosition(LogPositionMarker logPositionMarker) throws IOException {
        return channel.getCurrentPosition(logPositionMarker);
    }

    @VisibleForTesting
    public FlushablePositionAwareChecksumChannel getChannel() {
        return channel;
    }

    @VisibleForTesting
    public LogEntryWriter<FlushablePositionAwareChecksumChannel> getWriter() {
        return logEntryWriterFactory.createEntryWriter(channel);
    }

    public void append(ByteBuffer byteBuffer) throws IOException {
        channel.write(byteBuffer);
    }
}
