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

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_END;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_ROLLBACK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public class LogEntryWriter<T extends WritableChecksumChannel> {
    protected final T channel;

    public LogEntryWriter(T channel) {
        this.channel = channel;
    }

    public void writeLogEntryHeader(byte version, byte type, WritableChannel channel) throws IOException {
        channel.put(version).put(type);
    }

    public void writeStartEntry(
            byte version,
            long timeWritten,
            long latestCommittedTxWhenStarted,
            int previousChecksum,
            byte[] additionalHeaderData)
            throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(version, TX_START, channel);
        channel.putLong(timeWritten)
                .putLong(latestCommittedTxWhenStarted)
                .putInt(previousChecksum)
                .putInt(additionalHeaderData.length)
                .put(additionalHeaderData, additionalHeaderData.length);
    }

    public void writeChunkStartEntry(byte version, long timeWritten, long chunkId) throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(version, CHUNK_START, channel);
        channel.putLong(timeWritten).putLong(chunkId);
    }

    public int writeChunkEndEntry(byte version, long transactionId, long chunkId) throws IOException {
        writeLogEntryHeader(version, CHUNK_END, channel);
        channel.putLong(transactionId);
        channel.putLong(chunkId);
        return channel.putChecksum();
    }

    public int writeRollbackEntry(byte version, long transactionId, long timeWritten) throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(version, TX_ROLLBACK, channel);
        channel.putLong(transactionId).putLong(timeWritten);
        return channel.putChecksum();
    }

    public int writeCommitEntry(byte version, long transactionId, long timeWritten) throws IOException {
        writeLogEntryHeader(version, TX_COMMIT, channel);
        channel.putLong(transactionId).putLong(timeWritten);
        return channel.putChecksum();
    }

    public void serialize(CommandBatch batch) throws IOException {
        serialize(batch, batch.kernelVersion());
    }

    public void serialize(CommittedCommandBatch commandBatch) throws IOException {
        commandBatch.serialize(this);
    }

    public void serialize(Iterable<StorageCommand> commands, KernelVersion kernelVersion) throws IOException {
        byte version = kernelVersion.version();
        for (StorageCommand storageCommand : commands) {
            writeLogEntryHeader(version, COMMAND, channel);
            storageCommand.serialize(channel);
        }
    }

    public void serialize(StorageCommand command, KernelVersion kernelVersion) throws IOException {
        writeLogEntryHeader(kernelVersion.version(), COMMAND, channel);
        command.serialize(channel);
    }

    public T getChannel() {
        return channel;
    }
}
