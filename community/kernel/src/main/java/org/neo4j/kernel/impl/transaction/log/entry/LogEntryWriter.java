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
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import java.util.Collection;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public class LogEntryWriter<T extends WritableChecksumChannel> {
    private final Visitor<StorageCommand, IOException> serializer;
    protected final T channel;
    private final byte parserSetVersion;

    public LogEntryWriter(T channel, KernelVersion version) {
        this.channel = channel;
        this.parserSetVersion = version.version();
        this.serializer = new StorageCommandSerializer(channel, this);
    }

    public void writeLogEntryHeader(byte type, WritableChannel channel) throws IOException {
        channel.put(parserSetVersion).put(type);
    }

    public void writeStartEntry(
            long timeWritten, long latestCommittedTxWhenStarted, int previousChecksum, byte[] additionalHeaderData)
            throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(TX_START, channel);
        channel.putLong(timeWritten)
                .putLong(latestCommittedTxWhenStarted)
                .putInt(previousChecksum)
                .putInt(additionalHeaderData.length)
                .put(additionalHeaderData, additionalHeaderData.length);
    }

    public void writeChunkStartEntry(long timeWritten, long chunkId) throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(CHUNK_START, channel);
        channel.putLong(timeWritten).putLong(chunkId);
    }

    public int writeChunkEndEntry(long transactionId, long chunkId) throws IOException {
        writeLogEntryHeader(CHUNK_END, channel);
        channel.putLong(transactionId);
        channel.putLong(chunkId);
        return channel.putChecksum();
    }

    public int writeCommitEntry(long transactionId, long timeWritten) throws IOException {
        writeLogEntryHeader(TX_COMMIT, channel);
        channel.putLong(transactionId).putLong(timeWritten);
        return channel.putChecksum();
    }

    public void serialize(CommandBatch tx) throws IOException {
        tx.accept(serializer);
    }

    public void serialize(CommittedCommandBatch commandBatch) throws IOException {
        commandBatch.serialize(this);
    }

    public void serialize(Collection<StorageCommand> commands) throws IOException {
        for (StorageCommand command : commands) {
            serializer.visit(command);
        }
    }

    public void serialize(StorageCommand command) throws IOException {
        serializer.visit(command);
    }

    public T getChannel() {
        return channel;
    }

    private record StorageCommandSerializer(WritableChannel channel, LogEntryWriter<?> entryWriter)
            implements Visitor<StorageCommand, IOException> {

        @Override
        public boolean visit(StorageCommand command) throws IOException {
            entryWriter.writeLogEntryHeader(COMMAND, channel);
            command.serialize(channel);
            return false;
        }
    }
}
