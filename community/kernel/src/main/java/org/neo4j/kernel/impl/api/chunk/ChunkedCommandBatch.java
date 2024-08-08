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
package org.neo4j.kernel.impl.api.chunk;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.neo4j.common.Subject;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;

public record ChunkedCommandBatch(List<StorageCommand> commands, ChunkMetadata chunkMetadata) implements CommandBatch {

    @Override
    public long consensusIndex() {
        return chunkMetadata.consensusIndex().longValue();
    }

    @Override
    public long getTimeStarted() {
        return chunkMetadata.startTimeMillis();
    }

    @Override
    public long getLatestCommittedTxWhenStarted() {
        return chunkMetadata.lastTransactionIdWhenStarted();
    }

    @Override
    public long getTimeCommitted() {
        return chunkMetadata.chunkCommitTime();
    }

    @Override
    public int getLeaseId() {
        return chunkMetadata.leaseId();
    }

    @Override
    public Subject subject() {
        return chunkMetadata.subject();
    }

    @Override
    public KernelVersion kernelVersion() {
        return chunkMetadata.kernelVersion();
    }

    @Override
    public String toString(boolean includeCommands) {
        return "CommandChunk{" + "commands=" + commands + ", chunkMetadata=" + chunkMetadata + '}';
    }

    @Override
    public boolean isLast() {
        return chunkMetadata.last();
    }

    @Override
    public boolean isFirst() {
        return chunkMetadata.first();
    }

    @Override
    public boolean isRollback() {
        return chunkMetadata.rollback();
    }

    @Override
    public boolean accept(Visitor<StorageCommand, IOException> visitor) throws IOException {
        for (StorageCommand command : commands) {
            if (visitor.visit(command)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<StorageCommand> iterator() {
        return commands.iterator();
    }

    @Override
    public int commandCount() {
        return commands.size();
    }

    @Override
    public long appendIndex() {
        long appendIndex = chunkMetadata.appendIndex().longValue();
        if (appendIndex == UNKNOWN_APPEND_INDEX) {
            throw new IllegalStateException("Append index was not generated for the batch yet.");
        }
        return appendIndex;
    }

    @Override
    public void setAppendIndex(long appendIndex) {
        chunkMetadata.appendIndex().setValue(appendIndex);
    }

    @Override
    public void setConsensusIndex(long commandIndex) {
        chunkMetadata.consensusIndex().setValue(commandIndex);
    }
}
