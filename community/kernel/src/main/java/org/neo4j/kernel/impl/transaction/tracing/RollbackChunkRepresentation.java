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
package org.neo4j.kernel.impl.transaction.tracing;

import static java.util.Collections.emptyList;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import org.neo4j.common.Subject;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.CommandChunk;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.CommandBatch;

public record RollbackChunkRepresentation(
        KernelVersion kernelVersion, long transactionId, long timeWritten, int checksum)
        implements CommittedCommandBatch {

    @Override
    public CommandBatch commandBatch() {
        return new CommandChunk(
                emptyList(),
                new ChunkMetadata(
                        false,
                        true,
                        -1,
                        UNKNOWN_CONSENSUS_INDEX,
                        timeWritten,
                        -1,
                        timeWritten,
                        -1,
                        kernelVersion,
                        Subject.ANONYMOUS));
    }

    @Override
    public int serialize(LogEntryWriter<? extends WritableChecksumChannel> writer) throws IOException {
        return writer.writeRollbackEntry(kernelVersion.version(), transactionId, timeWritten);
    }

    @Override
    public int checksum() {
        return checksum;
    }

    @Override
    public long timeWritten() {
        return timeWritten;
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }

    @Override
    public long txId() {
        return transactionId;
    }

    @Override
    public boolean isRollback() {
        return true;
    }
}
