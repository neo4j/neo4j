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
package org.neo4j.kernel.impl.api.chunk;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_transaction_chunk_size;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.impl.api.TransactionClockContext;
import org.neo4j.kernel.impl.api.TransactionCommitter;
import org.neo4j.kernel.impl.api.commit.ChunkedTransactionSink;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.memory.MemoryTracker;

public class ChunkSink implements ChunkedTransactionSink {
    private final TransactionClockContext clocks;
    private final long chunkSize;
    private final TransactionCommitter committer;

    public ChunkSink(TransactionCommitter committer, TransactionClockContext clocks, Config config) {
        this.clocks = clocks;
        this.chunkSize = config.get(multi_version_transaction_chunk_size);
        this.committer = committer;
    }

    @Override
    public void write(TxState txState, TransactionEvent transactionEvent) {
        MemoryTracker memoryTracker = txState.memoryTracker();
        if (memoryTracker.estimatedHeapMemory() > chunkSize) {
            try (var commitEvent = transactionEvent.beginCommitEvent()) {
                committer.commit(commitEvent, clocks.systemClock().millis(), memoryTracker, false);
                txState.reset();
            } catch (KernelException ke) {
                throw new RuntimeException("Fail to append transaction chunk.", ke);
            }
        }
    }
}
