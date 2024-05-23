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

import java.util.function.Supplier;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.lock.LockTracer;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public sealed interface ChunkedTransactionSink permits ChunkSink, ChunkedTransactionSink.EmptyChunkedTransactionSink {

    ChunkedTransactionSink EMPTY = new EmptyChunkedTransactionSink();

    void write(TxState txState, TransactionEvent transactionEvent);

    void initialize(
            LeaseClient leaseClient,
            CursorContext cursorContext,
            Supplier<LockTracer> lockTracerSupplier,
            long startTimeMillis,
            long lastTransactionIdWhenStarted,
            Supplier<TransactionApplicationMode> applicationModeSupplier);

    final class EmptyChunkedTransactionSink implements ChunkedTransactionSink {

        private EmptyChunkedTransactionSink() {}

        @Override
        public void write(TxState txState, TransactionEvent event) {}

        @Override
        public void initialize(
                LeaseClient leaseClient,
                CursorContext cursorContext,
                Supplier<LockTracer> lockTracerSupplier,
                long startTimeMillis,
                long lastTransactionIdWhenStarted,
                Supplier<TransactionApplicationMode> applicationModeSupplier) {}
    }
}
