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
package org.neo4j.kernel.impl.transaction.tracing;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.transaction.stats.TransactionLogCounters;

/**
 * The TransactionTracer is the root of the tracer hierarchy that gets notified about the life of transactions. The
 * events encapsulate the entire life of each transaction, but most of the events are concerned with what goes on
 * during commit. Implementers should take great care to make their implementations as fast as possible. Note that
 * tracers are not allowed to throw exceptions.
 */
public interface TransactionTracer extends TransactionLogCounters {
    /**
     * A TransactionTracer implementation that does nothing, other than return the NULL variants of the companion
     * interfaces.
     */
    TransactionTracer NULL = new TransactionTracer() {

        @Override
        public TransactionEvent beginTransaction(CursorContext cursorContext) {
            return TransactionEvent.NULL;
        }

        @Override
        public TransactionWriteEvent beginAsyncCommit() {
            return TransactionWriteEvent.NULL;
        }

        @Override
        public long appendedBytes() {
            return 0;
        }

        @Override
        public long numberOfLogRotations() {
            return 0;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis() {
            return 0;
        }

        @Override
        public long numberOfFlushes() {
            return 0;
        }

        @Override
        public long lastTransactionLogAppendBatch() {
            return 0;
        }

        @Override
        public long batchesAppended() {
            return 0;
        }

        @Override
        public long rolledbackBatches() {
            return 0;
        }

        @Override
        public long rolledbackBatchedTransactions() {
            return 0;
        }
    };

    /**
     * A transaction starts.
     * @return An event that represents the transaction.
     * @param cursorContext page cursor context that used by transaction
     */
    TransactionEvent beginTransaction(CursorContext cursorContext);

    /**
     * A commit starts in a context where it is detached from the
     * execution of a transaction, for example when transactions
     * are replicated in a cluster setting.
     *
     * @return An event that represents the commit.
     */
    TransactionWriteEvent beginAsyncCommit();
}
