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
package org.neo4j.kernel.impl.api.commit;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public sealed interface TransactionCommitter permits ChunkCommitter, DefaultCommitter {
    long commit(
            TransactionWriteEvent transactionWriteEvent,
            LeaseClient leaseClient,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            KernelTransaction.KernelTransactionMonitor kernelTransactionMonitor,
            LockTracer lockTracer,
            long commitTime,
            long startTimeMillis,
            long lastTransactionIdWhenStarted,
            boolean commit,
            TransactionApplicationMode mode)
            throws KernelException;

    void rollback(TransactionRollbackEvent rollbackEvent) throws TransactionFailureException;

    default void reset() {}
}
