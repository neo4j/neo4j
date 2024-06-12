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
package org.neo4j.kernel.impl.newapi;

import java.util.function.Supplier;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public final class TransactionQueryContext implements QueryContext {

    private final Supplier<Read> read;
    private final CursorFactory cursorFactory;
    private final TxStateHolder txStateHolder;
    private final Supplier<CursorContext> cursorContext;
    private final MemoryTracker memoryTracker;
    private final IndexMonitor indexMonitor;

    public TransactionQueryContext(
            Supplier<Read> read,
            CursorFactory cursorFactory,
            TxStateHolder txStateHolder,
            Supplier<CursorContext> cursorContext,
            MemoryTracker memoryTracker,
            IndexMonitor indexMonitor) {
        this.read = read;
        this.cursorFactory = cursorFactory;
        this.txStateHolder = txStateHolder;
        this.cursorContext = cursorContext;
        this.memoryTracker = memoryTracker;
        this.indexMonitor = indexMonitor;
    }

    @Override
    public Read getRead() {
        return read.get();
    }

    @Override
    public CursorFactory cursors() {
        return cursorFactory;
    }

    @Override
    public ReadableTransactionState getTransactionStateOrNull() {
        return txStateHolder.hasTxStateWithChanges() ? txStateHolder.txState() : null;
    }

    @Override
    public CursorContext cursorContext() {
        return cursorContext.get();
    }

    @Override
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }

    @Override
    public IndexMonitor monitor() {
        return indexMonitor;
    }
}
