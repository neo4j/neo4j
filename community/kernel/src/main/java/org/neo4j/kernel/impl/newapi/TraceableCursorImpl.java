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

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.util.VisibleForTesting;

@VisibleForTesting
public abstract class TraceableCursorImpl<CURSOR> extends DefaultCloseListenable implements TraceableCursor {
    private final CursorPool<CURSOR> pool;
    protected KernelReadTracer tracer;
    private boolean returnedToPool;

    TraceableCursorImpl(CursorPool<CURSOR> pool) {
        this.pool = pool;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public void removeTracer() {
        this.tracer = null;
    }

    @Override
    public void acquire() {
        if (!returnedToPool) {
            throw new IllegalStateException(this + " hasn't been returned to pool yet");
        }
        returnedToPool = false;
    }

    @Override
    public void closeInternal() {
        if (!returnedToPool) {
            pool.accept((CURSOR) this);
            returnedToPool = true;
        }
    }

    @VisibleForTesting
    public boolean returnedToPool() {
        return returnedToPool;
    }
}
