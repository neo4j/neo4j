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
package org.neo4j.kernel.impl.api.parallel;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.CursorStatisticSnapshot;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;

public class ExecutionContextCursorTracer extends DefaultPageCursorTracer {
    public static final String TRANSACTION_EXECUTION_TAG = "transactionExecution";
    private volatile boolean completed;

    public ExecutionContextCursorTracer(PageCacheTracer pageCacheTracer, String tag) {
        super(pageCacheTracer, tag);
    }

    @Override
    public void reportEvents() {
        throw new UnsupportedOperationException("Please report events using merge snapshots.");
    }

    // We mark context as completed here since we want to capture all the events accumulated in the tracer for another
    // consumer thread.
    // That in ensued by waiting for completed flag by consumer thread.
    public void complete() {
        completed = true;
    }

    CursorStatisticSnapshot snapshot() {
        CursorStatisticSnapshot snapshot = new CursorStatisticSnapshot(
                super.pins(),
                super.unpins(),
                super.hits(),
                super.faults(),
                super.noFaults(),
                super.failedFaults(),
                super.vectoredFaults(),
                super.failedVectoredFaults(),
                super.noPinFaults(),
                super.bytesRead(),
                super.bytesWritten(),
                super.evictions(),
                super.evictionFlushes(),
                super.evictionExceptions(),
                super.flushes(),
                super.merges(),
                super.snapshotsLoaded(),
                super.copiedPages(),
                super.openedCursors(),
                super.closedCursors());
        reset();
        return snapshot;
    }

    public boolean isCompleted() {
        return completed;
    }
}
