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
package org.neo4j.kernel.impl.api.parallel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class ThreadExecutionContextTest {
    @Test
    void closeResourcesOnContextClose() {
        var pageCacheTracer = PageCacheTracer.NULL;
        var contextFactory = new CursorContextFactory(pageCacheTracer, EmptyVersionContextSupplier.EMPTY);
        var storageReader = mock(StorageReader.class);
        var lockClient = mock(Locks.Client.class);
        var allStoreHolder = mock(AllStoreHolder.ForThreadExecutionContextScope.class);

        var storeCursors = mock(StoreCursors.class);

        try (var executionContext = new ThreadExecutionContext(
                contextFactory.create("tag"),
                AccessMode.Static.READ,
                new ExecutionContextCursorTracer(mock(PageCacheTracer.class), "test"),
                contextFactory.create("tx-tag"),
                allStoreHolder,
                storeCursors,
                mock(IndexMonitor.class),
                mock(MemoryTracker.class),
                List.of(storageReader, lockClient))) {
            executionContext.complete();
        }

        verify(storeCursors).close();
        verify(storageReader).close();
        verify(lockClient).close();
    }
}
