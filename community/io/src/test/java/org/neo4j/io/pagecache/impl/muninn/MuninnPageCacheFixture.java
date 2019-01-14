/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

public class MuninnPageCacheFixture extends PageCacheTestSupport.Fixture<MuninnPageCache>
{
    CountDownLatch backgroundFlushLatch;

    @Override
    public MuninnPageCache createPageCache( PageSwapperFactory swapperFactory, int maxPages,
                                            PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier,
            VersionContextSupplier contextSupplier )
    {
        long memory = MuninnPageCache.memoryRequiredForPages( maxPages );
        MemoryAllocator allocator = MemoryAllocator.createAllocator( String.valueOf( memory ),
                new LocalMemoryTracker() );
        return new MuninnPageCache( swapperFactory, allocator, tracer, cursorTracerSupplier, contextSupplier );
    }

    @Override
    public void tearDownPageCache( MuninnPageCache pageCache )
    {
        if ( backgroundFlushLatch != null )
        {
            backgroundFlushLatch.countDown();
            backgroundFlushLatch = null;
        }
        pageCache.close();
    }
}
