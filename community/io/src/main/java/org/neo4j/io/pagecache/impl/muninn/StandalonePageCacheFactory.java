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

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.memory.GlobalMemoryTracker;

/*
 * This class is an helper to allow to construct properly a page cache in the few places we need it without all
 * the graph database stuff, e.g., various store dump programs.
 *
 * All other places where a "proper" page cache is available, e.g. in store migration, should have that one injected.
 * And tests should use the PageCacheRule.
 */
public final class StandalonePageCacheFactory
{
    private StandalonePageCacheFactory()
    {
    }

    public static PageCache createPageCache( FileSystemAbstraction fileSystem )
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.open( fileSystem, Configuration.EMPTY );

        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        DefaultPageCursorTracerSupplier cursorTracerSupplier = DefaultPageCursorTracerSupplier.INSTANCE;
        VersionContextSupplier versionContextSupplier = EmptyVersionContextSupplier.EMPTY;
        MemoryAllocator memoryAllocator = MemoryAllocator.createAllocator( "8 MiB", GlobalMemoryTracker.INSTANCE );
        return new MuninnPageCache( factory, memoryAllocator, cacheTracer, cursorTracerSupplier, versionContextSupplier );
    }
}
