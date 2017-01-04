/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;

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
        return createPageCache( fileSystem, null, PageCacheTracer.NULL, DefaultPageCursorTracerSupplier.INSTANCE );
    }

    public static PageCache createPageCache( FileSystemAbstraction fileSystem, Integer pageSize )
    {
        return createPageCache( fileSystem, pageSize, PageCacheTracer.NULL, DefaultPageCursorTracerSupplier.INSTANCE );
    }

    public static PageCache createPageCache( FileSystemAbstraction fileSystem, Integer pageSize,
            PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier )
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.setFileSystemAbstraction( fileSystem );

        int cachePageSize = pageSize != null ? pageSize : factory.getCachePageSizeHint();
        long pageCacheMemory = ByteUnit.mebiBytes( 8 );
        long pageCount = pageCacheMemory / cachePageSize;
        return new MuninnPageCache( factory, (int) pageCount, cachePageSize, tracer, cursorTracerSupplier );
    }
}
