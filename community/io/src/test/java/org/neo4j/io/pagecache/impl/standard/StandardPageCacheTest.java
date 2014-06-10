/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCacheTest;

public class StandardPageCacheTest extends PageCacheTest<StandardPageCache>
{
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final ConcurrentMap<PageCache, Future<?>> futures = new ConcurrentHashMap<>();

    @AfterClass
    public static void stopExecutor()
    {
        executor.shutdown();
    }

    @Override
    protected StandardPageCache createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor )
    {
        StandardPageCache pageCache = new StandardPageCache( fs, maxPages, pageSize );
        Future<?> future = executor.submit( pageCache );
        futures.put( pageCache, future );
        return pageCache;
    }

    @Override
    protected void tearDownPageCache( StandardPageCache pageCache ) throws IOException
    {
        pageCache.close();
        Future<?> future = futures.remove( pageCache );
        if ( future != null )
        {
            future.cancel( true );
        }
    }
}
