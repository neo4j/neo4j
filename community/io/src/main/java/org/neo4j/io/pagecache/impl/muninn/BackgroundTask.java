/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * A base class for page cache background tasks.
 */
abstract class BackgroundTask implements Runnable
{
    private final MuninnPageCache pageCache;

    public BackgroundTask( MuninnPageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    @Override
    public void run()
    {
        int pageCacheId = pageCache.getPageCacheId();
        String taskName = getClass().getSimpleName();
        String threadName = "MuninnPageCache[" + pageCacheId + "]-" + taskName;
        Thread thread = Thread.currentThread();
        String previousName = thread.getName();
        try
        {
            thread.setName( threadName );
            run( pageCache );
        }
        finally
        {
            thread.setName( previousName );
        }
    }

    protected abstract void run( MuninnPageCache pageCache );
}
