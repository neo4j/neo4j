/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

public class MuninnPageCacheFixture extends PageCacheTestSupport.Fixture<MuninnPageCache>
{
    CountDownLatch backgroundFlushLatch;

    @Override
    public MuninnPageCache createPageCache( PageSwapperFactory swapperFactory, int maxPages, int pageSize,
                                            PageCacheTracer tracer )
    {
        return new MuninnPageCache( swapperFactory, maxPages, pageSize, tracer );
    }

    @Override
    public void tearDownPageCache( MuninnPageCache pageCache ) throws IOException
    {
        if ( backgroundFlushLatch != null )
        {
            backgroundFlushLatch.countDown();
            backgroundFlushLatch = null;
        }
        pageCache.close();
    }
}
