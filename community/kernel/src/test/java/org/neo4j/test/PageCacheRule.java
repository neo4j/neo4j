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
package org.neo4j.test;

import org.junit.rules.ExternalResource;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.LifecycledPageCache;
import org.neo4j.kernel.impl.pagecache.PageCacheFactory;
import org.neo4j.kernel.impl.pagecache.StandardPageCacheFactory;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.monitoring.Monitors;

public class PageCacheRule extends ExternalResource
{
    private Neo4jJobScheduler jobScheduler;
    private LifecycledPageCache pageCache;

    public PageCache getPageCache( FileSystemAbstraction fs, Config config )
    {
        if ( pageCache != null )
        {
            pageCache.stop();
        }
        PageCacheFactory pageCacheFactory = new StandardPageCacheFactory();
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( fs );
        pageCache = new LifecycledPageCache(
                pageCacheFactory, swapperFactory, jobScheduler, config, new Monitors().newMonitor( PageCacheMonitor.class ) );
        pageCache.start();
        return pageCache;
    }

    @Override
    protected void before() throws Throwable
    {
        jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();
    }

    @Override
    protected void after()
    {
        if ( pageCache != null )
        {
            pageCache.stop();
            pageCache = null;
        }
        jobScheduler.shutdown();
    }
}
