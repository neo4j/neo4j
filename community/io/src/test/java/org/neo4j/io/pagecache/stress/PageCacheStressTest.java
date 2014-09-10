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
package org.neo4j.io.pagecache.stress;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertThat;
import static org.neo4j.io.pagecache.PageCacheMonitor.NULL;

import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.RunnablePageCache;

public class PageCacheStressTest
{
    private final SimplePageCacheFactory simplePageCacheFactory;
    private final int numberOfCachePages;
    private final int cachePageSize;
    private final PageCacheMonitor monitor;

    private final PageCacheStresser pageCacheStresser;
    private final Condition condition;

    private PageCacheStressTest( Builder builder )
    {
        this.simplePageCacheFactory = builder.simplePageCacheFactory;
        this.numberOfCachePages = builder.numberOfCachePages;
        this.cachePageSize = builder.cachePageSize;
        this.monitor = builder.monitor;
        this.pageCacheStresser = builder.pageCacheStresser;
        this.condition = builder.condition;
    }

    public void run() throws Exception
    {
        RunnablePageCache pageCache = simplePageCacheFactory.createPageCache( numberOfCachePages, cachePageSize, monitor );

        Thread thread = new Thread( pageCache );
        thread.start();

        try
        {
            pageCacheStresser.stress( pageCache, condition );
        }
        finally
        {
            thread.interrupt();
            thread.join();
        }
    }

    public static class Builder
    {
        private int numberOfPages = 10000;
        private int recordsPerPage = 20;
        private int numberOfThreads = 23;
        private int cachePagePadding = 7;

        SimplePageCacheFactory simplePageCacheFactory;
        int numberOfCachePages = 1000;
        int cachePageSize;
        PageCacheMonitor monitor = NULL;

        PageCacheStresser pageCacheStresser;
        Condition condition;

        public PageCacheStressTest build(SimplePageCacheFactory simplePageCacheFactory )
        {
            this.simplePageCacheFactory = simplePageCacheFactory;

            assertThat( "the cache should cover only a fraction of the mapped file",
                    numberOfPages, is( greaterThanOrEqualTo( 10 * numberOfCachePages ) ) );

            pageCacheStresser = new PageCacheStresser( numberOfPages, recordsPerPage, numberOfThreads );

            int pageSize = recordsPerPage * pageCacheStresser.getRecordSize();

            assertThat( "padding should not allow another page to fit", cachePagePadding, is( lessThan( pageSize ) ) );

            cachePageSize = pageSize + cachePagePadding;

            return new PageCacheStressTest( this );
        }

        public Builder with( PageCacheMonitor monitor )
        {
            this.monitor = monitor;
            return this;
        }

        public Builder with( Condition condition )
        {
            this.condition = condition;
            return this;
        }
    }
}
