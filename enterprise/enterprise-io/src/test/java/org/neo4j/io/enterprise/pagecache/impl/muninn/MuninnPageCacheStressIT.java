/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.enterprise.pagecache.impl.muninn;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.*;

import org.junit.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.stress.Condition;
import org.neo4j.io.pagecache.stress.Conditions;
import org.neo4j.io.pagecache.stress.PageCacheStresser;
import org.neo4j.io.pagecache.stress.StressMonitor;

public class MuninnPageCacheStressIT
{
    @Test
    public void shouldHandleTheStressOf10000000Evictions() throws Exception
    {
        int recordsPerPage = 20;
        int fileMaxPages = 10000;
        int numberOfThreads = 8;
        PageCacheStresser pageCacheStresser = new PageCacheStresser( fileMaxPages, recordsPerPage, numberOfThreads );

        int filePageSize = recordsPerPage * pageCacheStresser.getRecordSize();
        int cachePageSize = 17 * filePageSize + 7; // records not aligned
        int cacheMaxPages = 100;

        assertThat( "cache pages should be at least as big as a file page", filePageSize, is( lessThanOrEqualTo( cachePageSize ) ) );
        assertThat( "the cache should cover only a fraction of the mapped file", fileMaxPages * filePageSize, is( greaterThanOrEqualTo( cacheMaxPages * cachePageSize ) ) );

        int desiredNumberOfEvictions = 10_000_000;
        StressMonitor stressMonitor = new StressMonitor();
        Condition condition = Conditions.numberOfEvictions( stressMonitor, desiredNumberOfEvictions );

        RunnablePageCache pageCache = new MuninnPageCache(
                new DefaultFileSystemAbstraction(),
                cacheMaxPages,
                cachePageSize,
                stressMonitor );

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
}