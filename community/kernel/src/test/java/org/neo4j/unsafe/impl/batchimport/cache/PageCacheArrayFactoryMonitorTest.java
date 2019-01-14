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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Test;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;

public class PageCacheArrayFactoryMonitorTest
{
    private final PageCachedNumberArrayFactory factory = new PageCachedNumberArrayFactory( mock( PageCache.class ), new File( "storeDir" ) );
    private final PageCacheArrayFactoryMonitor monitor = new PageCacheArrayFactoryMonitor();

    @Test
    public void shouldComposeFailureDescriptionForFailedCandidates()
    {
        // given
        monitor.allocationSuccessful( 123, factory, asList(
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM1" ), HEAP ),
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM2" ), OFF_HEAP ) ) );

        // when
        String failure = monitor.pageCacheAllocationOrNull();

        // then
        assertThat( failure, containsString( "OOM1" ) );
        assertThat( failure, containsString( "OOM2" ) );
    }

    @Test
    public void shouldClearFailureStateAfterAccessorCall()
    {
        // given
        monitor.allocationSuccessful( 123, factory, asList(
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM1" ), HEAP ),
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM2" ), OFF_HEAP ) ) );

        // when
        String failure = monitor.pageCacheAllocationOrNull();
        String secondCall = monitor.pageCacheAllocationOrNull();

        // then
        assertNotNull( failure );
        assertNull( secondCall );
    }

    @Test
    public void shouldReturnNullFailureOnNoFailure()
    {
        // then
        assertNull( monitor.pageCacheAllocationOrNull() );
    }
}
