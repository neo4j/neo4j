/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport.cache;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class PageCacheArrayFactoryMonitorTest
{
    private final PageCachedNumberArrayFactory factory = new PageCachedNumberArrayFactory( mock( PageCache.class ), NULL, new File( "storeDir" ) );
    private final PageCacheArrayFactoryMonitor monitor = new PageCacheArrayFactoryMonitor();

    @Test
    void shouldComposeFailureDescriptionForFailedCandidates()
    {
        // given
        monitor.allocationSuccessful( 123, factory, asList(
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM1" ), NumberArrayFactory.HEAP ),
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM2" ), NumberArrayFactory.OFF_HEAP ) ) );

        // when
        String failure = monitor.pageCacheAllocationOrNull();

        // then
        assertThat( failure ).contains( "OOM1" );
        assertThat( failure ).contains( "OOM2" );
    }

    @Test
    void shouldClearFailureStateAfterAccessorCall()
    {
        // given
        monitor.allocationSuccessful( 123, factory, asList(
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM1" ), NumberArrayFactory.HEAP ),
                new NumberArrayFactory.AllocationFailure( new OutOfMemoryError( "OOM2" ), NumberArrayFactory.OFF_HEAP ) ) );

        // when
        String failure = monitor.pageCacheAllocationOrNull();
        String secondCall = monitor.pageCacheAllocationOrNull();

        // then
        assertNotNull( failure );
        assertNull( secondCall );
    }

    @Test
    void shouldReturnNullFailureOnNoFailure()
    {
        // then
        assertNull( monitor.pageCacheAllocationOrNull() );
    }
}
