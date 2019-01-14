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
package org.neo4j.kernel.api.query;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.MathUtil;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorCounters;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.test.FakeHeapAllocation;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.junit.Assert.assertTrue;

public class ExecutingQueryTest
{
    private final FakeClock clock = Clocks.fakeClock( ZonedDateTime.parse( "2016-12-03T15:10:00+01:00" ) );
    @Rule
    public final FakeCpuClock cpuClock = new FakeCpuClock().add( randomLong( 0x1_0000_0000L ) );
    @Rule
    public final FakeHeapAllocation heapAllocation = new FakeHeapAllocation().add( randomLong( 0x1_0000_0000L ) );
    private final PageCursorCountersStub page = new PageCursorCountersStub();
    private long lockCount;
    private ExecutingQuery query = createExecutingquery( 1, "hello world", page, clock, cpuClock, heapAllocation );
    private ExecutingQuery subQuery = createExecutingquery( 2, "goodbye world", page, clock, cpuClock, heapAllocation );

    @Test
    public void shouldReportElapsedTime()
    {
        // when
        clock.forward( 10, TimeUnit.SECONDS );
        long elapsedTime = query.snapshot().elapsedTimeMillis();

        // then
        assertEquals( 10_000, elapsedTime );
    }

    @Test
    public void shouldTransitionBetweenStates()
    {
        // initial
        assertEquals( "planning", query.snapshot().status() );

        // when
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );

        // then
        assertEquals( "running", query.snapshot().status() );

        // when
        try ( LockWaitEvent event = lock( "NODE", 17 ) )
        {
            // then
            assertEquals( "waiting", query.snapshot().status() );
        }
        // then
        assertEquals( "running", query.snapshot().status() );

        // when
        query.waitsForQuery( subQuery );

        // then
        assertEquals( "waiting", query.snapshot().status() );

        // when
        query.waitsForQuery( null );

        // then
        assertEquals( "running", query.snapshot().status() );
    }

    @Test
    public void shouldReportPlanningTime()
    {
        // when
        clock.forward( 124, TimeUnit.MILLISECONDS );

        // then
        QuerySnapshot snapshot = query.snapshot();
        assertEquals( snapshot.planningTimeMillis(), snapshot.elapsedTimeMillis() );

        // when
        clock.forward( 16, TimeUnit.MILLISECONDS );
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );
        clock.forward( 200, TimeUnit.MILLISECONDS );

        // then
        snapshot = query.snapshot();
        assertEquals( 140, snapshot.planningTimeMillis() );
        assertEquals( 340, snapshot.elapsedTimeMillis() );
    }

    @Test
    public void shouldReportWaitTime()
    {
        // given
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );

        // then
        assertEquals( "running", query.snapshot().status() );

        // when
        clock.forward( 10, TimeUnit.SECONDS );
        try ( LockWaitEvent event = lock( "NODE", 17 ) )
        {
            clock.forward( 5, TimeUnit.SECONDS );

            // then
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( "waiting", snapshot.status() );
            assertThat( snapshot.resourceInformation(), CoreMatchers.<Map<String,Object>>allOf(
                    hasEntry( "waitTimeMillis", 5_000L ),
                    hasEntry( "resourceType", "NODE" ),
                    hasEntry( equalTo( "resourceIds" ), longArray( 17 ) ) ) );
            assertEquals( 5_000, snapshot.waitTimeMillis() );
        }
        {
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( "running", snapshot.status() );
            assertEquals( 5_000, snapshot.waitTimeMillis() );
        }

        // when
        clock.forward( 2, TimeUnit.SECONDS );
        try ( LockWaitEvent event = lock( "RELATIONSHIP", 612 ) )
        {
            clock.forward( 1, TimeUnit.SECONDS );

            // then
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( "waiting", snapshot.status() );
            assertThat( snapshot.resourceInformation(), CoreMatchers.<Map<String,Object>>allOf(
                    hasEntry( "waitTimeMillis", 1_000L ),
                    hasEntry( "resourceType", "RELATIONSHIP" ),
                    hasEntry( equalTo( "resourceIds" ), longArray( 612 ) ) ) );
            assertEquals( 6_000, snapshot.waitTimeMillis() );
        }
        {
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( "running", snapshot.status() );
            assertEquals( 6_000, snapshot.waitTimeMillis() );
        }
    }

    @Test
    public void shouldReportQueryWaitTime()
    {
        // given
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );

        // when
        query.waitsForQuery( subQuery );
        clock.forward( 5, TimeUnit.SECONDS );

        // then
        QuerySnapshot snapshot = query.snapshot();
        assertEquals( 5_000L, snapshot.waitTimeMillis() );
        assertEquals( "waiting", snapshot.status() );
        assertThat( snapshot.resourceInformation(), CoreMatchers.<Map<String,Object>>allOf(
                hasEntry( "waitTimeMillis", 5_000L ),
                hasEntry( "queryId", "query-2" ) ) );

        // when
        clock.forward( 1, TimeUnit.SECONDS );
        query.waitsForQuery( null );
        clock.forward( 2, TimeUnit.SECONDS );

        // then
        snapshot = query.snapshot();
        assertEquals( 6_000L, snapshot.waitTimeMillis() );
        assertEquals( "running", snapshot.status() );
    }

    @Test
    public void shouldReportCpuTime()
    {
        // given
        cpuClock.add( 60, TimeUnit.MILLISECONDS );

        // when
        long cpuTime = query.snapshot().cpuTimeMillis();

        // then
        assertEquals( 60, cpuTime );
    }

    @Test
    public void shouldNotReportCpuTimeIfUnavailable()
    {
        // given
        ExecutingQuery query = new ExecutingQuery( 17,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                "neo4j",
                "hello world",
                EMPTY_MAP,
                Collections.emptyMap(),
                () -> lockCount, PageCursorTracer.NULL,
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                clock,
                FakeCpuClock.NOT_AVAILABLE,
                HeapAllocation.NOT_AVAILABLE );

        // when
        QuerySnapshot snapshot = query.snapshot();

        // then
        assertNull( snapshot.cpuTimeMillis() );
        assertNull( snapshot.idleTimeMillis() );
    }

    @Test
    public void shouldReportHeapAllocation()
    {
        // given
        heapAllocation.add( 4096 );

        // when
        long allocatedBytes = query.snapshot().allocatedBytes();

        // then
        assertEquals( 4096, allocatedBytes );

        // when
        heapAllocation.add( 4096 );
        allocatedBytes = query.snapshot().allocatedBytes();

        // then
        assertEquals( 8192, allocatedBytes );
    }

    @Test
    public void shouldNotReportHeapAllocationIfUnavailable()
    {
        // given
        ExecutingQuery query = new ExecutingQuery( 17,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                "neo4j",
                "hello world",
                EMPTY_MAP,
                Collections.emptyMap(),
                () -> lockCount,
                PageCursorTracer.NULL,
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                clock,
                FakeCpuClock.NOT_AVAILABLE,
                HeapAllocation.NOT_AVAILABLE );

        // when
        QuerySnapshot snapshot = query.snapshot();

        // then
        assertNull( snapshot.allocatedBytes() );
    }

    @Test
    public void shouldReportLockCount()
    {
        // given
        lockCount = 11;

        // then
        assertEquals( 11, query.snapshot().activeLockCount() );

        // given
        lockCount = 2;

        // then
        assertEquals( 2, query.snapshot().activeLockCount() );
    }

    @Test
    public void shouldReportPageHitsAndFaults()
    {
        // given
        page.hits( 7 );
        page.faults( 3 );

        // when
        QuerySnapshot snapshot = query.snapshot();

        // then
        assertEquals( 7, snapshot.pageHits() );
        assertEquals( 3, snapshot.pageFaults() );

        // when
        page.hits( 2 );
        page.faults( 5 );
        snapshot = query.snapshot();

        // then
        assertEquals( 9, snapshot.pageHits() );
        assertEquals( 8, snapshot.pageFaults() );
    }

    @Test
    public void includeQueryExecutorThreadName()
    {
        String queryDescription = query.toString();
        assertTrue( queryDescription.contains( "threadExecutingTheQueryName=" + Thread.currentThread().getName() ) );
    }

    private LockWaitEvent lock( String resourceType, long resourceId )
    {
        return query.lockTracer().waitForLock( false, resourceType( resourceType ), resourceId );
    }

    static ResourceType resourceType( String name )
    {
        return new ResourceType()
        {
            @Override
            public String toString()
            {
                return name();
            }

            @Override
            public int typeId()
            {
                throw new UnsupportedOperationException( "not used" );
            }

            @Override
            public WaitStrategy waitStrategy()
            {
                throw new UnsupportedOperationException( "not used" );
            }

            @Override
            public String name()
            {
                return name;
            }
        };
    }

    @SuppressWarnings( "unchecked" )
    private static Matcher<Object> longArray( long... expected )
    {
        return (Matcher) new TypeSafeMatcher<long[]>()
        {
            @Override
            protected boolean matchesSafely( long[] item )
            {
                return Arrays.equals( expected, item );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( expected );
            }
        };
    }

    private static long randomLong( long bound )
    {
        return ThreadLocalRandom.current().nextLong( bound );
    }

    private ExecutingQuery createExecutingquery( int queryId, String hello_world, PageCursorCountersStub page,
            FakeClock clock, FakeCpuClock cpuClock, FakeHeapAllocation heapAllocation )
    {
        return new ExecutingQuery( queryId, ClientConnectionInfo.EMBEDDED_CONNECTION, "neo4j", hello_world,
                EMPTY_MAP, Collections.emptyMap(), () -> lockCount, page, Thread.currentThread().getId(),
                Thread.currentThread().getName(), clock, cpuClock, heapAllocation );
    }

    private static class PageCursorCountersStub implements PageCursorCounters
    {
        private long faults;
        private long pins;
        private long unpins;
        private long hits;
        private long bytesRead;
        private long evictions;
        private long evictionExceptions;
        private long bytesWritten;
        private long flushes;

        @Override
        public long faults()
        {
            return faults;
        }

        public void faults( long increment )
        {
            faults += increment;
        }

        @Override
        public long pins()
        {
            return pins;
        }

        public void pins( long increment )
        {
            pins += increment;
        }

        @Override
        public long unpins()
        {
            return unpins;
        }

        public void unpins( long increment )
        {
            unpins += increment;
        }

        @Override
        public long hits()
        {
            return hits;
        }

        public void hits( long increment )
        {
            hits += increment;
        }

        @Override
        public long bytesRead()
        {
            return bytesRead;
        }

        public void bytesRead( long increment )
        {
            bytesRead += increment;
        }

        @Override
        public long evictions()
        {
            return evictions;
        }

        public void evictions( long increment )
        {
            evictions += increment;
        }

        @Override
        public long evictionExceptions()
        {
            return evictionExceptions;
        }

        public void evictionExceptions( long increment )
        {
            evictionExceptions += increment;
        }

        @Override
        public long bytesWritten()
        {
            return bytesWritten;
        }

        public void bytesWritten( long increment )
        {
            bytesWritten += increment;
        }

        @Override
        public long flushes()
        {
            return flushes;
        }

        public void flushes( long increment )
        {
            flushes += increment;
        }

        @Override
        public double hitRatio()
        {
            return MathUtil.portion( hits(), faults() );
        }
    }
}
