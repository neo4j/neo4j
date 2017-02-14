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
package org.neo4j.kernel.api.query;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ExecutingQueryTest
{
    private final FakeClock clock = Clocks.fakeClock( ZonedDateTime.parse( "2016-12-03T15:10:00+01:00" ) );
    private final FakeCpuClock cpuClock = new FakeCpuClock();
    private long lockCount;
    private ExecutingQuery query = new ExecutingQuery(
            1,
            ClientConnectionInfo.EMBEDDED_CONNECTION,
            "neo4j",
            "hello world",
            Collections.emptyMap(),
            Collections.emptyMap(),
            () -> lockCount, Thread.currentThread(),
            clock,
            cpuClock );

    @Test
    public void shouldReportElapsedTime() throws Exception
    {
        // when
        clock.forward( 10, TimeUnit.SECONDS );
        long elapsedTime = query.snapshot().elapsedTimeMillis();

        // then
        assertEquals( 10_000, elapsedTime );
    }

    @Test
    public void shouldTransitionBetweenStates() throws Exception
    {
        // initial
        assertThat( query.snapshot().status(), hasEntry( "state", "PLANNING" ) );

        // when
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );

        // then
        assertThat( query.snapshot().status(), hasEntry( "state", "RUNNING" ) );

        // when
        try ( LockWaitEvent event = lock( "NODE", 17 ) )
        {
            // then
            assertThat( query.snapshot().status(), hasEntry( "state", "WAITING" ) );
        }
        // then
        assertThat( query.snapshot().status(), hasEntry( "state", "RUNNING" ) );
    }

    @Test
    public void shouldReportPlanningTime() throws Exception
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
    public void shouldReportWaitTime() throws Exception
    {
        // given
        query.planningCompleted( new PlannerInfo( "the-planner", "the-runtime", emptyList() ) );

        // then
        assertEquals( singletonMap( "state", "RUNNING" ), query.snapshot().status() );

        // when
        clock.forward( 10, TimeUnit.SECONDS );
        try ( LockWaitEvent event = lock( "NODE", 17 ) )
        {
            clock.forward( 5, TimeUnit.SECONDS );

            // then
            QuerySnapshot snapshot = query.snapshot();
            assertThat( snapshot.status(), CoreMatchers.<Map<String,Object>>allOf(
                    hasEntry( "state", "WAITING" ),
                    hasEntry( "waitTimeMillis", 5_000L ),
                    hasEntry( "resourceType", "NODE" ),
                    hasEntry( equalTo( "resourceIds" ), longArray( 17 ) ) ) );
            assertEquals( 5_000, snapshot.waitTimeMillis() );
        }
        {
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( singletonMap( "state", "RUNNING" ), snapshot.status() );
            assertEquals( 5_000, snapshot.waitTimeMillis() );
        }

        // when
        clock.forward( 2, TimeUnit.SECONDS );
        try ( LockWaitEvent event = lock( "RELATIONSHIP", 612 ) )
        {
            clock.forward( 1, TimeUnit.SECONDS );

            // then
            QuerySnapshot snapshot = query.snapshot();
            assertThat( snapshot.status(), CoreMatchers.<Map<String,Object>>allOf(
                    hasEntry( "state", "WAITING" ),
                    hasEntry( "waitTimeMillis", 1_000L ),
                    hasEntry( "resourceType", "RELATIONSHIP" ),
                    hasEntry( equalTo( "resourceIds" ), longArray( 612 ) ) ) );
            assertEquals( 6_000, snapshot.waitTimeMillis() );
        }
        {
            QuerySnapshot snapshot = query.snapshot();
            assertEquals( singletonMap( "state", "RUNNING" ), snapshot.status() );
            assertEquals( 6_000, snapshot.waitTimeMillis() );
        }
    }

    @Test
    public void shouldReportCpuTime() throws Exception
    {
        // given
        cpuClock.add( 60, TimeUnit.MILLISECONDS );

        // when
        long cpuTime = query.snapshot().cpuTimeMillis();

        // then
        assertEquals( 60, cpuTime );
    }

    @Test
    public void shouldReportLockCount() throws Exception
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
}
