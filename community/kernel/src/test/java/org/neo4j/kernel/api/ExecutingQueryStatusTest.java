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
package org.neo4j.kernel.api;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.ExecutingQueryTest.resourceType;

public class ExecutingQueryStatusTest
{
    private final FakeClock clock = Clocks.fakeClock( ZonedDateTime.parse( "2016-12-16T16:14:12+01:00" ) );

    @Test
    public void shouldProduceSensibleMapRepresentationInRunningState() throws Exception
    {
        // when
        Map<String,Object> status = ExecutingQueryStatus.running().toMap( clock );

        // then
        assertEquals( singletonMap( "state", "RUNNING" ), status );
    }

    @Test
    public void shouldProduceSensibleMapRepresentationInPlanningState() throws Exception
    {
        // when
        Map<String,Object> status = ExecutingQueryStatus.planning().toMap( clock );

        // then
        assertEquals( singletonMap( "state", "PLANNING" ), status );
    }

    @Test
    public void shouldProduceSensibleMapRepresentationInWaitingState() throws Exception
    {
        // given
        long[] resourceIds = {17};
        ExecutingQueryStatus.WaitingOnLock status =
                new ExecutingQueryStatus.WaitingOnLock( resourceType( "NODE" ), resourceIds, clock.nanos() );
        clock.forward( 17, TimeUnit.MILLISECONDS );

        // when
        Map<String,Object> statusMap = status.toMap( clock );

        // then
        Map<String,Object> expected = new HashMap<>();
        expected.put( "state", "WAITING" );
        expected.put( "waitTimeMillis", 17L );
        expected.put( "resourceType", "NODE" );
        expected.put( "resourceIds", resourceIds );
        assertEquals( expected, statusMap );
    }
}
