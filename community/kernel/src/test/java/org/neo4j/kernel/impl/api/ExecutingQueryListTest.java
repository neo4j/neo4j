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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class ExecutingQueryListTest
{
    @Test
    public void removingTheLastQueryReturnsAnEmptyList()
    {
        // Given
        ExecutingQuery aQuery = createExecutingQuery( 1, "query" );
        ExecutingQueryList list = ExecutingQueryList.EMPTY.push( aQuery );

        // When
        ExecutingQueryList result = list.remove( aQuery );

        // Then
        assertThat( result, equalTo( ExecutingQueryList.EMPTY ) );
    }

    @Test
    public void shouldNotChangeAListWhenRemovingAQueryThatIsNotInTheList() throws Exception
    {
        // given
        ExecutingQuery query1 = createExecutingQuery( 1, "query1" );
        ExecutingQuery query2 = createExecutingQuery( 2, "query2" );
        ExecutingQueryList list = ExecutingQueryList.EMPTY.push( query1 );

        // when
        ExecutingQueryList result = list.remove( query2 );

        // then
        assertThat( result, equalTo( list ) );
    }

    @Test
    public void addingQueriesKeepsInsertOrder()
    {
        // Given
        ExecutingQuery query1 = createExecutingQuery( 1, "query1" );
        ExecutingQuery query2 = createExecutingQuery( 2, "query2" );
        ExecutingQuery query3 = createExecutingQuery( 3, "query3" );
        ExecutingQuery query4 = createExecutingQuery( 4, "query4" );
        ExecutingQuery query5 = createExecutingQuery( 5, "query5" );

        ExecutingQueryList list = ExecutingQueryList.EMPTY
                .push( query1 )
                .push( query2 )
                .push( query3 )
                .push( query4 )
                .push( query5 );

        // When
        List<ExecutingQuery> result = list.queries().collect( Collectors.toList() );

        // Then
        assertThat( result, equalTo( asList( query5, query4, query3, query2, query1 ) ) );
    }

    @Test
    public void removingQueryInTheMiddleKeepsOrder()
    {
        // Given
        ExecutingQuery query1 = createExecutingQuery( 1, "query1" );
        ExecutingQuery query2 = createExecutingQuery( 2, "query2" );
        ExecutingQuery query3 = createExecutingQuery( 3, "query3" );
        ExecutingQuery query4 = createExecutingQuery( 4, "query4" );
        ExecutingQuery query5 = createExecutingQuery( 5, "query5" );

        ExecutingQueryList list = ExecutingQueryList.EMPTY
                .push( query1 )
                .push( query2 )
                .push( query3 )
                .push( query4 )
                .push( query5 );

        // When
        List<ExecutingQuery> result = list.remove( query3 ).queries().collect( Collectors.toList() );

        // Then
        assertThat( result, equalTo( asList( query5, query4, query2, query1 ) ) );
    }

    private ExecutingQuery createExecutingQuery( int queryId, String query )
    {
        return new ExecutingQuery( queryId, ClientConnectionInfo.EMBEDDED_CONNECTION, "me", query,
                EMPTY_MAP, Collections.emptyMap(), () -> 0, PageCursorTracer.NULL, Thread.currentThread(),
                Clocks.nanoClock(), CpuClock.CPU_CLOCK, HeapAllocation.HEAP_ALLOCATION );
    }
}
