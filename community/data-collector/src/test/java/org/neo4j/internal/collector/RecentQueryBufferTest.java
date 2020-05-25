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
package org.neo4j.internal.collector;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.map;

class RecentQueryBufferTest
{
    public static final NamedDatabaseId DATABASE_ID = DatabaseIdFactory.from( "", UUID.randomUUID() );
    private final MemoryMeter meter = new MemoryMeter();
    private final long EMPTY_MAP_SIZE = meter.measureDeep( EMPTY_MAP );
    private final long DATABASE_ID_SIZE = meter.measureDeep( DATABASE_ID );

    @Test
    void shouldEstimateHeapOfEmptyBuffer()
    {
        // when
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        RecentQueryBuffer buffer = new RecentQueryBuffer( 4, memoryTracker );

        // then
        assertEstimatedMemory( memoryTracker, buffer );
    }

    @Test
    void shouldEstimateHeapOfAddedQuery()
    {
        // given
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        RecentQueryBuffer buffer = new RecentQueryBuffer( 4, memoryTracker );

        // when
        var q = query( "RETURN 1", EMPTY_MAP );
        buffer.produce( q );

        // then
        assertEstimatedMemory( memoryTracker, buffer, q );
    }

    @Test
    void shouldEstimateHeapOfFullBuffer()
    {
        // given
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        RecentQueryBuffer buffer = new RecentQueryBuffer( 4, memoryTracker );

        // when
        var queries = new TruncatedQuerySnapshot[]{
                query( "RETURN 1", EMPTY_MAP ),
                query( "RETURN 2", EMPTY_MAP ),
                query( "RETURN 3", EMPTY_MAP ),
                query( "RETURN 4", EMPTY_MAP )
            };

        for ( TruncatedQuerySnapshot q : queries )
        {
            buffer.produce( q );
        }

        // then
        assertEstimatedMemory( memoryTracker, buffer, queries );

        // when
        var q2 = query( "RETURN 'I'm a longer query' AS x", EMPTY_MAP );
        buffer.produce( q2 );

        // then
        queries[0] = q2;
        assertEstimatedMemory( memoryTracker, buffer, queries );
    }

    @Test
    void shouldEstimateHeapOfFilledEmptiedBuffer()
    {
        // given
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        RecentQueryBuffer buffer = new RecentQueryBuffer( 4, memoryTracker );

        // when
        var queries = new TruncatedQuerySnapshot[]{
                query( "RETURN 1", EMPTY_MAP ),
                query( "RETURN 2", EMPTY_MAP ),
                query( "RETURN 3", EMPTY_MAP ),
                query( "RETURN 4", EMPTY_MAP )
        };

        for ( TruncatedQuerySnapshot q : queries )
        {
            buffer.produce( q );
        }

        buffer.clear( DATABASE_ID );

        // then
        assertEstimatedMemory( memoryTracker, buffer );
    }

    @Test
    void shouldEstimateHeapOfQueryText()
    {
        // given
        var q1 = query( "RETURN 1", EMPTY_MAP );
        var q2 = query( "RETURN 2 AS x", EMPTY_MAP );

        // when
        assertThat( q2.estimatedHeap, greaterThan( q1.estimatedHeap ) );
        assertThat( q1.estimatedHeap, equalTo( meter.measureDeep( q1 ) - EMPTY_MAP_SIZE - DATABASE_ID_SIZE ) );
        assertThat( q2.estimatedHeap, equalTo( meter.measureDeep( q2 ) - EMPTY_MAP_SIZE - DATABASE_ID_SIZE ) );
    }

    @Test
    void shouldEstimateHeapOfQueryParameters()
    {
        // given
        var q1 = query( "RETURN 1", EMPTY_MAP );
        var q2 = query( "RETURN 2", map( new String[]{"hi"}, new AnyValue[]{longValue( 42 )} ) );

        // when
        assertThat( q2.estimatedHeap, greaterThan( q1.estimatedHeap ) );
        assertThat( q2.estimatedHeap, equalTo( meter.measureDeep( q2 ) - DATABASE_ID_SIZE ) );
    }

    private void assertEstimatedMemory( LocalMemoryTracker memoryTracker, RecentQueryBuffer buffer, TruncatedQuerySnapshot... queries )
    {
        long notExpected = meter.measureDeep( memoryTracker );
        boolean hasEmptyParamQuery = Stream.of( queries ).anyMatch( q -> q.queryParameters == EMPTY_MAP );

        if ( queries.length > 0 )
        {
            notExpected += DATABASE_ID_SIZE;
        }

        if ( hasEmptyParamQuery )
        {
            notExpected += EMPTY_MAP_SIZE;
        }
        assertThat( memoryTracker.estimatedHeapMemory(), equalTo( meter.measureDeep( buffer ) - notExpected ) );
    }

    private TruncatedQuerySnapshot query( String query, MapValue params )
    {
        return new TruncatedQuerySnapshot(
                DATABASE_ID,
                query,
                freshSupplier( 42 ),
                params,
                1L,
                2L,
                3L,
                1000
        );
    }

    // This wizardry is done to emulate a new supplier instance for every query.
    private Supplier<ExecutionPlanDescription> freshSupplier( int i )
    {
        return () -> i == 0 ? null : null;
    }
}
