/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.neo4j.causalclustering.core.consensus.log.segmented.OpenEndRangeMap.ValueRange;

public class OpenEndRangeMapTest
{
    private OpenEndRangeMap<Integer,String> ranges = new OpenEndRangeMap<>();

    @Test
    public void shouldFindNothingInEmptyMap()
    {
        assertRange( -100, 100, new ValueRange<>( null, null ) );
    }

    @Test
    public void shouldFindSingleRange()
    {
        // when
        ranges.replaceFrom( 0, "A" );

        // then
        assertRange( -100,  -1, new ValueRange<>( 0, null ) );
        assertRange(    0, 100, new ValueRange<>( null, "A" ) );
    }

    @Test
    public void shouldHandleMultipleRanges()
    {
        // when
        ranges.replaceFrom(  0, "A" );
        ranges.replaceFrom(  5, "B" );
        ranges.replaceFrom( 10, "C" );

        // then
        assertRange( -100,  -1,    new ValueRange<>( 0, null ) );
        assertRange(    0,   4,    new ValueRange<>( 5,  "A" ) );
        assertRange(    5,   9,   new ValueRange<>( 10,  "B" ) );
        assertRange(   10, 100, new ValueRange<>(  null,  "C" ) );
    }

    @Test
    public void shouldTruncateAtPreviousEntry()
    {
        // given
        ranges.replaceFrom(  0, "A" );
        ranges.replaceFrom( 10, "B" );

        // when
        Collection<String> removed = ranges.replaceFrom( 10, "C" );

        // then
        assertRange( -100,  -1,  new ValueRange<>( 0, null ) );
        assertRange(    0,   9, new ValueRange<>( 10, "A" ) );
        assertRange(   10, 100, new ValueRange<>( null, "C" ) );

        assertThat( removed, hasItems( "B" ) );
    }

    @Test
    public void shouldTruncateBeforePreviousEntry()
    {
        // given
        ranges.replaceFrom(  0, "A" );
        ranges.replaceFrom( 10, "B" );

        // when
        Collection<String> removed = ranges.replaceFrom( 7, "C" );

        // then
        assertRange( -100,  -1,  new ValueRange<>( 0, null ) );
        assertRange(    0,   6,  new ValueRange<>( 7, "A" ) );
        assertRange(   7,  100,  new ValueRange<>( null, "C" ) );

        assertThat( removed, hasItems( "B" ) );
    }

    @Test
    public void shouldTruncateSeveralEntries()
    {
        // given
        ranges.replaceFrom(  0, "A" );
        ranges.replaceFrom( 10, "B" );
        ranges.replaceFrom( 20, "C" );
        ranges.replaceFrom( 30, "D" );

        // when
        Collection<String> removed = ranges.replaceFrom( 15, "E" );

        // then
        assertRange( -100,  -1,  new ValueRange<>( 0, null ) );
        assertRange(    0,   9,  new ValueRange<>( 10, "A" ) );
        assertRange(   10,  14,  new ValueRange<>( 15, "B" ) );
        assertRange(   15, 100,  new ValueRange<>( null, "E" ) );

        assertThat( removed, hasItems( "C", "D" ) );
    }

    @Test
    public void shouldOnlyPruneWholeEntries()
    {
        // given
        ranges.replaceFrom(  0, "A" );
        ranges.replaceFrom(  5, "B" );

        Collection<String> removed;

        // when / then
        removed = ranges.remove( 4 );
        assertTrue( removed.isEmpty() );

        // when
        removed = ranges.remove( 5 );
        assertFalse( removed.isEmpty() );
        assertThat( removed, hasItems( "A" ) );
    }

    private <T> void assertRange( int from, int to, ValueRange<Integer,T> expected )
    {
        for ( int i = from; i <= to; i++ )
        {
            ValueRange<Integer,String> valueRange = ranges.lookup( i );
            assertEquals( expected, valueRange );
        }
    }
}
