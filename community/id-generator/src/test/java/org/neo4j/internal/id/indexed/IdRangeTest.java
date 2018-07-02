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

package org.neo4j.internal.id.indexed;

import org.eclipse.collections.impl.factory.BiMaps;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.internal.id.indexed.IdRange.IdState;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.stream;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.FREE;
import static org.neo4j.internal.id.indexed.IdRange.IdState.RESERVED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.USED;

@ExtendWith( RandomExtension.class )
class IdRangeTest
{
    @Inject
    private RandomRule random;

    @Test
    void defaultStateIsUsed()
    {
        final var idRange = new IdRange( 1 );
        assertEquals( USED, idRange.getState( 0 ) );
    }

    @Test
    void setAndGet()
    {
        final var idRange = new IdRange( 1 );
        idRange.setState( 0, DELETED );
        assertEquals( DELETED, idRange.getState( 0 ) );
        idRange.setState( 0, FREE );
        assertEquals( FREE, idRange.getState( 0 ) );
        idRange.setState( 0, RESERVED );
        assertEquals( RESERVED, idRange.getState( 0 ) );
    }

    @Test
    void clear()
    {
        final var idRange = new IdRange( 1 );
        idRange.setState( 0, FREE );
        idRange.setState( 1, DELETED );
        idRange.setState( 2, RESERVED );
        idRange.clear( 1 );
        assertEquals( USED, idRange.getState( 0 ) );
        assertEquals( USED, idRange.getState( 1 ) );
        assertEquals( USED, idRange.getState( 2 ) );
    }

    @TestFactory
    Collection<DynamicTest> merge()
    {
        return Arrays.asList(
                dynamicTest( "USED -> DELETED", () -> testMerge( 0b00, 0b01, 0b01, false ) ),
                dynamicTest( "USED -> FREE", () -> testMerge( 0b00, 0b10, 0b10, false ) ),
                dynamicTest( "initial set USED (USED -> RESERVED ignored)", () -> testMerge( 0b00, 0b11, 0b00, false ) ),
                dynamicTest( "DELETED -> FREE", () -> testMerge( 0b01, 0b10, 0b10, false ) ),
                dynamicTest( "FREE -> RESERVED", () -> testMerge( 0b10, 0b11, 0b11, false ) ),
                dynamicTest( "RESERVED -> USED", () -> testMerge( 0b11, 0b11, 0b00, false ) ),
                dynamicTest( "RESERVED -> FREE", () -> testMerge( 0b11, 0b10, 0b10, false ) ),
                dynamicTest( "complex", () -> testMerge( 0b00_01_10_10_11_11_00_01_11, 0b00_10_00_11_00_11_01_00_10, 0b00_10_10_11_11_00_01_01_10, false ) )
        );
    }

    @TestFactory
    Collection<DynamicTest> mergeInRecoveryMode()
    {
        return Arrays.asList(
                dynamicTest( "USED -> DELETED", () -> testMerge( 0b00, 0b01, 0b01, true ) ),
                dynamicTest( "USED -> FREE", () -> testMerge( 0b00, 0b10, 0b10, true ) ),
                dynamicTest( "initial set USED (USED -> RESERVED ignored)", () -> testMerge( 0b00, 0b11, 0b00, true ) ),
                dynamicTest( "DELETED -> FREE", () -> testMerge( 0b01, 0b10, 0b10, true ) ),
                dynamicTest( "FREE -> RESERVED", () -> testMerge( 0b10, 0b11, 0b11, true ) ),
                dynamicTest( "RESERVED -> USED", () -> testMerge( 0b11, 0b11, 0b00, true ) ),
                dynamicTest( "RESERVED -> FREE", () -> testMerge( 0b11, 0b10, 0b10, true ) ),
                dynamicTest( "complex", () -> testMerge( 0b00_01_10_10_11_11_00_01_11, 0b00_10_00_11_00_11_01_00_10, 0b00_10_10_11_11_00_01_01_10, true ) ),
                dynamicTest( "DELETED -> DELETED", () -> testMerge( 0b01, 0b01, 0b01, true ) ),
                dynamicTest( "DELETED -> USED (RESERVED)", () -> testMerge( 0b01, 0b11, 0b00, true ) ),
                dynamicTest( "RESERVED -> DELETED", () -> testMerge( 0b11, 0b01, 0b01, true ) ),
                dynamicTest( "FREE -> DELETED", () -> testMerge( 0b10, 0b01, 0b01, true ) ),
                dynamicTest( "FREE -> FREE", () -> testMerge( 0b10, 0b10, 0b10, true ) )
        );
    }

    @Test
    void mergeFailInvalidTransitions()
    {
        testFailMerge( "DELETED ! DELETED", 0b01, 0b01 );
        testFailMerge( "DELETED ! RESERVED", 0b01, 0b11 );
        testFailMerge( "FREE ! DELETED", 0b10, 0b01 );
        testFailMerge( "RESERVED ! DELETED", 0b11, 0b01 );
        testFailMerge( "FREE ! FREE", 0b11, 0b01 );
    }

    private static void testFailMerge( String msg, long into, long from )
    {
        var intoRange = new IdRange( 1 );
        var fromRange = new IdRange( 1 );
        intoRange.getOctlets()[0] = into;
        fromRange.getOctlets()[0] = from;
        assertThrows( IllegalStateException.class, () -> intoRange.mergeFrom( fromRange, false ), msg );
    }

    private static void testMerge( long into, long from, long expected, boolean recoveryMode )
    {
        var intoRange = new IdRange( 1 );
        var fromRange = new IdRange( 1 );
        intoRange.getOctlets()[0] = into;
        fromRange.getOctlets()[0] = from;
        intoRange.mergeFrom( fromRange, recoveryMode );
        var actual = intoRange.getOctlets()[0];
        assertEquals( expected, actual );
    }

    @RepeatedTest( 10 )
    void mergeRandom()
    {
        var transitions = BiMaps.immutable.with(
                USED, DELETED,
                DELETED, FREE,
                FREE, RESERVED,
                RESERVED, USED
        );

        var intoRange = new IdRange( 4 );
        var fromRange = new IdRange( 4 );
        var expectRange = new IdRange( 4 );

        for ( int i = 0; i < intoRange.size(); i++ )
        {
            var startState = random.among( IdState.values() );
            intoRange.setState( i, startState );
            if ( random.nextBoolean() )
            {
                var endState = transitions.get( startState );
                fromRange.setState( i, startState == RESERVED ? RESERVED : endState );
                expectRange.setState( i, endState );
            }
            else
            {
                expectRange.setState( i, startState );
            }
        }

        intoRange.mergeFrom( fromRange, false );

        for ( int i = 0; i < intoRange.size(); i++ )
        {
            assertEquals( expectRange.getState( i ), intoRange.getState( i ) );
        }
    }

    @Test
    void normalize()
    {
        final var idRange = idRange( USED, DELETED, FREE, RESERVED );
        final var expected = states( USED, FREE, FREE, FREE );

        idRange.normalize();

        final var actual = range( 0, 4 ).mapToObj( idRange::getState ).toArray( IdState[]::new );
        assertArrayEquals( expected, actual );
    }

    private static IdRange idRange( IdState... states )
    {
        final var idRange = new IdRange( 1 );
        for ( int i = 0; i < states.length; i++ )
        {
            idRange.setState( i, states[i] );
        }
        return idRange;
    }

    private static IdState[] states( IdState... states )
    {
        return states;
    }

    @RepeatedTest( 10 )
    void normalizeRandom()
    {
        final int OCTLETS_COUNT = 8;
        final var idRange = new IdRange( OCTLETS_COUNT );

        final var input = new IdState[OCTLETS_COUNT * 32];
        for ( int i = 0; i < input.length; i++ )
        {
            input[i] = random.among( IdState.values() );
        }

        for ( int i = 0; i < input.length; i++ )
        {
            idRange.setState( i, input[i] );
        }

        idRange.normalize();

        final var expected = stream( input ).map( s -> s == DELETED ? FREE : s ).map( s -> s == RESERVED ? FREE : s ).toArray( IdState[]::new );
        final var actual = new IdState[input.length];
        for ( int i = 0; i < input.length; i++ )
        {
            actual[i] = idRange.getState( i );
        }

        assertArrayEquals( expected, actual );
    }

    @RepeatedTest( 10 )
    void setAndGetRandom()
    {
        var idRange = new IdRange( 4 );
        var input = new IdState[idRange.size()];
        for ( var i = 0; i < input.length; i++ )
        {
            input[i] = random.among( IdState.values() );
        }

        for ( var i = 0; i < input.length; i++ )
        {
            idRange.setState( i, input[i] );
        }
        var expected = stream( input ).toArray( IdState[]::new );
        var actual = new IdState[idRange.size()];
        for ( var i = 0; i < input.length; i++ )
        {
            actual[i] = idRange.getState( i );
        }

        assertArrayEquals( expected, actual );
    }
}
