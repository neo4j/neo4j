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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.internal.id.indexed.IdRange.IdState;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_COMMIT;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_RESERVED;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_REUSE;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.FREE;
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
        IdRange idRange = new IdRange( 1 );
        IdRange merger = new IdRange( 1 );
        assertEquals( USED, idRange.getState( 0 ) );

        merger.clear( 1, true );
        merger.setBit( BITSET_COMMIT, 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( DELETED, idRange.getState( 0 ) );

        merger.clear( 1, true );
        merger.setBit( BITSET_REUSE, 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( FREE, idRange.getState( 0 ) );

        merger.clear( 1, false );
        merger.setBitsForAllTypes( 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( USED, idRange.getState( 0 ) );
    }

    @Test
    void clear()
    {
        final var idRange = new IdRange( 1 );
        idRange.setBit( BITSET_REUSE, 0 );
        idRange.setBit( BITSET_COMMIT, 1 );
        idRange.clear( 1, false );
        assertEquals( USED, idRange.getState( 0 ) );
        assertEquals( USED, idRange.getState( 1 ) );
        assertEquals( USED, idRange.getState( 2 ) );
    }

    @TestFactory
    Collection<DynamicTest> merge()
    {
        return Arrays.asList(
                // USED -> USED is special case because 00 is the default initial state for any ID
                dynamicTest( "USED -> USED", () -> testMerge( USED, USED, USED, false ) ),
                dynamicTest( "USED -> DELETED", () -> testMerge( USED, DELETED, DELETED, false ) ),
                dynamicTest( "USED -> FREE", () -> testMerge( USED, FREE, USED, false ) ),
                dynamicTest( "DELETED -> USED", () -> testMerge( DELETED, USED, USED, false ) ),
                dynamicTest( "DELETED -> DELETED", () -> testFailMerge( DELETED, DELETED ) ),
                dynamicTest( "DELETED -> FREE", () -> testMerge( DELETED, FREE, FREE, false ) ),
                dynamicTest( "FREE -> USED", () -> testMerge( FREE, USED, USED, false ) ),
                dynamicTest( "FREE -> DELETED", () -> testMerge( FREE, DELETED, DELETED, false ) ),
                dynamicTest( "FREE -> FREE", () -> testMerge( FREE, FREE, FREE, false ) )
        );
    }

    @TestFactory
    Collection<DynamicTest> mergeInRecoveryMode()
    {
        return Arrays.asList(
                dynamicTest( "USED -> USED", () -> testMerge( USED, USED, USED, true ) ),
                dynamicTest( "USED -> DELETED", () -> testMerge( USED, DELETED, DELETED, true ) ),
                dynamicTest( "USED -> FREE", () -> testMerge( USED, FREE, USED, true ) ),
                dynamicTest( "DELETED -> USED", () -> testMerge( DELETED, USED, USED, true ) ),
                dynamicTest( "DELETED -> DELETED", () -> testMerge( DELETED, DELETED, DELETED, true ) ),
                dynamicTest( "DELETED -> FREE", () -> testMerge( DELETED, FREE, FREE, true ) ),
                dynamicTest( "FREE -> USED", () -> testMerge( FREE, USED, USED, true ) ),
                dynamicTest( "FREE -> DELETED", () -> testMerge( FREE, DELETED, DELETED, true ) ),
                dynamicTest( "FREE -> FREE", () -> testMerge( FREE, FREE, FREE, true ) )
        );
    }

    @TestFactory
    Collection<DynamicTest> normalize()
    {
        return Arrays.asList(
                dynamicTest( "USED", () -> testNormalize( USED, USED ) ),
                dynamicTest( "DELETED", () -> testNormalize( DELETED, FREE ) ),
                dynamicTest( "FREE", () -> testNormalize( FREE, FREE ) ),
                dynamicTest( "FREE", () -> testNormalize( 0x10, USED ) )
        );
    }

    @Test
    void shouldDetermineCorrectStateForBitsCombinations()
    {
        // COMMIT, REUSE, RESERVED
        assertEquals( USED,    idStateGetsDeterminedAs( 0, 0, 0 ) );
        assertEquals( USED,    idStateGetsDeterminedAs( 0, 0, 1 ) );
        assertEquals( USED,    idStateGetsDeterminedAs( 0, 1, 0 ) );
        assertEquals( USED,    idStateGetsDeterminedAs( 0, 1, 1 ) );
        assertEquals( DELETED, idStateGetsDeterminedAs( 1, 0, 0 ) );
        assertEquals( DELETED, idStateGetsDeterminedAs( 1, 0, 1 ) );
        assertEquals( FREE,    idStateGetsDeterminedAs( 1, 1, 0 ) );
        assertEquals( DELETED, idStateGetsDeterminedAs( 1, 1, 1 ) );
    }

    @Test
    void shouldNormalizeAllPossibleStatesCorrectly()
    {
        // COMMIT, REUSE, RESERVED
        assertEquals( USED, idStateGetsNormalizedAs( 0, 0, 0 ) );
        assertEquals( USED, idStateGetsNormalizedAs( 0, 0, 1 ) );
        assertEquals( USED, idStateGetsNormalizedAs( 0, 1, 0 ) );
        assertEquals( USED, idStateGetsNormalizedAs( 0, 1, 1 ) );
        assertEquals( FREE, idStateGetsNormalizedAs( 1, 0, 0 ) );
        assertEquals( FREE, idStateGetsNormalizedAs( 1, 0, 1 ) );
        assertEquals( FREE, idStateGetsNormalizedAs( 1, 1, 0 ) );
        assertEquals( FREE, idStateGetsNormalizedAs( 1, 1, 1 ) );
    }

    private IdState idStateGetsNormalizedAs( int commitBit, int reuseBit, int reservedBit )
    {
        int numLongs = random.nextInt( 1, 3 );
        int offset = random.nextInt( numLongs * Long.SIZE );
        IdRange idRange = idRange( numLongs, offset, commitBit, reuseBit, reservedBit );
        idRange.normalize();
        return idRange.getState( offset );
    }

    private IdState idStateGetsDeterminedAs( int commitBit, int reuseBit, int reservedBit )
    {
        int numLongs = random.nextInt( 1, 3 );
        int offset = random.nextInt( numLongs * Long.SIZE );
        IdRange idRange = idRange( numLongs, offset, commitBit, reuseBit, reservedBit );
        return idRange.getState( offset );
    }

    private IdRange idRange( int numLongs, int offset, int commitBit, int reuseBit, int reservedBit )
    {
        IdRange idRange = new IdRange( numLongs );
        potentiallySetBit( idRange, BITSET_COMMIT, commitBit, offset );
        potentiallySetBit( idRange, BITSET_REUSE, reuseBit, offset );
        potentiallySetBit( idRange, BITSET_RESERVED, reservedBit, offset );
        return idRange;
    }

    private void potentiallySetBit( IdRange idRange, int type, int value, int offset )
    {
        if ( value == 1 )
        {
            idRange.setBit( type, offset );
        }
    }

    private void testNormalize( IdState beforeState, IdState afterState )
    {
        // given
        var range = initialIdRange( beforeState );

        // when
        range.normalize();

        // then
        assertEquals( afterState, range.getState( 0 ) );
    }

    private void testNormalize( int beforeState, IdState afterState )
    {
        // given
        var range = new IdRange( 1 );
        range.clear( 1, true );
        if ( (beforeState & 0x01) != 0 )
        {
            range.setBit( BITSET_COMMIT, 0 );
        }
        if ( (beforeState & 0x10) != 0 )
        {
            range.setBit( BITSET_REUSE, 0 );
        }

        // when
        range.normalize();

        // then
        assertEquals( afterState, range.getState( 0 ) );
    }

    private static void testFailMerge( IdState intoState, IdState fromState )
    {
        var into = initialIdRange( intoState );
        var from = idRange( intoState, fromState );
        assertThrows( IllegalStateException.class, () -> into.mergeFrom( from, false ), intoState + "!" + fromState );
    }

    private static void testMerge( IdState intoState, IdState fromState, IdState expected, boolean recoveryMode )
    {
        var into = initialIdRange( intoState );
        var from = idRange( intoState, fromState );
        into.mergeFrom( from, recoveryMode );
        var actual = into.getState( 0 );
        assertEquals( expected, actual );
    }

    private static IdRange initialIdRange( IdState state )
    {
        final var idRange = new IdRange( 1 );
        switch ( state )
        {
        case FREE:
            idRange.setBit( BITSET_REUSE, 0 );
        case DELETED:
            idRange.setBit( BITSET_COMMIT, 0 );
        case USED:
            break;
        default:
            throw new UnsupportedOperationException( state.name() );
        }
        return idRange;
    }

    private static IdRange idRange( IdState intoState, IdState state )
    {
        final var idRange = new IdRange( 1 );
        switch ( state )
        {
        case USED:
            idRange.clear( 1, false );
            idRange.setBitsForAllTypes( 0 );
            break;
        case DELETED:
            if ( intoState == FREE )
            {
                // If we're going from FREE to DELETED, the transition is to remove the reuse bit
                idRange.clear( 1, false );
                idRange.setBit( BITSET_REUSE, 0 );
            }
            else
            {
                // If we're going from USED to DELETED, the transition is to add the commit bit
                idRange.clear( 1, true );
                idRange.setBit( BITSET_COMMIT, 0 );
            }
            break;
        case FREE:
            idRange.clear( 1, true );
            idRange.setBit( BITSET_REUSE, 0 );
            break;
        default:
            throw new UnsupportedOperationException( state.name() );
        }
        return idRange;
    }
}
