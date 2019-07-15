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
        merger.setCommitBit( 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( DELETED, idRange.getState( 0 ) );

        merger.clear( 1, true );
        merger.setReuseBit( 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( FREE, idRange.getState( 0 ) );

        merger.clear( 1, false );
        merger.setCommitAndReuseBit( 0 );
        idRange.mergeFrom( merger, false );
        assertEquals( USED, idRange.getState( 0 ) );
    }

    @Test
    void clear()
    {
        final var idRange = new IdRange( 1 );
        idRange.setReuseBit( 0 );
        idRange.setCommitBit( 1 );
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
            range.setCommitBit( 0 );
        }
        if ( (beforeState & 0x10) != 0 )
        {
            range.setReuseBit( 0 );
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
            idRange.setReuseBit( 0 );
        case DELETED:
            idRange.setCommitBit( 0 );
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
            idRange.setCommitAndReuseBit( 0 );
            break;
        case DELETED:
            if ( intoState == FREE )
            {
                // If we're going from FREE to DELETED, the transition is to remove the reuse bit
                idRange.clear( 1, false );
                idRange.setReuseBit( 0 );
            }
            else
            {
                // If we're going from USED to DELETED, the transition is to add the commit bit
                idRange.clear( 1, true );
                idRange.setCommitBit( 0 );
            }
            break;
        case FREE:
            idRange.clear( 1, true );
            idRange.setReuseBit( 0 );
            break;
        default:
            throw new UnsupportedOperationException( state.name() );
        }
        return idRange;
    }
}
