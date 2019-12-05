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
package org.neo4j.io.pagecache.impl.muninn;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class SwapperSetTest
{
    private SwapperSet set;

    @BeforeEach
    void setUp()
    {
        set = new SwapperSet();
    }

    @Test
    void mustReturnAllocationWithSwapper()
    {
        DummyPageSwapper a = new DummyPageSwapper( "a", 42 );
        DummyPageSwapper b = new DummyPageSwapper( "b", 43 );
        int idA = set.allocate( a );
        int idB = set.allocate( b );
        SwapperSet.SwapperMapping allocA = set.getAllocation( idA );
        SwapperSet.SwapperMapping allocB = set.getAllocation( idB );
        assertThat( allocA.swapper ).isEqualTo( a );
        assertThat( allocB.swapper ).isEqualTo( b );
    }

    @Test
    void accessingFreedAllocationMustReturnNull()
    {
        int id = set.allocate( new DummyPageSwapper( "a", 42 ) );
        set.free( id );
        assertNull( set.getAllocation( id ) );
    }

    @Test
    void doubleFreeMustThrow()
    {
        int id = set.allocate( new DummyPageSwapper( "a", 42 ) );
        set.free( id );
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> set.free( id ) );
        assertThat( exception.getMessage() ).contains( "double free" );
    }

    @Test
    void freedIdsMustNotBeReusedBeforeVacuum()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        MutableIntSet ids = new IntHashSet( 10_000 );
        for ( int i = 0; i < 10_000; i++ )
        {
            allocateFreeAndAssertNotReused( swapper, ids, i );
        }
    }

    private void allocateFreeAndAssertNotReused( PageSwapper swapper, MutableIntSet ids, int i )
    {
        int id = set.allocate( swapper );
        set.free( id );
        if ( !ids.add( id ) )
        {
            fail( "Expected ids.add( id ) to return true for id " + id + " in iteration " + i +
                  " but it instead returned false" );
        }
    }

    @Test
    void freedAllocationsMustBecomeAvailableAfterVacuum()
    {
        MutableIntSet allocated = new IntHashSet();
        MutableIntSet freed = new IntHashSet();
        MutableIntSet vacuumed = new IntHashSet();
        MutableIntSet reused = new IntHashSet();
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );

        allocateAndAddTenThousand( allocated, swapper );

        allocated.forEach( id ->
        {
            set.free( id );
            freed.add( id );
        } );
        set.vacuum( vacuumed::addAll );

        allocateAndAddTenThousand( reused, swapper );

        assertThat( allocated ).isEqualTo( freed );
        assertThat( allocated ).isEqualTo( vacuumed );
        assertThat( allocated ).isEqualTo( reused );
    }

    private void allocateAndAddTenThousand( MutableIntSet allocated, PageSwapper swapper )
    {
        for ( int i = 0; i < 10_000; i++ )
        {
            allocateAndAdd( allocated, swapper );
        }
    }

    private void allocateAndAdd( MutableIntSet allocated, PageSwapper swapper )
    {
        int id = set.allocate( swapper );
        allocated.add( id );
    }

    @Test
    void vacuumMustNotDustOffAnyIdsWhenNoneHaveBeenFreed()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        for ( int i = 0; i < 100; i++ )
        {
            set.allocate( swapper );
        }
        MutableIntSet vacuumedIds = new IntHashSet();
        set.vacuum( vacuumedIds::addAll );
        if ( !vacuumedIds.isEmpty() )
        {
            throw new AssertionError( "Vacuum found id " + vacuumedIds + " when it should have found nothing" );
        }
    }

    @Test
    void mustNotUseZeroAsSwapperId()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        for ( int i = 0; i < 10_000; i++ )
        {
            assertThat( set.allocate( swapper ) ).isNotZero();
        }
    }

    @Test
    void gettingAllocationZeroMustThrow()
    {
        assertThrows( IllegalArgumentException.class, () -> set.getAllocation( (short) 0 ) );
    }

    @Test
    void freeOfIdZeroMustThrow()
    {
        assertThrows( IllegalArgumentException.class, () -> set.free( 0 ) );
    }

    @Test
    void mustKeepTrackOfAvailableSwapperIds()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        int initial = (1 << 21) - 2;
        assertThat( set.countAvailableIds() ).isEqualTo( initial );
        int id = set.allocate( swapper );
        assertThat( set.countAvailableIds() ).isEqualTo( initial - 1 );
        set.free( id );
        assertThat( set.countAvailableIds() ).isEqualTo( initial - 1 );
        set.vacuum( x -> {} );
        assertThat( set.countAvailableIds() ).isEqualTo( initial );
    }
}
