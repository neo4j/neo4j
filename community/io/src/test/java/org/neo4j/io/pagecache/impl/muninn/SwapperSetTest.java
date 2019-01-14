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

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SwapperSetTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    SwapperSet set;

    @Before
    public void setUp()
    {
        set = new SwapperSet();
    }

    @Test
    public void mustReturnAllocationWithSwapper()
    {
        DummyPageSwapper a = new DummyPageSwapper( "a", 42 );
        DummyPageSwapper b = new DummyPageSwapper( "b", 43 );
        int idA = set.allocate( a );
        int idB = set.allocate( b );
        SwapperSet.SwapperMapping allocA = set.getAllocation( idA );
        SwapperSet.SwapperMapping allocB = set.getAllocation( idB );
        assertThat( allocA.swapper, is( a ) );
        assertThat( allocB.swapper, is( b ) );
    }

    @Test
    public void accessingFreedAllocationMustReturnNull()
    {
        int id = set.allocate( new DummyPageSwapper( "a", 42 ) );
        set.free( id );
        assertNull( set.getAllocation( id ) );
    }

    @Test
    public void doubleFreeMustThrow()
    {
        int id = set.allocate( new DummyPageSwapper( "a", 42 ) );
        set.free( id );
        exception.expect( IllegalStateException.class );
        exception.expectMessage( "double free" );
        set.free( id );
    }

    @Test
    public void freedIdsMustNotBeReusedBeforeVacuum()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        PrimitiveIntSet ids = Primitive.intSet( 10_000 );
        for ( int i = 0; i < 10_000; i++ )
        {
            allocateFreeAndAssertNotReused( swapper, ids, i );
        }
    }

    private void allocateFreeAndAssertNotReused( PageSwapper swapper, PrimitiveIntSet ids, int i )
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
    public void freedAllocationsMustBecomeAvailableAfterVacuum()
    {
        PrimitiveIntSet allocated = Primitive.intSet();
        PrimitiveIntSet freed = Primitive.intSet();
        PrimitiveIntSet vacuumed = Primitive.intSet();
        PrimitiveIntSet reused = Primitive.intSet();
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );

        allocateAndAddTenThousand( allocated, swapper );

        allocated.visitKeys( id ->
        {
            set.free( id );
            freed.add( id );
            return false;
        } );
        set.vacuum( swapperIds -> vacuumed.addAll( ((PrimitiveIntSet) swapperIds).iterator() ) );

        allocateAndAddTenThousand( reused, swapper );

        assertThat( allocated, is( equalTo( freed ) ) );
        assertThat( allocated, is( equalTo( vacuumed ) ) );
        assertThat( allocated, is( equalTo( reused ) ) );
    }

    private void allocateAndAddTenThousand( PrimitiveIntSet allocated, PageSwapper swapper )
    {
        for ( int i = 0; i < 10_000; i++ )
        {
            allocateAndAdd( allocated, swapper );
        }
    }

    private void allocateAndAdd( PrimitiveIntSet allocated, PageSwapper swapper )
    {
        int id = set.allocate( swapper );
        allocated.add( id );
    }

    @Test
    public void vacuumMustNotDustOffAnyIdsWhenNoneHaveBeenFreed()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        for ( int i = 0; i < 100; i++ )
        {
            set.allocate( swapper );
        }
        PrimitiveIntSet vacuumedIds = Primitive.intSet();
        set.vacuum( swapperIds -> vacuumedIds.addAll( ((PrimitiveIntSet) swapperIds).iterator() ) );
        if ( !vacuumedIds.isEmpty() )
        {
            throw new AssertionError( "Vacuum found id " + vacuumedIds + " when it should have found nothing" );
        }
    }

    @Test
    public void mustNotUseZeroAsSwapperId()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        Matcher<Integer> isNotZero = is( not( 0 ) );
        for ( int i = 0; i < 10_000; i++ )
        {
            assertThat( set.allocate( swapper ), isNotZero );
        }
    }

    @Test
    public void gettingAllocationZeroMustThrow()
    {
        exception.expect( IllegalArgumentException.class );
        set.getAllocation( (short) 0 );
    }

    @Test
    public void freeOfIdZeroMustThrow()
    {
        exception.expect( IllegalArgumentException.class );
        set.free( 0 );
    }

    @Test
    public void mustKeepTrackOfAvailableSwapperIds()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        int initial = (1 << 21) - 2;
        assertThat( set.countAvailableIds(), is( initial ) );
        int id = set.allocate( swapper );
        assertThat( set.countAvailableIds(), is( initial - 1 ) );
        set.free( id );
        assertThat( set.countAvailableIds(), is( initial - 1 ) );
        set.vacuum( x -> {} );
        assertThat( set.countAvailableIds(), is( initial ) );
    }
}
