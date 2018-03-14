/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
        short idA = set.allocate( a );
        short idB = set.allocate( b );
        SwapperSet.SwapperMapping allocA = set.getAllocation( idA );
        SwapperSet.SwapperMapping allocB = set.getAllocation( idB );
        assertThat( allocA.swapper, is( a ) );
        assertThat( allocB.swapper, is( b ) );
    }

    @Test
    public void accessingFreedAllocationMustReturnNull()
    {
        short id = set.allocate( new DummyPageSwapper( "a", 42 ) );
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
    public void freedAllocationsMustBecomeAvailableAfterVacuum()
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

        assertThat( allocated, is( equalTo( freed ) ) );
        assertThat( allocated, is( equalTo( vacuumed ) ) );
        assertThat( allocated, is( equalTo( reused ) ) );
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
    public void vacuumMustNotDustOffAnyIdsWhenNoneHaveBeenFreed()
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
    public void mustNotUseZeroAsSwapperId()
    {
        PageSwapper swapper = new DummyPageSwapper( "a", 42 );
        Matcher<Short> isNotZero = is( not( (short) 0 ) );
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
        short initial = Short.MAX_VALUE - 1;
        assertThat( set.countAvailableIds(), is( initial ) );
        int id = set.allocate( swapper );
        assertThat( set.countAvailableIds(), is( (short) (initial - 1) ) );
        set.free( id );
        assertThat( set.countAvailableIds(), is( (short) (initial - 1) ) );
        set.vacuum( x -> {} );
        assertThat( set.countAvailableIds(), is( initial ) );
    }
}
