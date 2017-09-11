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
package org.neo4j.collection.pool;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MarshlandPoolTest
{
    @Test
    public void shouldNotLooseObjectsWhenThreadsDie() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock( Pool.class );
        when( delegatePool.acquire() ).thenReturn( 1337, -1 );

        final MarshlandPool<Object> pool = new MarshlandPool<>( delegatePool );

        // When
        claimAndReleaseInSeparateThread( pool );

        // Then
        verify( delegatePool ).acquire();
        verifyNoMoreInteractions( delegatePool );
        assertPoolEventuallyReturns( pool, 1337 );
    }

    @Test
    public void shouldReturnToDelegatePoolIfLocalPoolIsFull() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock( Pool.class );
        when( delegatePool.acquire() ).thenReturn( 1337, 1338L, 1339L );

        final MarshlandPool<Object> pool = new MarshlandPool<>( delegatePool );

        Object first = pool.acquire();
        Object second = pool.acquire();
        Object third = pool.acquire();

        // When
        pool.release( first );
        pool.release( second );
        pool.release( third );

        // Then
        verify( delegatePool, times( 3 ) ).acquire();
        verify( delegatePool, times( 2 ) ).release( any() );
        verifyNoMoreInteractions( delegatePool );
    }

    @Test
    public void shouldReleaseAllSlotsOnClose() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock( Pool.class );
        when( delegatePool.acquire() ).thenReturn( 1337 );

        final MarshlandPool<Object> pool = new MarshlandPool<>( delegatePool );

        Object first = pool.acquire();
        pool.release( first );

        // When
        pool.close();

        // Then
        verify( delegatePool, times( 1 ) ).acquire();
        verify( delegatePool, times( 1 ) ).release( any() );
        verifyNoMoreInteractions( delegatePool );
    }

    /*
     * This test is about how the LocalSlot works with nested use, i.e. that acquiring an instance from the local slot
     * and while having it (before releasing it) acquiring a "nested" instance which gets released before releasing the
     * first one. The test is about how that interacts with the delegate pool and that the nested instance should not
     * take precedence over the first instance in the local slot.
     */
    @Test
    public void shouldHaveNestedUsageFallBackToDelegatePool() throws Exception
    {
        // given
        Pool<Integer> delegatePool = mock( Pool.class );
        when( delegatePool.acquire() ).thenReturn( 5, 6 );
        MarshlandPool<Integer> pool = new MarshlandPool<>( delegatePool );

        // when
        Integer top = pool.acquire();
        assertEquals( 5, top.intValue() );
        verify( delegatePool, times( 1 ) ).acquire();

        // do this a couple of times, just to verify that too
        int hoops = 2;
        for ( int i = 1; i <= hoops; i++ )
        {
            // and when w/o releasing the top one, acquire a nested
            Integer nested = pool.acquire();
            assertEquals( 6, nested.intValue() );
            verify( delegatePool, times( 1 + i ) ).acquire();

            // releasing the nested
            pool.release( nested );
            verify( delegatePool, times( i ) ).release( nested );
        }

        // when finally releasing the top one
        verify( delegatePool, times( hoops ) ).release( anyInt() );
        pool.release( top );
        verify( delegatePool, times( hoops ) ).release( anyInt() );

        // then the next acquire should see the top one
        verify( delegatePool, times( 1 + hoops ) ).acquire();
        Integer topAgain = pool.acquire();
        verify( delegatePool, times( 1 + hoops ) ).acquire();
        assertEquals( 5, topAgain.intValue() );
        pool.release( topAgain );
        verify( delegatePool, times( hoops ) ).release( anyInt() );
    }

    private void assertPoolEventuallyReturns( Pool<Object> pool, int expected ) throws InterruptedException
    {
        long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 10 );
        while ( System.currentTimeMillis() < maxTime )
        {
            if ( pool.acquire().equals( expected ) )
            {
                return;
            }
        }

        fail( "Waited 10 seconds for pool to return object from dead thread, but it was never returned." );
    }

    private void claimAndReleaseInSeparateThread( final MarshlandPool<Object> pool ) throws InterruptedException
    {
        Thread thread = new Thread( () ->
        {
            Object obj = pool.acquire();
            pool.release( obj );
        } );
        thread.start();
        thread.join();
    }

}
