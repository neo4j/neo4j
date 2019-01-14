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
package org.neo4j.collection.pool;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
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
    public void shouldReturnToDelegatePoolIfLocalPoolIsFull()
    {
        // Given
        Pool<Object> delegatePool = mock( Pool.class );
        when(delegatePool.acquire()).thenReturn( 1337 );

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
    public void shouldReleaseAllSlotsOnClose()
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

    private void assertPoolEventuallyReturns( Pool<Object> pool, int expected )
    {
        long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 100 );
        while ( System.currentTimeMillis() < maxTime )
        {
            if ( pool.acquire().equals( expected ) )
            {
                return;
            }
        }

        fail( "Waited 100 seconds for pool to return object from dead thread, but it was never returned." );
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
