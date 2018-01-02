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
package org.neo4j.collection.pool;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MarshlandPoolTest
{
    @Test
    public void shouldNotLooseObjectsWhenThreadsDie() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock(Pool.class);
        when(delegatePool.acquire()).thenReturn( 1337, -1 );

        final MarshlandPool<Object> pool = new MarshlandPool<>(delegatePool);

        // When
        claimAndReleaseInSeparateThread( pool );

        // Then
        verify(delegatePool).acquire();
        verifyNoMoreInteractions( delegatePool );
        assertPoolEventuallyReturns( pool, 1337 );
    }

    @Test
    public void shouldReturnToDelegatePoolIfLocalPoolIsFull() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock(Pool.class);
        when(delegatePool.acquire()).thenReturn( 1337 );

        final MarshlandPool<Object> pool = new MarshlandPool<>(delegatePool);

        Object first  = pool.acquire();
        Object second = pool.acquire();
        Object third  = pool.acquire();

        // When
        pool.release( first );
        pool.release( second );
        pool.release( third );

        // Then
        verify( delegatePool, times(3) ).acquire();
        verify( delegatePool, times(2) ).release( any() );
        verifyNoMoreInteractions( delegatePool );
    }

    @Test
    public void shouldReleaseAllSlotsOnClose() throws Exception
    {
        // Given
        Pool<Object> delegatePool = mock(Pool.class);
        when(delegatePool.acquire()).thenReturn( 1337 );

        final MarshlandPool<Object> pool = new MarshlandPool<>(delegatePool);

        Object first  = pool.acquire();
        pool.release( first );

        // When
        pool.close();

        // Then
        verify( delegatePool, times(1) ).acquire();
        verify( delegatePool, times(1) ).release( any() );
        verifyNoMoreInteractions( delegatePool );
    }

    private void assertPoolEventuallyReturns( Pool<Object> pool, int expected ) throws InterruptedException
    {
        long maxTime = System.currentTimeMillis() + 1000 * 10;
        while(System.currentTimeMillis() < maxTime)
        {
            System.gc();
            Thread.sleep( 100 );
            System.gc();
            if(pool.acquire().equals( expected ))
            {
                return;
            }
        }

        fail("Waited 10 seconds for pool to return object from dead thread, but it was never returned.");
    }

    private void claimAndReleaseInSeparateThread( final MarshlandPool<Object> pool ) throws InterruptedException
    {
        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                Object obj = pool.acquire();
                pool.release( obj );
            }
        });
        thread.start();
        thread.join();
    }

}
