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
package org.neo4j.lock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.lock.ReentrantLockService.OwnerQueueElement;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.getBlocker;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.lock.LockService.LockType.WRITE_LOCK;

class ReentrantLockServiceTest
{
    private final ReentrantLockService locks = new ReentrantLockService();

    @Test
    void shouldFormLinkedListOfWaitingLockOwners()
    {
        // given
        OwnerQueueElement<Integer> queue = new OwnerQueueElement<>( 0 );
        OwnerQueueElement<Integer> element1 = new OwnerQueueElement<>( 1 );
        OwnerQueueElement<Integer> element2 = new OwnerQueueElement<>( 2 );
        OwnerQueueElement<Integer> element3 = new OwnerQueueElement<>( 3 );
        OwnerQueueElement<Integer> element4 = new OwnerQueueElement<>( 4 );

        // when
        queue.enqueue( element1 );
        // then
        assertEquals( 1, queue.dequeue().intValue() );

        // when
        queue.enqueue( element2 );
        queue.enqueue( element3 );
        queue.enqueue( element4 );
        // then
        assertEquals( 2, queue.dequeue().intValue() );
        assertEquals( 3, queue.dequeue().intValue() );
        assertEquals( 4, queue.dequeue().intValue() );
        assertEquals( 4, queue.dequeue().intValue(), "should get the current element when dequeuing the current head" );
        assertNull( queue.dequeue(), "should get null when dequeuing from a dead list" );
        assertNull( queue.dequeue(), "should get null continuously when dequeuing from a dead list" );
    }

    @Test
    void shouldAllowReEntrance()
    {
        var lock = locks.acquireNodeLock( 11, WRITE_LOCK );
        var lock2 = locks.acquireNodeLock( 11, WRITE_LOCK );
        var lock3 = locks.acquireNodeLock( 11, WRITE_LOCK );
    }

    @Test
    @Timeout( 60 )
    void shouldBlockOnLockedLock()
    {
        // given
        var executor = Executors.newSingleThreadExecutor();

        try
        {
            var threadHolder = new AtomicReference<Thread>();
            try ( var lock = locks.acquireNodeLock( 17, WRITE_LOCK ) )
            {
                executor.execute( () -> {
                    threadHolder.set( currentThread() );
                    locks.acquireNodeLock( 17, WRITE_LOCK );
                } );

                while ( true )
                {
                    if ( threadHolder.get() != null )
                    {
                        var blocker = getBlocker( threadHolder.get() );
                        if ( blocker != null )
                        {
                            return;
                        }
                    }
                    parkNanos( MILLISECONDS.toNanos( 10 ) );
                }
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    void shouldNotLeaveResidualLockStateAfterAllLocksHaveBeenReleased()
    {
        // when
        locks.acquireNodeLock( 42, WRITE_LOCK ).release();

        // then
        assertEquals( 0, locks.lockCount() );
    }

    @Test
    void shouldPresentLockStateInStringRepresentationOfLock()
    {
        // given
        Lock first;
        Lock second;

        // when
        var currentThread = currentThread();
        try ( Lock lock = first = locks.acquireNodeLock( 666, WRITE_LOCK ) )
        {
            // then
            assertEquals( "LockedNode[id=666; HELD_BY=1*" + currentThread + "]", lock.toString() );

            // when
            try ( Lock inner = second = locks.acquireNodeLock( 666, WRITE_LOCK ) )
            {
                assertEquals( "LockedNode[id=666; HELD_BY=2*" + currentThread + "]", lock.toString() );
                assertEquals( lock.toString(), inner.toString() );
            }

            // then
            assertEquals( "LockedNode[id=666; HELD_BY=1*" + currentThread + "]", lock.toString() );
            assertEquals( "LockedNode[id=666; RELEASED]", second.toString() );
        }

        // then
        assertEquals( "LockedNode[id=666; RELEASED]", first.toString() );
        assertEquals( "LockedNode[id=666; RELEASED]", second.toString() );
    }
}
