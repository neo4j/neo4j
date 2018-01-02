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
package org.neo4j.kernel.impl.locking;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReentrantLockServiceTest
{
    @Rule
    public final ThreadRepository threads = new ThreadRepository( 5, TimeUnit.SECONDS );

    @Test
    public void shouldFormLinkedListOfWaitingLockOwners() throws Exception
    {
        // given
        ReentrantLockService.OwnerQueueElement<Integer> queue = new ReentrantLockService.OwnerQueueElement<>( 0 );
        ReentrantLockService.OwnerQueueElement<Integer> element1 = new ReentrantLockService.OwnerQueueElement<>( 1 );
        ReentrantLockService.OwnerQueueElement<Integer> element2 = new ReentrantLockService.OwnerQueueElement<>( 2 );
        ReentrantLockService.OwnerQueueElement<Integer> element3 = new ReentrantLockService.OwnerQueueElement<>( 3 );
        ReentrantLockService.OwnerQueueElement<Integer> element4 = new ReentrantLockService.OwnerQueueElement<>( 4 );

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
        assertEquals( "should get the current element when dequeuing the current head", 4, queue.dequeue().intValue() );
        assertEquals( "should get null when dequeuing from a dead list", null, queue.dequeue() );
        assertEquals( "should get null continuously when dequeuing from a dead list", null, queue.dequeue() );
    }

    @Test
    public void shouldAllowReEntrance() throws Exception
    {
        // given
        LockService locks = new ReentrantLockService();

        ThreadRepository.Events events = threads.events();
        LockNode lock1once = new LockNode( locks, 1 );
        LockNode lock1again = new LockNode( locks, 1 );
        LockNode lock1inOtherThread = new LockNode( locks, 1 );

        ThreadRepository.Signal lockedOnce = threads.signal();
        ThreadRepository.Signal ready = threads.signal();

        // when
        threads.execute( lock1once, ready.await(), lockedOnce, lock1again,
                         events.trigger( "Double Locked" ),
                         lock1once.release, lock1again.release );
        threads.execute( ready, lockedOnce.await(), lock1inOtherThread,
                         events.trigger( "Other Thread" ),
                         lock1inOtherThread.release );

        // then
        events.assertInOrder( "Double Locked", "Other Thread" );
    }

    @Test
    public void shouldBlockOnLockedLock() throws Exception
    {
        // given
        LockService locks = new ReentrantLockService();
        LockNode lockSameNode = new LockNode( locks, 17 );
        ThreadRepository.Events events = threads.events();
        ThreadRepository.Signal ready = threads.signal();

        // when
        try ( Lock ignored = locks.acquireNodeLock( 17, LockService.LockType.WRITE_LOCK ) )
        {
            ThreadRepository.ThreadInfo thread =
                    threads.execute( ready, lockSameNode, events.trigger( "locked" ), lockSameNode.release );
            ready.awaitNow();

            // then
            assertTrue( awaitParked( thread, 5, TimeUnit.SECONDS ) );
            assertTrue( events.snapshot().isEmpty() );
        }
        events.assertInOrder( "locked" );
    }

    @Test
    public void shouldNotLeaveResidualLockStateAfterAllLocksHaveBeenReleased() throws Exception
    {
        // given
        ReentrantLockService locks = new ReentrantLockService();

        // when
        locks.acquireNodeLock( 42, LockService.LockType.WRITE_LOCK ).release();

        // then
        assertEquals( 0, locks.lockCount() );
    }

    @Test
    public void shouldPresentLockStateInStringRepresentationOfLock() throws Exception
    {
        // given
        LockService locks = new ReentrantLockService();
        Lock first, second;

        // when
        try ( Lock lock = first = locks.acquireNodeLock( 666, LockService.LockType.WRITE_LOCK ) )
        {
            // then
            assertEquals( "LockedNode[id=666; HELD_BY=1*" + Thread.currentThread() + "]", lock.toString() );

            // when
            try ( Lock inner = second = locks.acquireNodeLock( 666, LockService.LockType.WRITE_LOCK ) )
            {
                assertEquals( "LockedNode[id=666; HELD_BY=2*" + Thread.currentThread() + "]", lock.toString() );
                assertEquals( lock.toString(), inner.toString() );
            }

            // then
            assertEquals( "LockedNode[id=666; HELD_BY=1*" + Thread.currentThread() + "]", lock.toString() );
            assertEquals( "LockedNode[id=666; RELEASED]", second.toString() );
        }

        // then
        assertEquals( "LockedNode[id=666; RELEASED]", first.toString() );
        assertEquals( "LockedNode[id=666; RELEASED]", second.toString() );
    }

    private static class LockNode implements ThreadRepository.Task
    {
        private final LockService locks;
        private final long nodeId;
        private Lock lock;

        LockNode( LockService locks, long nodeId )
        {
            this.locks = locks;
            this.nodeId = nodeId;
        }

        private final ThreadRepository.Task release = new ThreadRepository.Task()
        {
            @Override
            public void perform() throws Exception
            {
                lock.release();
            }
        };

        @Override
        public void perform() throws Exception
        {
            this.lock = locks.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK );
        }
    }

    private static boolean awaitParked( ThreadRepository.ThreadInfo thread, long timeout, TimeUnit unit )
    {
        boolean parked = false;
        for ( long end = System.currentTimeMillis() + unit.toMillis( timeout ); System.currentTimeMillis() < end; )
        {
            StackTraceElement frame = thread.getStackTrace()[0];
            if ( "park".equals( frame.getMethodName() ) && "sun.misc.Unsafe".equals( frame.getClassName() ) )
            {
                if ( thread.getState().name().endsWith( "WAITING" ) )
                {
                    parked = true;
                    break;
                }
            }
        }
        return parked;
    }
}
