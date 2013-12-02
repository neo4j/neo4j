/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.*;

public class ReentrantLockServiceTest
{
    @Rule
    public final ThreadRepository threads = new ThreadRepository( 5, TimeUnit.SECONDS );

    @Test
    public void shouldFormLinkedListOfWaitingLockOwners() throws Exception
    {
        // given
        ReentrantLockService.LockOwner<Integer> head = new ReentrantLockService.LockOwner<>( 0 );
        ReentrantLockService.LockOwner<Integer> owner1 = new ReentrantLockService.LockOwner<>( 1 );
        ReentrantLockService.LockOwner<Integer> owner2 = new ReentrantLockService.LockOwner<>( 2 );
        ReentrantLockService.LockOwner<Integer> owner3 = new ReentrantLockService.LockOwner<>( 3 );
        ReentrantLockService.LockOwner<Integer> owner4 = new ReentrantLockService.LockOwner<>( 4 );

        // when
        head.push( owner1 );
        // then
        assertEquals( 1, head.pop().intValue() );

        // when
        head.push( owner2 );
        head.push( owner3 );
        head.push( owner4 );
        // then
        assertEquals( 2, head.pop().intValue() );
        assertEquals( 3, head.pop().intValue() );
        assertEquals( 4, head.pop().intValue() );
        assertEquals( "should get the current element when popping 'head' itself", 4, head.pop().intValue() );
        assertEquals( "should get null when popping from a dead list", null, head.pop() );
        assertEquals( "should get null continuously when popping from a dead list", null, head.pop() );
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
            ready.await();

            // then
            StackTraceElement current = thread.getStackTrace()[0];
            assertTrue( current.toString(),
                        ("park".equals( current.getMethodName() )
                         && "sun.misc.Unsafe".equals( current.getClassName() )) ||
                        ("acquire".equals( current.getMethodName() )
                         && ReentrantLockService.class.getName().equals( current.getClassName() )) );
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
}
