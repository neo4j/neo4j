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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Fairness in this implementation is achieved through a {@link OwnerQueueElement queue} of waiting threads for
 * {@link #locks each lock}. It guarantees that readers are not allowed before a waiting writer by not differentiating
 * between readers and writers, the locks are mutex locks, but reentrant from the same thread.
 */
public final class ReentrantLockService extends AbstractLockService<ReentrantLockService.OwnerQueueElement<Thread>>
{
    private final ConcurrentMap<LockedEntity, OwnerQueueElement<Thread>> locks = new ConcurrentHashMap<>();
    private final long maxParkNanos;

    int lockCount()
    {
        return locks.size();
    }

    public ReentrantLockService()
    {
        this( 1, TimeUnit.MILLISECONDS );
    }

    public ReentrantLockService( long maxParkTime, TimeUnit unit )
    {
        this.maxParkNanos = unit.toNanos( maxParkTime );
    }

    @Override
    protected OwnerQueueElement<Thread> acquire( LockedEntity key )
    {
        OwnerQueueElement<Thread> suggestion = new OwnerQueueElement<>( currentThread() );
        for(;;)
        {
            OwnerQueueElement<Thread> owner = locks.putIfAbsent( key, suggestion );
            if ( owner == null )
            { // Our suggestion was accepted, we got the lock
                return suggestion;
            }

            Thread other = owner.owner;
            if ( other == currentThread() )
            { // the lock has been handed to us (or we are re-entering), claim it!
                owner.count++;
                return owner;
            }

            // Make sure that we only add to the queue once, and if that addition fails (because the queue is dead
            // - i.e. has been removed from the map), retry form the top of the loop immediately.
            if ( suggestion.head == suggestion ) // true if enqueue() has not been invoked (i.e. first time around)
            { // otherwise it has already been enqueued, and we are in a spurious (or timed) wake up
                if ( !owner.enqueue( suggestion ) )
                {
                    continue; // the lock has already been released, the queue is dead, retry!
                }
            }
            parkNanos( key, maxParkNanos );
        }
    }

    @Override
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void release( LockedEntity key, OwnerQueueElement<Thread> ownerQueueElement )
    {
        if ( 0 == --ownerQueueElement.count )
        {
            Thread nextThread;
            synchronized ( ownerQueueElement )
            {
                nextThread = ownerQueueElement.dequeue();
                if ( nextThread == currentThread() )
                { // no more threads in the queue, remove this list
                    locks.remove( key, ownerQueueElement ); // done under synchronization to honour definition of 'dead'
                    nextThread = null; // to make unpark() a no-op.
                }
            }
            unpark( nextThread );
        }
    }

    /**
     * Element in a queue of owners. Contains two fields {@link #head} and {@link #tail} which form the queue.
     *
     * Example queue with 3 members:
     *
     * <pre>
     * locks -> [H]--+ <+
     *          [T]  |  |
     *          ^|   V  |
     *          ||  [H]-+
     *          ||  [T] ^
     *          ||   |  |
     *          ||   V  |
     *          |+->[H]-+
     *          +---[T]
     * </pre>
     * @param <OWNER> Type of the object that owns (or wishes to own) the lock.
     *               In practice this is always {@link Thread}, only a parameter for testing purposes.
     */
    static final class OwnerQueueElement<OWNER>
    {
        volatile OWNER owner;
        int count = 1; // does not need to be volatile, only updated by the owning thread.

        OwnerQueueElement( OWNER owner )
        {
            this.owner = owner;
        }

        /**
         * In the first element, head will point to the next waiting element, and tail is where we enqueue new elements.
         * In the waiting elements, head will point to the first element, and tail to the next element.
         */
        private OwnerQueueElement<OWNER> head = this, tail = this;

        /**
         * Return true if the item was enqueued, or false if this LockOwner is dead.
         * A dead LockOwner is no longer reachable from the map, and so no longer participates in the lock.
         */
        synchronized boolean enqueue( OwnerQueueElement<OWNER> last )
        {
            if ( owner == null )
            {
                return false; // don't enqueue into a dead queue
            }
            last.head = this;
            last.tail = this;
            tail.tail = last;
            this.tail = last;
            if ( head == this )
            {
                head = last;
            }
            return true;
        }

        synchronized OWNER dequeue()
        {
            OwnerQueueElement<OWNER> first = this.head;
            (this.head = first.tail).head = this;
            first.tail = this;
            if ( this.head == this )
            {
                this.tail = this; // don't leave junk references around!
            }
            try
            {
                return (this.owner = first.owner);
            }
            finally
            {
                first.owner = null; // mark 'first' as dead.
            }
        }

        @Override
        public String toString()
        {
            return String.format( "%s*%s", count, owner );
        }
    }
}
