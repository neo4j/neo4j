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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * A concurrent pool that attempts to use thread-local pooling (puddles!) rather than a single big pool of objects to
 * lower contention. Falls back to a delegate pool if no local object can be claimed. This means this can be used as
 * a wrapper around a contended pool to alleviate contention in cases where threads generally claim and release one object
 * at a time.
 */
public class MarshlandPool<T> implements Pool<T>
{
    /*
     * This is a somewhat complicated class. What it does is to keep a single-slot local pool for each thread that
     * uses it, to allow very rapid claim calls that don't need to communicate with other threads. However, this is
     * dangerous, since pooled objects may then be lost when threads die.
     *
     * To mitigate this, the local slots are tracked by phantom references, which allows us to use a reference queue
     * to find objects that used to "belong" to now-dead threads.
     *
     * So, our algo for claiming is:
     *  - Check thread local for available object.
     *  - If none found, check the reference queue
     *  - If none found, use the delegate pool.
     */

    private final Pool<T> delegate;

    // Used to reclaim objects from dead threads
    private final Set<LocalSlotReference<T>> slotReferences = newSetFromMap( new ConcurrentHashMap<>() );
    private final ReferenceQueue<LocalSlot<T>> objectsFromDeadThreads = new ReferenceQueue<>();

    private final ThreadLocal<LocalSlot<T>> puddle = ThreadLocal.withInitial( () ->
    {
        LocalSlot<T> localSlot = new LocalSlot<>( objectsFromDeadThreads );
        slotReferences.add( localSlot.slotWeakReference );
        return localSlot;
    } );

    public MarshlandPool( Pool<T> delegatePool )
    {
        this.delegate = delegatePool;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public T acquire()
    {
        // Try and get it from the thread local
        LocalSlot<T> localSlot = puddle.get();

        T obj = localSlot.localSlotObject;
        if ( obj != null )
        {
            localSlot.set( null );
            return obj;
        }

        // Try the reference queue, containing objects from dead threads
        LocalSlotReference<T> slotReference = (LocalSlotReference<T>) objectsFromDeadThreads.poll();
        if ( slotReference != null && slotReference.localSlotReferenceObject != null )
        {
            slotReferences.remove( slotReference ); // remove from old threads
            return slotReference.localSlotReferenceObject;
        }

        // Fall back to the delegate pool
        return delegate.acquire();
    }

    @Override
    public void release( T obj )
    {
        // Return it locally if possible
        LocalSlot<T> localSlot = puddle.get();

        if ( localSlot.localSlotObject == null )
        {
            localSlot.set( obj );
        }
        else // Fall back to the delegate pool
        {
            delegate.release( obj );
        }
    }

    /**
     * Dispose of all objects in this pool, releasing them back to the delegate pool
     */
    @SuppressWarnings( "unchecked" )
    @Override
    public void close()
    {
        for ( LocalSlotReference<T> slotReference : slotReferences )
        {
            LocalSlot<T> slot = slotReference.get();
            if ( slot != null )
            {
                T obj = slot.localSlotObject;
                if ( obj != null )
                {
                    slot.set( null );
                    delegate.release( obj );
                }
            }
        }

        for ( LocalSlotReference<T> reference; (reference = (LocalSlotReference<T>) objectsFromDeadThreads.poll()) != null; )
        {
            T obj = reference.localSlotReferenceObject;
            if ( obj != null )
            {
                delegate.release( obj );
            }
        }
    }

    /**
     * Container for the "puddle", the small local pool each thread keeps.
     */
    private static final class LocalSlot<T>
    {
        private T localSlotObject;
        private final LocalSlotReference<T> slotWeakReference;

        private LocalSlot( ReferenceQueue<LocalSlot<T>> referenceQueue )
        {
            slotWeakReference = new LocalSlotReference<>( this, referenceQueue );
        }

        public void set( T obj )
        {
            slotWeakReference.localSlotReferenceObject = obj;
            this.localSlotObject = obj;
        }
    }

    /**
     * This is used to trigger the GC to notify us whenever the thread local has been garbage collected.
     */
    private static final class LocalSlotReference<T> extends WeakReference<LocalSlot<T>>
    {
        private T localSlotReferenceObject;

        private LocalSlotReference( LocalSlot<T> referent, ReferenceQueue<? super LocalSlot<T>> q )
        {
            super( referent, q );
        }
    }
}
