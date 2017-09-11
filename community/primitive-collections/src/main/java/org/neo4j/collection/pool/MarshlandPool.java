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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.function.Factory;

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

    private final Pool<T> pool;
    private final ThreadLocal<LocalSlot<T>> puddle = new ThreadLocal<LocalSlot<T>>()
    {
        @Override
        protected LocalSlot<T> initialValue()
        {
            LocalSlot<T> localSlot = new LocalSlot<>( objectsFromDeadThreads );
            slotReferences.add( localSlot.phantomReference );
            return localSlot;
        }
    };

    // Used to reclaim objects from dead threads
    private final Set<LocalSlotReference<T>> slotReferences =
            newSetFromMap( new ConcurrentHashMap<LocalSlotReference<T>, Boolean>() );
    private final ReferenceQueue<LocalSlot<T>> objectsFromDeadThreads = new ReferenceQueue<>();

    public MarshlandPool( Factory<T> objectFactory )
    {
        this( new LinkedQueuePool<>( 4, objectFactory ) );
    }

    public MarshlandPool( Pool<T> delegatePool )
    {
        this.pool = delegatePool;
    }

    @Override
    public T acquire()
    {
        // Try and get it from the thread local
        LocalSlot<T> localSlot = puddle.get();

        T object = localSlot.object;
        if ( object != null && localSlot.acquire() )
        {
            return object;
        }

        // Try the reference queue, containing objects from dead threads
        @SuppressWarnings( "unchecked" )
        LocalSlotReference<T> slotReference = (LocalSlotReference<T>) objectsFromDeadThreads.poll();
        if ( slotReference != null && slotReference.object != null )
        {
            slotReferences.remove( slotReference );
            return localSlot.assignIfNotAssigned( slotReference.object );
        }

        // Fall back to the delegate pool
        return localSlot.assignIfNotAssigned( pool.acquire() );
    }

    @Override
    public void release( T obj )
    {
        // Return it locally if possible
        LocalSlot<T> localSlot = puddle.get();

        if ( !localSlot.release( obj ) )
        {   // Fall back to the delegate pool
            pool.release( obj );
        }
        // else it was released back into the slot
    }

    /**
     * Dispose of all objects in this pool, releasing them back to the delegate pool
     */
    @SuppressWarnings( "unchecked" )
    public void disposeAll()
    {
        for ( LocalSlotReference<T> slotReference : slotReferences )
        {
            LocalSlot<T> slot = slotReference.get();
            if ( slot != null )
            {
                T obj = slot.clear();
                if ( obj != null )
                {
                    pool.release( obj );
                }
            }
        }

        for ( LocalSlotReference<T> reference = (LocalSlotReference<T>) objectsFromDeadThreads.poll();
            reference != null;
            reference = (LocalSlotReference<T>) objectsFromDeadThreads.poll() )
        {
            T instance = reference.object;
            if ( instance != null )
            {
                pool.release( instance );
            }
        }
    }

    public void close()
    {
        disposeAll();
    }

    /**
     * This is used to trigger the GC to notify us whenever the thread local has been garbage collected.
     */
    private static class LocalSlotReference<T> extends WeakReference<LocalSlot<T>>
    {
        private T object;

        private LocalSlotReference( LocalSlot<T> referent, ReferenceQueue<? super LocalSlot<T>> q )
        {
            super( referent, q );
        }
    }

    /**
     * Container for the "puddle", the small local pool each thread keeps.
     */
    private static class LocalSlot<T>
    {
        private T object;
        private final LocalSlotReference<T> phantomReference;
        private boolean acquired;

        LocalSlot( ReferenceQueue<LocalSlot<T>> referenceQueue )
        {
            phantomReference = new LocalSlotReference<>( this, referenceQueue );
        }

        T clear()
        {
            T result = acquired ? null : object;
            set( null );
            acquired = false;
            return result;
        }

        /**
         * Will assign {@code obj} to this slot if not already assigned.
         * When calling this method this slot may be in different states:
         * <ul>
         * <li>object = null, acquired = false: initial assignment</li>
         * <li>object != null, acquired = true: already assigned, but someone has it already</li>
         * </ul>
         *
         * @param obj instance to assign
         * @return the {@code obj} for convenience for the caller
         */
        T assignIfNotAssigned( T obj )
        {
            if ( object == null )
            {
                boolean wasAcquired = acquire();
                assert wasAcquired;
                set( obj );
            }
            else
            {
                assert acquired;
            }
            return obj;
        }

        /**
         * Marks this slot as not acquired anymore. This will only succeed if the released object matches the
         * object in this slot.
         *
         * @param obj the object to release and to match with this slot object.
         * @return whether or not {@code obj} matches the slot object.
         */
        boolean release( T obj )
        {
            if ( obj == object )
            {
                assert acquired;
                acquired = false;
                return true;
            }
            return false;
        }

        /**
         * Marks this slot as acquired. Object must have been assigned at this point.
         *
         * @return {@code true} if this slot wasn't acquired when calling this method, otherwise {@code false}.
         */
        boolean acquire()
        {
            if ( acquired )
            {
                return false;
            }
            acquired = true;
            return true;
        }

        private void set( T obj )
        {
            phantomReference.object = obj;
            this.object = obj;
        }
    }
}
