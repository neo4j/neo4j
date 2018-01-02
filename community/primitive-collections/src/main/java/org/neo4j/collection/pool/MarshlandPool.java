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
            LocalSlot<T> localSlot = new LocalSlot<>(objectsFromDeadThreads);
            slotReferences.add( localSlot.phantomReference );
            return localSlot;
        }
    };

    // Used to reclaim objects from dead threads
    private final Set<LocalSlotReference> slotReferences =
            newSetFromMap( new ConcurrentHashMap<LocalSlotReference, Boolean>() );
    private final ReferenceQueue<LocalSlot<T>> objectsFromDeadThreads = new ReferenceQueue<>();

    public MarshlandPool( Factory<T> objectFactory )
    {
        this(new LinkedQueuePool<>( 4, objectFactory ));
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
        if(object != null)
        {
            localSlot.set( null );
            return object;
        }

        // Try the reference queue, containing objects from dead threads
        LocalSlotReference<T> slotReference = (LocalSlotReference) objectsFromDeadThreads.poll();
        if( slotReference != null && slotReference.object != null )
        {
            slotReferences.remove( slotReference );
            return slotReference.object;
        }

        // Fall back to the delegate pool
        return pool.acquire();
    }

    @Override
    public void release( T obj )
    {
        // Return it locally if possible
        LocalSlot<T> localSlot = puddle.get();

        if(localSlot.object == null)
        {
            localSlot.set(obj);
        }

        // Fall back to the delegate pool
        else
        {
            pool.release( obj );
        }
    }

    /**
     * Dispose of all objects in this pool, releasing them back to the delegate pool
     */
    public void disposeAll()
    {
        for ( LocalSlotReference slotReference : slotReferences )
        {
            LocalSlot<T> slot = (LocalSlot) slotReference.get();
            if(slot != null)
            {
                T obj = slot.object;
                if(obj != null)
                {
                    slot.set( null );
                    pool.release( obj );
                }
            }
        }

        for(LocalSlotReference<T> reference = (LocalSlotReference) objectsFromDeadThreads.poll();
            reference != null;
            reference = (LocalSlotReference) objectsFromDeadThreads.poll() )
        {
            T instance = reference.object;
            if (instance != null)
                pool.release( instance );
        }
    }

    public void close()
    {
        disposeAll();
    }

    /**
     * This is used to trigger the GC to notify us whenever the thread local has been garbage collected.
     */
    private static class LocalSlotReference<T> extends WeakReference<LocalSlot>
    {
        private T object;

        private LocalSlotReference( LocalSlot referent, ReferenceQueue<? super LocalSlot> q )
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
        private final LocalSlotReference phantomReference;

        public LocalSlot( ReferenceQueue<LocalSlot<T>> referenceQueue )
        {
            phantomReference = new LocalSlotReference( this, referenceQueue );
        }

        public void set(T obj)
        {
            phantomReference.object = obj;
            this.object = obj;
        }
    }
}
