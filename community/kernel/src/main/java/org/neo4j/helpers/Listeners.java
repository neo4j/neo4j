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
package org.neo4j.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Mutable thread-safe container of listeners that can be notified with {@link Notification}.
 *
 * @param <T> the type of listeners.
 */
public class Listeners<T> implements Iterable<T>
{
    private final List<T> listeners;

    /**
     * Construct new empty listeners;
     */
    public Listeners()
    {
        this.listeners = createListeners( emptyList() );
    }

    /**
     * Construct a copy of the given listeners.
     *
     * @param other listeners to copy.
     */
    public Listeners( Listeners<T> other )
    {
        requireNonNull( other, "prototype listeners can't be null" );

        this.listeners = createListeners( other.listeners );
    }

    /**
     * Adds the specified listener to this container.
     *
     * @param listener the listener to add.
     */
    public void add( T listener )
    {
        requireNonNull( listener, "added listener can't be null" );

        listeners.add( listener );
    }

    /**
     * Remove the first occurrence of the specified listener from this container, if it is present.
     *
     * @param listener the listener to remove.
     */
    public void remove( T listener )
    {
        requireNonNull( listener, "removed listener can't be null" );

        listeners.remove( listener );
    }

    /**
     * Notify all listeners in this container with the given notification.
     * Notification of each listener is synchronized on this listener.
     *
     * @param notification the notification to be applied to each listener.
     */
    public void notify( Notification<T> notification )
    {
        requireNonNull( notification, "notification can't be null" );

        for ( T listener : listeners )
        {
            notifySingleListener( listener, notification );
        }
    }

    /**
     * Notify all listeners in this container with the given notification using the given executor.
     * Each notification is submitted as a {@link Runnable} to the executor.
     * Notification of each listener is synchronized on this listener.
     *
     * @param executor the executor to submit notifications to.
     * @param notification the notification to be applied to each listener.
     */
    public void notify( Executor executor, Notification<T> notification )
    {
        requireNonNull( executor, "executor can't be null" );
        requireNonNull( notification, "notification can't be null" );

        for ( T listener : listeners )
        {
            executor.execute( () -> notifySingleListener( listener, notification ) );
        }
    }

    /**
     * Returns the iterator over listeners in this container in the order they were added.
     * <p>
     * The returned iterator provides a snapshot of the state of the list
     * when the iterator was constructed. No synchronization is needed while
     * traversing the iterator. The iterator does <em>NOT</em> support the
     * {@code remove} method.
     *
     * @return iterator over listeners.
     */
    @Override
    public Iterator<T> iterator()
    {
        return listeners.iterator();
    }

    private static <T> void notifySingleListener( T listener, Notification<T> notification )
    {
        synchronized ( listener )
        {
            notification.notify( listener );
        }
    }

    private static <T> List<T> createListeners( List<T> existingListeners )
    {
        List<T> result = new CopyOnWriteArrayList<>();
        result.addAll( existingListeners );
        return result;
    }

    public interface Notification<T>
    {
        void notify( T listener );
    }
}
