/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Helper class for dealing with listeners
 */
public class Listeners
{
    public interface Notification<T>
    {
        void notify( T listener );
    }

    public static <T> Collection<T> newListeners()
    {
        return new LinkedList<>();
    }

    public static <T> Collection<T> addListener( T listener, Collection<T> listeners )
    {
        List<T> newListeners = new LinkedList<>( listeners );
        newListeners.add( listener );
        return newListeners;
    }

    public static <T> Collection<T> removeListener( T listener, Collection<T> listeners )
    {
        List<T> newListeners = new LinkedList<>( listeners );
        newListeners.remove( listener );
        return newListeners;
    }

    public static <T> void notifyListeners( Collection<T> listeners, Notification<T> notification )
    {
        for ( T listener : listeners )
        {
            notifySingleListener( notification, listener );
        }
    }

    public static <T> void notifyListeners( Collection<T> listeners, Executor executor, Notification<T> notification )
    {
        for ( final T listener : listeners )
        {
            executor.execute( () -> notifySingleListener( notification, listener ) );
        }
    }

    private static <T> void notifySingleListener( Notification<T> notification, T listener )
    {
        synchronized ( listener )
        {
            try
            {
                notification.notify( listener );
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
