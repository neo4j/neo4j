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
        void notify(T listener);
    }
    
    public static <T> Iterable<T> newListeners()
    {
        return new LinkedList<T>();
    }
    
    public static <T> Iterable<T> addListener(T listener, Iterable<T> listeners)
    {
        List<T> newListeners = new LinkedList<T>( (Collection<T>) listeners );
        newListeners.add( listener );
        return newListeners;
    }

    public static <T> Iterable<T> removeListener(T listener, Iterable<T> listeners)
    {
        List<T> newListeners = new LinkedList<T>( (Collection<T>) listeners );
        newListeners.remove( listener );
        return newListeners;
    }
    
    public static <T> void notifyListeners(Iterable<T> listeners, Notification<T> notification)
    {
        for( T listener : listeners )
        {
            synchronized( listener )
            {
                try
                {
                    notification.notify( listener );
                }
                catch( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public static <T> void notifyListeners(Iterable<T> listeners, Executor executor, final Notification<T> notification)
    {
        for( final T listener : listeners )
        {
            executor.execute( new Runnable()
            {
                @Override
                public void run()
                {
                    synchronized( listener )
                    {
                        try
                        {
                            notification.notify( listener );
                        }
                        catch( Throwable e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
