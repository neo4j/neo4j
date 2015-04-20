/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Predicate;

public abstract class CappedOperation<T>
{
    public interface Switch<T> extends Predicate<T>
    {
        void reset();
    }

    private final Switch<T> opener;

    public CappedOperation( Switch... openers )
    {
        this.opener = firstOccurenceOf( openers );
    }

    public void event( T event )
    {
        if ( opener.accept( event ) )
        {
            triggered( event );
        }
    }

    protected abstract void triggered( T event );

    @SuppressWarnings( "rawtypes" )
    private static <T> Switch<T> firstOccurenceOf( final Switch... filters )
    {
        for ( Switch filter : filters )
        {
            filter.reset();
        }

        return new Switch<T>()
        {
            @SuppressWarnings( "unchecked" )
            @Override
            public synchronized boolean accept( T item )
            {
                boolean accepted = false;
                // Pass it through all since they are probably stateful
                for ( Switch<T> filter : filters )
                {
                    if ( filter.accept( item ) )
                    {
                        accepted = true;
                    }
                }

                if ( accepted )
                {
                    reset();
                }
                return accepted;
            }

            @SuppressWarnings( "unchecked" )
            @Override
            public synchronized void reset()
            {
                for ( Switch<T> filter : filters )
                {
                    filter.reset();
                }
            }
        };
    }

    public static <T> Switch<T> time( final long time, TimeUnit unit )
    {
        return time( Clock.SYSTEM_CLOCK, time, unit );
    }

    public static <T> Switch<T> time( final Clock clock, long time, TimeUnit unit )
    {
        final long timeMillis = unit.toMillis( time );
        return new Switch<T>()
        {
            private long lastSeen;

            @Override
            public boolean accept( T item )
            {
                return clock.currentTimeMillis()-lastSeen >= timeMillis;
            }

            @Override
            public void reset()
            {
                lastSeen = clock.currentTimeMillis();
            }
        };
    }

    public static <T> Switch<T> count( final long maxCount )
    {
        return new Switch<T>()
        {
            private long count;

            @Override
            public boolean accept( T item )
            {
                return ++count >= maxCount;
            }

            @Override
            public void reset()
            {
                count = 0;
            }
        };
    }

    public static <T> Switch<T> differentItems()
    {
        return new Switch<T>()
        {
            private T lastSeenItem;

            @Override
            public boolean accept( T item )
            {
                boolean accepted = lastSeenItem == null || !lastSeenItem.equals( item );
                lastSeenItem = item;
                return accepted;
            }

            @Override
            public void reset()
            {   // Don't reset
            }
        };
    }

    public static <T> Switch<T> differentItemClasses()
    {
        return new Switch<T>()
        {
            private Class lastSeenItemClass;

            @Override
            public boolean accept( T item )
            {
                boolean accepted = lastSeenItemClass == null || !lastSeenItemClass.equals( item.getClass() );
                lastSeenItemClass = item.getClass();
                return accepted;
            }

            @Override
            public void reset()
            {   // Don't reset
            }
        };
    }
}
