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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;

import org.neo4j.cursor.IOCursor;

public class IOCursors
{
    private static IOCursor<Object> EMPTY = new IOCursor<Object>()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public Object get()
        {
            return null;
        }

        @Override
        public void close()
        {

        }
    };

    @SuppressWarnings("unchecked")
    public static <T> IOCursor<T> empty()
    {
        return (IOCursor<T>) EMPTY;
    }

    @SafeVarargs
    public static <T> IOCursor<T> cursor( final T... items )
    {
        return new IOCursor<T>()
        {
            int idx = 0;
            T current;

            @Override
            public boolean next()
            {
                if ( idx < items.length )
                {
                    current = items[idx++];
                    return true;
                }
                else
                {
                    return false;
                }
            }

            @Override
            public void close()
            {
                idx = 0;
                current = null;
            }

            @Override
            public T get()
            {
                if ( current == null )
                {
                    throw new IllegalStateException();
                }

                return current;
            }
        };
    }

    public static <T> IOCursor<T> cursor( final Iterable<T> items )
    {
        return new IOCursor<T>()
        {
            Iterator<T> iterator = items.iterator();

            T current;

            @Override
            public boolean next()
            {
                if ( iterator.hasNext() )
                {
                    current = iterator.next();
                    return true;
                }
                else
                {
                    return false;
                }
            }

            @Override
            public void close()
            {
                iterator = items.iterator();
                current = null;
            }

            @Override
            public T get()
            {
                if ( current == null )
                {
                    throw new IllegalStateException();
                }

                return current;
            }
        };
    }
}
