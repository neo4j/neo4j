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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.ToIntFunction;
import org.neo4j.graphdb.Resource;

public class Cursors
{
    private static Cursor<Object> EMPTY = new Cursor<Object>()
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
    public static <T> Cursor<T> empty()
    {
        return (Cursor<T>) EMPTY;
    }

    public static <T> Cursor<T> cursor( final T... items )
    {
        return new Cursor<T>()
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

    public static <T> Cursor<T> cursor( final Iterable<T> items )
    {
        return new Cursor<T>()
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

    public static <T> PrimitiveIntIterator intIterator( final Cursor<T> resourceCursor, final ToIntFunction<T> map )
    {
        return new CursorPrimitiveIntIterator<>( resourceCursor, map );
    }

    private static class CursorPrimitiveIntIterator<T> implements PrimitiveIntIterator, Resource
    {
        private final ToIntFunction<T> map;
        private Cursor<T> cursor;
        private boolean hasNext;

        public CursorPrimitiveIntIterator( Cursor<T> resourceCursor, ToIntFunction<T> map )
        {
            this.map = map;
            cursor = resourceCursor;
            hasNext = nextCursor();
        }

        private boolean nextCursor()
        {
            if ( cursor != null )
            {
                boolean hasNext = cursor.next();
                if ( !hasNext )
                {
                    close();
                }
                return hasNext;
            }
            else
            {
                return false;
            }
        }

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public int next()
        {
            if ( hasNext )
            {
                try
                {
                    return map.apply( cursor.get() );
                }
                finally
                {
                    hasNext = nextCursor();
                }
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void close()
        {
            if ( cursor != null )
            {
                cursor.close();
                cursor = null;
            }
        }
    }


}
