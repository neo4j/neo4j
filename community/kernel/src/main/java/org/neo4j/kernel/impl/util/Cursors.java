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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.RawCursor;

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
            throw new IllegalStateException( "no elements" );
        }

        @Override
        public void close()
        {
        }
    };

    @SuppressWarnings( "unchecked" )
    private Cursors()
    {
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Cursor<T> empty()
    {
        return (Cursor<T>) EMPTY;
    }

    public static <E extends Exception> int count( RawCursor<?,E> cursor ) throws E
    {
        try
        {
            int count = 0;
            while ( cursor.next() )
            {
                count++;
            }
            return count;
        }
        finally
        {
            cursor.close();
        }
    }

    public static <T, EX extends Exception> RawCursor<T,EX> rawCursorOf( T... values )
    {
        return new RawCursor<T,EX>()
        {
            private int idx;
            private CursorValue<T> current = new CursorValue<>();

            @Override
            public T get()
            {
                return current.get();
            }

            @Override
            public boolean next() throws EX
            {
                if ( idx >= values.length )
                {
                    current.invalidate();
                    return false;
                }

                current.set( values[idx] );
                idx++;

                return true;
            }

            @Override
            public void close() throws EX
            {
                idx = values.length;
                current.invalidate();
            }
        };
    }

    public static <T, EX extends Exception> RawCursor<T,EX> rawCursorOf( Iterable<T> iterable )
    {
        return new RawCursor<T,EX>()
        {
            private CursorValue<T> current = new CursorValue<>();
            private Iterator<T> itr = iterable.iterator();

            @Override
            public T get()
            {
                return current.get();
            }

            @Override
            public boolean next() throws EX
            {
                if ( itr.hasNext() )
                {
                    current.set( itr.next() );
                    return true;
                }
                else
                {
                    current.invalidate();
                    return false;
                }
            }

            @Override
            public void close() throws EX
            {
                current.invalidate();
            }
        };
    }
}
