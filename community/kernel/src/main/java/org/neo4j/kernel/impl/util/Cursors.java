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
package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.IOCursor;
import org.neo4j.cursor.IntCursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;

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
   private static IntCursor EMPTY_INT = new IntCursor()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public int getAsInt()
        {
            throw new IllegalStateException( "no elements" );
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

    @SuppressWarnings("unchecked")
    public static IntCursor emptyInt()
    {
        return EMPTY_INT;
    }

    public static <T> Cursor<T> cursor( final T... items )
    {
        return cursor( Iterables.asIterable( items ) );
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

    public static TransactionCursor txCursor( Cursor<CommittedTransactionRepresentation> cursor )
    {
        return new TransactionCursor()
        {
            @Override
            public LogPosition position()
            {
                throw new UnsupportedOperationException(
                        "LogPosition does not apply when moving a generic cursor over a list of transactions" );
            }

            @Override
            public boolean next() throws IOException
            {
                return cursor.next();
            }

            @Override
            public void close() throws IOException
            {
                cursor.close();
            }

            @Override
            public CommittedTransactionRepresentation get()
            {
                return cursor.get();
            }
        };
    }

    public static <T> IOCursor<T> io( Cursor<T> cursor )
    {
        return new IOCursor<T>()
        {
            @Override
            public boolean next() throws IOException
            {
                return cursor.next();
            }

            @Override
            public void close() throws IOException
            {
                cursor.close();
            }

            @Override
            public T get()
            {
                return cursor.get();
            }
        };
    }

    public static IntCursor intCursor( int... integers )
    {
        return intCursor( Iterables.asIterable( integers ) );
    }

    private static IntCursor intCursor( Iterable<Integer> integers )
    {
        return new IntCursor()
        {
            Iterator<Integer> iterator = integers.iterator();

            boolean valid = false;
            int current;

            @Override
            public boolean next()
            {
                if ( iterator.hasNext() )
                {
                    valid = true;
                    current = iterator.next();
                    return true;
                }
                else
                {
                    valid = false;
                    return false;
                }
            }

            @Override
            public void close()
            {
                iterator = integers.iterator();
                valid = false;
            }

            @Override
            public int getAsInt()
            {
                if ( valid )
                {
                    return current;
                }

                throw new IllegalStateException();
            }
        };
    }
}
