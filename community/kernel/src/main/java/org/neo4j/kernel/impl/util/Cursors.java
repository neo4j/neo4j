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

import java.io.IOException;
import java.util.Collections;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.transaction.log.IOCursor;

public class Cursors
{
    public static <T> ResourceIterable<T> iterable(final IOCursor<T> cursor)
    {
        return new ResourceIterable<T>()
        {
            @Override
            public ResourceIterator<T> iterator()
            {
                try
                {
                    if (cursor.next())
                    {
                        final T first = cursor.get();

                        return new ResourceIterator<T>()
                        {
                            T instance = first;

                            @Override
                            public boolean hasNext()
                            {
                                return instance != null;
                            }

                            @Override
                            public T next()
                            {
                                try
                                {
                                    return instance;
                                }
                                finally
                                {
                                    try
                                    {
                                        if (cursor.next())
                                        {
                                            instance = cursor.get();
                                        } else
                                        {
                                            cursor.close();
                                            instance = null;
                                        }
                                    }
                                    catch ( IOException e )
                                    {
                                        instance = null;
                                    }
                                }
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException(  );
                            }

                            @Override
                            public void close()
                            {
                                try
                                {
                                    cursor.close();
                                }
                                catch ( IOException e )
                                {
                                    // Ignore
                                }
                            }
                        };
                    } else
                    {
                        cursor.close();
                        return IteratorUtil.<T>asResourceIterator( Collections.<T>emptyIterator());
                    }
                }
                catch ( IOException e )
                {
                    return IteratorUtil.<T>asResourceIterator( Collections.<T>emptyIterator());
                }
            }
        };
    }

    public static Cursor countDownCursor( final int count )
    {
        return new CountDownCursor( count );
    }

    public static class CountDownCursor implements Cursor
    {
        private final int count;
        private int current;

        public CountDownCursor( int count )
        {
            this.count = count;
            current = count;
        }

        @Override
        public boolean next()
        {
            return current-- > 0;
        }

        @Override
        public void reset()
        {
            current = count;
        }

        @Override
        public void close()
        {
            current = 0;
        }
    }
}
