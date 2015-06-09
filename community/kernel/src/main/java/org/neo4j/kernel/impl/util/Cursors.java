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
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Function;
import org.neo4j.function.ToIntFunction;
import org.neo4j.graphdb.Resource;
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

    public static <T,C extends Cursor> ResourceIterator<T> iterator( final C resourceCursor, final Function<C, T> map)
    {
        return new CursorResourceIterator<>( resourceCursor, map );
    }

    public static <C extends Cursor> PrimitiveIntIterator intIterator( final C resourceCursor, final ToIntFunction<C> map)
    {
        return new CursorPrimitiveIntIterator<>( resourceCursor, map );
    }

    private static class CursorPrimitiveIntIterator<C extends Cursor> implements PrimitiveIntIterator, Resource
    {
        private final ToIntFunction<C> map;
        private C cursor;
        private boolean hasNext;

        public CursorPrimitiveIntIterator( C resourceCursor, ToIntFunction<C> map )
        {
            this.map = map;
            cursor = resourceCursor;
            hasNext = nextCursor();
        }

        private boolean nextCursor()
        {
            if (cursor != null)
            {
                boolean hasNext = cursor.next();
                if ( !hasNext )
                {
                    close();
                }
                return hasNext;
            } else
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
                    return map.apply( cursor );
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
            if (cursor != null )
            {
                cursor.close();
                cursor = null;
            }
        }
    }

    private static class CursorResourceIterator<T, C extends Cursor> implements ResourceIterator<T>
    {
        private final Function<C, T> map;
        private C cursor;
        private boolean hasNext;

        public CursorResourceIterator( C resourceCursor, Function<C, T> map )
        {
            this.map = map;
            cursor = resourceCursor;
            hasNext = nextCursor();
        }

        private boolean nextCursor()
        {
            if (cursor != null)
            {
                boolean hasNext = cursor.next();
                if ( !hasNext )
                {
                    close();
                }
                return hasNext;
            } else
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
        public T next()
        {
            if ( hasNext )
            {
                try
                {
                    return map.apply( cursor );
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
        public void remove()
        {
            throw new UnsupportedOperationException(  );
        }

        @Override
        public void close()
        {
            if (cursor != null )
            {
                cursor.close();
                cursor = null;
            }
        }
    }
}
