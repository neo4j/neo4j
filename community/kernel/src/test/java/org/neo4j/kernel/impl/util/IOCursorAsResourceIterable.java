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

import java.io.IOException;
import java.util.Collections;

import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;

public class IOCursorAsResourceIterable<T> implements ResourceIterable<T>
{
    private final IOCursor<T> cursor;

    public IOCursorAsResourceIterable( IOCursor<T> cursor )
    {
        this.cursor = cursor;
    }

    @Override
    public ResourceIterator<T> iterator()
    {
        try
        {
            if ( cursor.next() )
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
                                if ( cursor.next() )
                                {
                                    instance = cursor.get();
                                }
                                else
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
                        throw new UnsupportedOperationException();
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
            }

            cursor.close();
            return Iterators.asResourceIterator( Collections.emptyIterator() );
        }
        catch ( IOException e )
        {
            return Iterators.asResourceIterator( Collections.emptyIterator() );
        }
    }
}
