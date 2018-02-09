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
package org.neo4j.storageengine.api.schema;

import java.util.Iterator;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.IOUtils;

public final class IndexProgressors
{
    private IndexProgressors()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static IndexProgressor concat( IndexProgressor... progressors )
    {
        return new IndexProgressor()
        {
            private int current;
            private volatile boolean closed;

            @Override
            public boolean next()
            {
                while ( !closed && current < progressors.length )
                {
                    if ( progressors[current].next() )
                    {
                        return true;
                    }
                    current++;
                }
                return false;
            }

            @Override
            public void close()
            {
                if ( !closed )
                {
                    closed = true;
                    IOUtils.closeAllSilently( progressors );
                }
            }
        };
    }

    public static IndexProgressor concat( Iterable<IndexProgressor> progressors )
    {
        return new IndexProgressor()
        {
            private Iterator<IndexProgressor> iterator = progressors.iterator();
            private IndexProgressor current;
            private volatile boolean closed;

            @Override
            public boolean next()
            {
                if ( current == null && !iterator.hasNext() )
                {
                    return false;
                }
                else if ( current == null )
                {
                    current = iterator.next();
                }

                while ( !closed )
                {
                    if ( current.next() )
                    {
                        return true;
                    }
                    else if ( iterator.hasNext() )
                    {
                        current = iterator.next();
                    }
                    else
                    {
                        return false;
                    }
                }
                return false;
            }

            @Override
            public void close()
            {
                if ( !closed )
                {
                    closed = true;
                    IOUtils.closeAllSilently( Iterables.asCollection( progressors ) );
                }
            }
        };
    }
}
