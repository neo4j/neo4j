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
package org.neo4j.helpers.collection;

import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;

public abstract class NestingResourceIterator<T, U> extends PrefetchingResourceIterator<T>
{
    private final Iterator<U> source;
    private ResourceIterator<T> currentNestedIterator;
    private U currentSurfaceItem;

    public NestingResourceIterator( Iterator<U> source )
    {
        this.source = source;
    }

    protected abstract ResourceIterator<T> createNestedIterator( U item );

    @Override
    protected T fetchNextOrNull()
    {
        if ( currentNestedIterator == null ||
                !currentNestedIterator.hasNext() )
        {
            while ( source.hasNext() )
            {
                currentSurfaceItem = source.next();
                close();
                currentNestedIterator = createNestedIterator( currentSurfaceItem );
                if ( currentNestedIterator.hasNext() )
                {
                    break;
                }
            }
        }
        return currentNestedIterator != null && currentNestedIterator.hasNext() ? currentNestedIterator.next() : null;
    }

    @Override
    public void close()
    {
        if ( currentNestedIterator != null )
        {
            currentNestedIterator.close();
        }
    }
}
