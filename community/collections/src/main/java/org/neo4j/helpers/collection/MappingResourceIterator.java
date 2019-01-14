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

import org.neo4j.graphdb.ResourceIterator;

public abstract class MappingResourceIterator<T, S> implements ResourceIterator<T>
{
    private ResourceIterator<S> sourceIterator;

    public MappingResourceIterator( ResourceIterator<S> sourceResourceIterator )
    {
        this.sourceIterator = sourceResourceIterator;
    }

    protected abstract T map( S object );

    @Override
    public boolean hasNext()
    {
        return sourceIterator.hasNext();
    }

    @Override
    public T next()
    {
        return map( sourceIterator.next() );
    }

    @Override
    public void remove()
    {
        sourceIterator.remove();
    }

    @Override
    public void close()
    {
        sourceIterator.close();
    }
}
