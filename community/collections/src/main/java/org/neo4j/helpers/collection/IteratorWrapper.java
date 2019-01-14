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

/**
 * Wraps an {@link Iterator} so that it returns items of another type. The
 * iteration is done lazily.
 *
 * @param <T> the type of items to return
 * @param <U> the type of items to wrap/convert from
 */
public abstract class IteratorWrapper<T, U> implements Iterator<T>
{
    private Iterator<U> source;

    public IteratorWrapper( Iterator<U> iteratorToWrap )
    {
        this.source = iteratorToWrap;
    }

    @Override
    public boolean hasNext()
    {
        return this.source.hasNext();
    }

    @Override
    public T next()
    {
        return underlyingObjectToObject( this.source.next() );
    }

    @Override
    public void remove()
    {
        this.source.remove();
    }

    protected abstract T underlyingObjectToObject( U object );
}
