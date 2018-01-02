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

/**
 * Dresses up an array in an {@link Iterator} costume. Can be reused for multiple arrays.
 */
public class ReusableIteratorCostume<T> implements Iterator<T>
{
    private T[] items;
    private int cursor;
    private int offset;
    private int length;

    public Iterator<T> dressArray( T[] items, int offset, int length )
    {
        this.items = items;
        this.offset = offset;
        this.length = length;
        this.cursor = 0;
        return this;
    }

    @Override
    public boolean hasNext()
    {
        return cursor < length;
    }

    @Override
    public T next()
    {
        return items[offset+cursor++];
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
