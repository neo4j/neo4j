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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.cursor.IOCursor;

/**
 * {@link IOCursor} abstraction over a given array
 */
public class ArrayIOCursor<T> implements IOCursor<T>
{
    private final T[] entries;
    private int pos;
    private boolean closed;

    public ArrayIOCursor( T... entries )
    {
        this.entries = entries;
    }

    @Override
    public T get()
    {
        assert !closed;
        return entries[pos - 1];
    }

    @Override
    public boolean next()
    {
        assert !closed;
        return pos++ < entries.length;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
