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
package org.neo4j.kernel.impl.util.collection;

import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;

/**
 * {@link Cursor} which moves over one or more arrays, automatically transitioning to the next
 * array when one runs out of items.
 */
public class ContinuableArrayCursor<T> implements Cursor<T>
{
    private final Supplier<T[]> supplier;
    private T[] current;
    private int cursor;

    public ContinuableArrayCursor( Supplier<T[]> supplier )
    {
        this.supplier = supplier;
    }

    @Override
    public boolean next()
    {
        while ( current == null || cursor >= current.length )
        {
            current = supplier.get();
            if ( current == null )
            {   // End reached
                return false;
            }

            cursor = 0;
        }
        cursor++;
        return true;
    }

    @Override
    public void close()
    {
        // Do nothing
    }

    @Override
    public T get()
    {
        if ( current == null )
        {
            throw new IllegalStateException();
        }
        return current[cursor - 1];
    }
}
