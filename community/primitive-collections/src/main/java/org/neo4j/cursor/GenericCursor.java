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
package org.neo4j.cursor;

/**
 * Generic base class for cursor where clients
 * can access the current state through the get method.
 * <p>
 * Subclasses must implement the {@link #next()} method and
 * set the current field to the next item.
 *
 * @param <T> the type of instances being iterated
 */
public abstract class GenericCursor<T>
        implements Cursor<T>
{
    protected T current;

    @Override
    public T get()
    {
        if ( current == null )
        {
            throw new IllegalStateException();
        }

        return current;
    }

    @Override
    public void close()
    {
        current = null;
    }
}
