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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.helpers.Predicate;

/**
 * {@link IOCursor} implementation that uses a predicate to decide on what to keep and what to skip
 */
public class FilteringIOCursor<T> implements IOCursor<T>
{
    private final IOCursor<T> delegate;
    private final Predicate<T> toKeep;

    public FilteringIOCursor( IOCursor<T> delegate, Predicate<T> toKeep )
    {
        this.delegate = delegate;
        this.toKeep = toKeep;
    }

    @Override
    public T get()
    {
        return delegate.get();
    }

    @Override
    public boolean next() throws IOException
    {
        do
        {
            if ( !delegate.next() )
            {
                return false;
            }
        } while ( !toKeep.accept( delegate.get() ) );
        return true;
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
