/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.cleanup;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.ResourceIterator;

class AutoCleanupResourceIterator<T> implements ResourceIterator<T>
{
    private final Iterator<T> iterator;
    CleanupReference cleanup;

    public AutoCleanupResourceIterator( Iterator<T> iterator )
    {
        this.iterator = iterator;
    }

    @Override
    public void close()
    {
        try
        {
            cleanup.cleanupNow( true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Exception when closing.", e );
        }
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = iterator.hasNext();
        if ( !hasNext )
        {
            close();
        }
        return hasNext;
    }

    @Override
    public T next()
    {
        try
        {
            return iterator.next();
        }
        catch ( NoSuchElementException e )
        {
            close();
            throw e;
        }
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }
}
