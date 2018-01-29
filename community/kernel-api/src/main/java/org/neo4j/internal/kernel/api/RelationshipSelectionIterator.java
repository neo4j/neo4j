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
package org.neo4j.internal.kernel.api;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.ResourceIterator;

/**
 * Iterator for traversing selected relationships of a single node, based on type and direction.
 */
public abstract class RelationshipSelectionIterator<R> implements ResourceIterator<R>
{
    public interface RelationshipFactory<R>
    {
        R relationship( long id, long startNodeId, int typeId, long endNodeId );
    }

    private R _next;
    private boolean initialized = false;

    @Override
    public boolean hasNext()
    {
        if ( !initialized )
        {
            _next = fetchNext();
            initialized = true;
        }

        if ( _next == null )
        {
            close();
            return false;
        }
        return true;
    }

    @Override
    public R next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        R current = _next;
        _next = fetchNext();
        return current;
    }

    protected abstract R fetchNext();
}
