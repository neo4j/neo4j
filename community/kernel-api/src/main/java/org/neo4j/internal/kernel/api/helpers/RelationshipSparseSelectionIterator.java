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
package org.neo4j.internal.kernel.api.helpers;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.ResourceIterator;

/**
 * Helper iterator for traversing specific types and directions of a sparse node.
 */
public final class RelationshipSparseSelectionIterator<R> extends RelationshipSparseSelection
        implements ResourceIterator<R>
{

    private final RelationshipFactory<R> factory;
    private R _next;
    private boolean initialized;

    public RelationshipSparseSelectionIterator( RelationshipFactory<R> factory )
    {
        this.factory = factory;
        this.initialized = false;
    }

    @Override
    public boolean hasNext()
    {
        if ( !initialized )
        {
            fetchNext();
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
        if ( !fetchNext() )
        {
            _next = null;
        }
        return current;
    }

    @Override
    protected void setRelationship( long id, long sourceNode, int type, long targetNode )
    {
        _next = factory.relationship( id, sourceNode, type, targetNode );
    }
}
