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

import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.NOT_INITIALIZED;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.NO_ID;

/**
 * Helper iterator for traversing specific types and directions of a dense node.
 */
public final class RelationshipDenseSelectionIterator<R> extends RelationshipDenseSelection
        implements ResourceIterator<R>
{
    private RelationshipFactory<R> factory;
    private long _next = NOT_INITIALIZED;

    RelationshipDenseSelectionIterator( RelationshipFactory<R> factory )
    {
        this.factory = factory;
    }

    @Override
    public boolean hasNext()
    {
        if ( _next == NOT_INITIALIZED )
        {
            fetchNext();
            _next = relationshipCursor.relationshipReference();
        }

        if ( _next == NO_ID )
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
        R current = factory.relationship( relationshipCursor.relationshipReference(),
                relationshipCursor.sourceNodeReference(),
                relationshipCursor.label(), relationshipCursor.targetNodeReference() );
        if ( !fetchNext() )
        {
            _next = NO_ID;
        }
        _next = relationshipCursor.relationshipReference();

        return current;
    }
}
