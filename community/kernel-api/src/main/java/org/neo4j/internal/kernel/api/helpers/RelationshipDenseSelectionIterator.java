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
package org.neo4j.internal.kernel.api.helpers;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.ResourceIterator;

/**
 * Helper iterator for traversing specific types and directions of a dense node.
 */
public final class RelationshipDenseSelectionIterator<R> extends RelationshipDenseSelection
        implements ResourceIterator<R>
{
    private RelationshipFactory<R> factory;
    private long next;

    RelationshipDenseSelectionIterator( RelationshipFactory<R> factory )
    {
        this.factory = factory;
        this.next = RelationshipSelections.UNINITIALIZED;
    }

    @Override
    public boolean hasNext()
    {
        if ( next == RelationshipSelections.UNINITIALIZED )
        {
            fetchNext();
            next = relationshipCursor.relationshipReference();
        }

        if ( next == RelationshipSelections.NO_ID )
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
        R current = factory.relationship( next,
                                          relationshipCursor.sourceNodeReference(),
                                          relationshipCursor.type(),
                                          relationshipCursor.targetNodeReference() );

        if ( !fetchNext() )
        {
            close();
            next = RelationshipSelections.NO_ID;
        }
        else
        {
            next = relationshipCursor.relationshipReference();
        }

        return current;
    }
}
