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
package org.neo4j.kernel.impl.core;

import org.apache.commons.lang3.ArrayUtils;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

public class RelationshipConversion implements ResourceIterator<Relationship>
{
    private final EmbeddedProxySPI actions;
    private final RelationshipTraversalCursor cursor;
    private final Direction direction;
    private final int[] typeIds;
    private Relationship next;
    private boolean closed;

    public RelationshipConversion(
            EmbeddedProxySPI actions,
            RelationshipTraversalCursor cursor,
            Direction direction,
            int[] typeIds)
    {
        this.actions = actions;
        this.cursor = cursor;
        this.direction = direction;
        this.typeIds = typeIds;
    }

    @Override
    public boolean hasNext()
    {
        if ( next == null && !closed )
        {
            while ( cursor.next() )
            {
                if ( correctDirection() && correctType() )
                {
                    next = actions.newRelationshipProxy(
                            cursor.relationshipReference(),
                            cursor.sourceNodeReference(),
                            cursor.label(),
                            cursor.targetNodeReference() );
                    return true;
                }
            }
            close();
        }
        return next != null;
    }

    private boolean correctDirection()
    {
        return direction == Direction.BOTH ||
                (direction == Direction.OUTGOING && cursor.originNodeReference() == cursor.sourceNodeReference()) ||
                (direction == Direction.INCOMING && cursor.originNodeReference() == cursor.targetNodeReference());
    }

    private boolean correctType()
    {
        return typeIds == null || ArrayUtils.contains( typeIds, cursor.label() );
    }

    @Override
    public Relationship next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        Relationship current = next;
        next = null;
        return current;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            cursor.close();
            closed = true;
        }
    }
}
