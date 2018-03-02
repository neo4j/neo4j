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

/**
 * Helper cursor for traversing specific types and directions of a dense node.
 */
public final class RelationshipDenseSelectionCursor extends RelationshipDenseSelection implements RelationshipSelectionCursor
{
    @Override
    public boolean next()
    {
        if ( !fetchNext() )
        {
            close();
            return false;
        }
        return true;
    }

    @Override
    public long relationshipReference()
    {
        return relationshipCursor.relationshipReference();
    }

    @Override
    public int type()
    {
        return relationshipCursor.label();
    }

    @Override
    public long otherNodeReference()
    {
        return relationshipCursor.originNodeReference() == relationshipCursor.sourceNodeReference() ?
               relationshipCursor.targetNodeReference() : relationshipCursor.sourceNodeReference();
    }

    @Override
    public long sourceNodeReference()
    {
        return relationshipCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return relationshipCursor.targetNodeReference();
    }
}
