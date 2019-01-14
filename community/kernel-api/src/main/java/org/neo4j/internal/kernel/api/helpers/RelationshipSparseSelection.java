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

import org.apache.commons.lang3.ArrayUtils;

import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

/**
 * Helper for traversing specific types and directions of a sparse node.
 */
abstract class RelationshipSparseSelection
{
    private enum Dir
    {
        OUT,
        IN,
        BOTH
    }

    protected RelationshipTraversalCursor cursor;
    private int[] types;
    private Dir targetDirection;
    private boolean onRelationship;
    private boolean firstNext;

    /**
     * Traverse all outgoing relationships including loops of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     */
    public final void outgoing( RelationshipTraversalCursor relationshipCursor )
    {
        init( relationshipCursor, null, Dir.OUT );
    }

    /**
     * Traverse all outgoing relationships including loops of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     * @param types Relationship types to traverse
     */
    public final void outgoing(
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        init( relationshipCursor, types, Dir.OUT );
    }

    /**
     * Traverse all incoming relationships including loops of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     */
    public final void incoming( RelationshipTraversalCursor relationshipCursor )
    {
        init( relationshipCursor, null, Dir.IN );
    }

    /**
     * Traverse all incoming relationships including loops of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     * @param types Relationship types to traverse
     */
    public final void incoming(
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        init( relationshipCursor, types, Dir.IN );
    }

    /**
     * Traverse all relationships of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     */
    public final void all( RelationshipTraversalCursor relationshipCursor )
    {
        init( relationshipCursor, null, Dir.BOTH );
    }

    /**
     * Traverse all relationships of the provided relationship types.
     *
     * @param relationshipCursor Relationship traversal cursor to use. Pre-initialized on node.
     * @param types Relationship types to traverse
     */
    public final void all(
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        init( relationshipCursor, types, Dir.BOTH );
    }

    private void init( RelationshipTraversalCursor relationshipCursor, int[] types, Dir targetDirection )
    {
        this.cursor = relationshipCursor;
        this.types = types;
        this.targetDirection = targetDirection;
        this.onRelationship = false;
        this.firstNext = true;
    }

    /**
     * Fetch the next valid relationship.
     *
     * @return True is a valid relationship was found
     */
    protected boolean fetchNext()
    {
        if ( onRelationship || firstNext )
        {
            firstNext = false;
            do
            {
                onRelationship = cursor.next();
            } while ( onRelationship && (!correctDirection() || !correctType()) );
        }

        return onRelationship;
    }

    private boolean correctDirection()
    {
        return targetDirection == Dir.BOTH ||
                (targetDirection == Dir.OUT && cursor.originNodeReference() == cursor.sourceNodeReference()) ||
                (targetDirection == Dir.IN && cursor.originNodeReference() == cursor.targetNodeReference());
    }

    private boolean correctType()
    {
        return types == null || ArrayUtils.contains( types, cursor.type() );
    }

    public void close()
    {
        try
        {
            if ( cursor != null )
            {
                cursor.close();
            }
        }
        finally
        {
            cursor = null;
        }
    }
}
