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

import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

/**
 * Helper for traversing specific types and directions of a dense node.
 */
abstract class RelationshipDenseSelection
{
    private enum Dir
    {
        OUT,
        IN,
        LOOP
    }

    private RelationshipGroupCursor groupCursor;
    protected RelationshipTraversalCursor relationshipCursor;
    private int[] types;
    private Dir[] directions;
    private int currentDirection;
    private int nDirections;
    private boolean onRelationship;
    private boolean onGroup;
    private int foundTypes;

    RelationshipDenseSelection()
    {
        this.directions = new Dir[3];
    }

    /**
     * Traverse all outgoing relationships including loops of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     */
    public final void outgoing(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor )
    {
        outgoing( groupCursor, relationshipCursor, null );
    }

    /**
     * Traverse all outgoing relationships including loops of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     * @param types Relationship types to traverse
     */
    public final void outgoing(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        this.groupCursor = groupCursor;
        this.relationshipCursor = relationshipCursor;
        this.types = types;
        this.directions[0] = Dir.OUT;
        this.directions[1] = Dir.LOOP;
        this.nDirections = 2;
        this.currentDirection = directions.length;
        this.onRelationship = false;
        this.onGroup = false;
        this.foundTypes = 0;
    }

    /**
     * Traverse all incoming relationships including loops of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     */
    public final void incoming(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor )
    {
        incoming( groupCursor, relationshipCursor, null );
    }

    /**
     * Traverse all incoming relationships including loops of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     * @param types Relationship types to traverse
     */
    public final void incoming(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        this.groupCursor = groupCursor;
        this.relationshipCursor = relationshipCursor;
        this.types = types;
        this.directions[0] = Dir.IN;
        this.directions[1] = Dir.LOOP;
        this.nDirections = 2;
        this.currentDirection = directions.length;
        this.onRelationship = false;
        this.onGroup = false;
        this.foundTypes = 0;
    }

    /**
     * Traverse all relationships of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     */
    public final void all(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor )
    {
        all( groupCursor, relationshipCursor, null );
    }

    /**
     * Traverse all relationships of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     * @param types Relationship types to traverse
     */
    public final void all(
            RelationshipGroupCursor groupCursor,
            RelationshipTraversalCursor relationshipCursor,
            int[] types )
    {
        this.groupCursor = groupCursor;
        this.relationshipCursor = relationshipCursor;
        this.types = types;
        this.directions[0] = Dir.OUT;
        this.directions[1] = Dir.IN;
        this.directions[2] = Dir.LOOP;
        this.nDirections = 3;
        this.currentDirection = directions.length;
        this.onRelationship = false;
        this.onGroup = false;
        this.foundTypes = 0;
    }

    /**
     * Fetch the next valid relationship.
     *
     * @return True is a valid relationship was found
     */
    protected boolean fetchNext()
    {
        if ( onRelationship )
        {
            onRelationship = relationshipCursor.next();
        }

        while ( !onRelationship )
        {
            currentDirection++;
            if ( currentDirection >= nDirections )
            {
                if ( types != null && foundTypes >= types.length )
                {
                    onGroup = false;
                    return false;
                }

                loopOnRelationship();
            }

            if ( !onGroup )
            {
                return false;
            }

            setupCursors();
        }

        return true;
    }

    private void loopOnRelationship()
    {
        do
        {
            onGroup = groupCursor.next();
        } while ( onGroup && !correctRelationshipType() );

        if ( onGroup )
        {
            foundTypes++;
            currentDirection = 0;
        }
    }

    private void setupCursors()
    {
        Dir d = directions[currentDirection];

        switch ( d )
        {
        case OUT:
            groupCursor.outgoing( relationshipCursor );
            onRelationship = relationshipCursor.next();
            break;

        case IN:
            groupCursor.incoming( relationshipCursor );
            onRelationship = relationshipCursor.next();
            break;

        case LOOP:
            groupCursor.loops( relationshipCursor );
            onRelationship = relationshipCursor.next();
            break;

        default:
            throw new IllegalStateException( "Lorem ipsus, Brutus. (could not setup cursor for Dir='" + d + "')" );
        }
    }

    private boolean correctRelationshipType()
    {
        return types == null || ArrayUtils.contains( types, groupCursor.type() );
    }

    public void close()
    {
        Throwable closeGroupError = null;
        try
        {
            if ( groupCursor != null )
            {
                groupCursor.close();
            }
        }
        catch ( Throwable t )
        {
            closeGroupError = t;
        }

        try
        {
            if ( relationshipCursor != null )
            {
                relationshipCursor.close();
            }
        }
        catch ( Throwable t )
        {
            if ( closeGroupError != null )
            {
                t.addSuppressed( closeGroupError );
            }
            throw t;
        }
        finally
        {
            relationshipCursor = null;
            groupCursor = null;
        }
    }
}
