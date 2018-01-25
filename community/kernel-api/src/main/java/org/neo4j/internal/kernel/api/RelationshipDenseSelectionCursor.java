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

import org.apache.commons.lang3.ArrayUtils;

/**
 * Helper cursor for traversing specific types and directions of a dense node.
 */
public class RelationshipDenseSelectionCursor implements RelationshipSelectionCursor
{
    private RelationshipGroupCursor groupCursor;
    private RelationshipTraversalCursor relationshipCursor;
    private int[] types;
    private Dir[] directions;
    private int currentDirection;
    private int nDirections;
    private boolean onRelationship;
    private boolean onGroup;
    private int foundTypes;

    private enum Dir
    {
        OUT,
        IN,
        LOOP
    }

    public RelationshipDenseSelectionCursor()
    {
        directions = new Dir[3];
    }

    /**
     * Traverse all outgoing relationships including loops of the provided relationship types.
     *
     * @param groupCursor Group cursor to use. Pre-initialized on node.
     * @param relationshipCursor Relationship traversal cursor to use.
     */
    public void outgoing(
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
    public void outgoing(
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
    public void incoming(
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
    public void incoming(
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
    public void all(
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
    public void all(
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

    @Override
    public boolean next()
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

            if ( !onGroup )
            {
                return false;
            }

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
                throw new IllegalStateException( "Lorem ipsus, Brutus" );
            }
        }
        return true;
    }

    private boolean correctRelationshipType()
    {
        return types == null || ArrayUtils.contains( types, groupCursor.relationshipLabel() );
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
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

    @Override
    public boolean isClosed()
    {
        return relationshipCursor == null;
    }

    @Override
    public long relationshipReference()
    {
        return relationshipCursor.relationshipReference();
    }

    @Override
    public int label()
    {
        return relationshipCursor.label();
    }

    @Override
    public boolean hasProperties()
    {
        return relationshipCursor.hasProperties();
    }

    @Override
    public void source( NodeCursor cursor )
    {
        relationshipCursor.source( cursor );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        relationshipCursor.target( cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        relationshipCursor.properties( cursor );
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

    @Override
    public long propertiesReference()
    {
        return relationshipCursor.propertiesReference();
    }
}
