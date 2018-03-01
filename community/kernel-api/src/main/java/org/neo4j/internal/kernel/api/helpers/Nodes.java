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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;

/**
 * Helper methods for working with nodes
 */
public final class Nodes
{
    private Nodes()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    /**
     * Counts the number of outgoing relationships from node where the cursor is positioned.
     * <p>
     * NOTE: The number of outgoing relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @return the number of outgoing - including loops - relationships from the node
     */
    public static int countOutgoing( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.outgoingCount() + group.loopCount();
        }
        return count;
    }

    /**
     * Counts the number of incoming relationships from node where the cursor is positioned.
     * <p>
     * NOTE: The number of incoming relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @return the number of incoming - including loops - relationships from the node
     */
    public static int countIncoming( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.incomingCount() + group.loopCount();
        }
        return count;
    }

    /**
     * Counts all the relationships from node where the cursor is positioned.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @return the number of relationships from the node
     */
    public static int countAll( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.totalCount();
        }
        return count;
    }

    /**
     * Counts the number of outgoing relationships of the given type from node where the cursor is positioned.
     * <p>
     * NOTE: The number of outgoing relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @param type the type of the relationship we're counting
     * @return the number of outgoing - including loops - relationships from the node with the given type
     */
    public static int countOutgoing( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        nodeCursor.relationships( group );
        while ( group.next() )
        {
            if ( group.type() == type )
            {
                return group.outgoingCount() + group.loopCount();
            }
        }
        return 0;
    }

    /**
     * Counts the number of incoming relationships of the given type from node where the cursor is positioned.
     * <p>
     * NOTE: The number of incoming relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @param type the type of the relationship we're counting
     * @return the number of incoming - including loops - relationships from the node with the given type
     */
    public static int countIncoming( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            if ( group.type() == type )
            {
                return group.incomingCount() + group.loopCount();
            }
        }
        return count;
    }

    /**
     * Counts all the relationships of the given type from node where the cursor is positioned.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param group a group cursor
     * @param type the type of the relationship we're counting
     * @return the number relationships from the node with the given type
     */
    public static int countAll( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            if ( group.type() == type )
            {
                return group.totalCount();
            }
        }
        return count;
    }
}
