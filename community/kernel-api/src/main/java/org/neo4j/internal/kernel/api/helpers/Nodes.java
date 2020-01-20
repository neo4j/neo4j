/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

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
     * @param cursors a factory for cursors
     * @param cursorTracer underlying page cursor tracer.
     * @return the number of outgoing - including loops - relationships from the node
     */
    public static int countOutgoing( NodeCursor nodeCursor, CursorFactory cursors, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
               return countOutgoingDense( nodeCursor, group );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
               return countOutgoingSparse( nodeCursor, traversal );
            }
        }
    }

    public static int countOutgoingDense( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.outgoingCount() + group.loopCount();
        }
        return count;
    }

    private static int countOutgoingSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            if ( traversal.sourceNodeReference() == nodeCursor.nodeReference() )
            {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of incoming relationships from node where the cursor is positioned.
     * <p>
     * NOTE: The number of incoming relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @param cursorTracer underlying page cursor tracer.
     * @return the number of incoming - including loops - relationships from the node
     */
    public static int countIncoming( NodeCursor nodeCursor, CursorFactory cursors, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
               return countIncomingDense( nodeCursor, group );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
              return countIncomingSparse( nodeCursor, traversal );
            }
        }
    }

    public static int countIncomingDense( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.incomingCount() + group.loopCount();
        }
        return count;
    }

    private static int countIncomingSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            if ( traversal.targetNodeReference() == nodeCursor.nodeReference() )
            {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts all the relationships from node where the cursor is positioned.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @param cursorTracer underlying page cursor tracer.
     * @return the number of relationships from the node
     */
    public static int countAll( NodeCursor nodeCursor, CursorFactory cursors, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
              return countAllDense( nodeCursor, group );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
                return countAllSparse( nodeCursor, traversal );
            }
        }
    }

    public static int countAllDense( NodeCursor nodeCursor, RelationshipGroupCursor group )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationships( group );
        int count = 0;
        while ( group.next() )
        {
            count += group.totalCount();
        }
        return count;
    }

    private static int countAllSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            count++;
        }
        return count;
    }

    /**
     * Counts the number of outgoing relationships of the given type from node where the cursor is positioned.
     * <p>
     * NOTE: The number of outgoing relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @param type the type of the relationship we're counting
     * @param cursorTracer underlying page cursor tracer.
     * @return the number of outgoing - including loops - relationships from the node with the given type
     */
    public static int countOutgoing( NodeCursor nodeCursor, CursorFactory cursors, int type, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
               return countOutgoingDense( nodeCursor, group, type );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
               return countOutgoingSparse( nodeCursor, traversal, type );
            }
        }
    }

    public static int countOutgoingDense( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        assert nodeCursor.isDense();
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

    private static int countOutgoingSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal, int type )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            if ( traversal.sourceNodeReference() == nodeCursor.nodeReference() && traversal.type() == type )
            {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of incoming relationships of the given type from node where the cursor is positioned.
     * <p>
     * NOTE: The number of incoming relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @param type the type of the relationship we're counting
     * @param cursorTracer underlying page cursor tracer.
     * @return the number of incoming - including loops - relationships from the node with the given type
     */
    public static int countIncoming( NodeCursor nodeCursor, CursorFactory cursors, int type, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
               return countIncomingDense( nodeCursor, group, type );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
               return countIncomingSparse( nodeCursor, traversal, type );
            }
        }
    }

    public static int countIncomingDense( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationships( group );
        while ( group.next() )
        {
            if ( group.type() == type )
            {
                return group.incomingCount() + group.loopCount();
            }
        }
        return 0;
    }

    private static int countIncomingSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal, int type )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            if ( traversal.targetNodeReference() == nodeCursor.nodeReference() && traversal.type() == type )
            {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts all the relationships of the given type from node where the cursor is positioned.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @param type the type of the relationship we're counting
     * @param cursorTracer underlying page cursor tracer.
     * @return the number relationships from the node with the given type
     */
    public static int countAll( NodeCursor nodeCursor, CursorFactory cursors, int type, PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
                return countAllDense( nodeCursor, group, type );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
               return countAllSparse( nodeCursor, traversal, type );
            }
        }
    }

    public static int countAllDense( NodeCursor nodeCursor, RelationshipGroupCursor group, int type )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationships( group );
        while ( group.next() )
        {
            if ( group.type() == type )
            {
                return group.totalCount();
            }
        }
        return 0;
    }

    private static int countAllSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal, int type )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.allRelationships( traversal );
        while ( traversal.next() )
        {
            if ( traversal.type() == type )
            {
                count++;
            }
        }
        return count;
    }
}
