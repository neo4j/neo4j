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

import java.util.function.ToLongFunction;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipSelection;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

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
        return count( nodeCursor, cursors, selection( OUTGOING ), RelationshipGroupCursor::outgoingCount, cursorTracer );
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
        return count( nodeCursor, cursors, selection( type, OUTGOING ), RelationshipGroupCursor::outgoingCount, cursorTracer );
    }

    /**
     * Counts the number of incoming relationships from node where the cursor is positioned.
     * <p>
     * NOTE: The number of incoming relationships also includes eventual loops.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @return the number of incoming - including loops - relationships from the node
     */
    public static int countIncoming( NodeCursor nodeCursor, CursorFactory cursors, PageCursorTracer cursorTracer )
    {
        return count( nodeCursor, cursors, selection( INCOMING ), RelationshipGroupCursor::incomingCount, cursorTracer );
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
        return count( nodeCursor, cursors, selection( type, INCOMING ),RelationshipGroupCursor::incomingCount, cursorTracer );
    }

    /**
     * Counts all the relationships from node where the cursor is positioned.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors a factory for cursors
     * @return the number of relationships from the node
     */
    public static int countAll( NodeCursor nodeCursor, CursorFactory cursors, PageCursorTracer cursorTracer )
    {
        return count( nodeCursor, cursors, selection( BOTH ), RelationshipGroupCursor::totalCount, cursorTracer );
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
        return count( nodeCursor, cursors, selection( type, BOTH ), RelationshipGroupCursor::totalCount, cursorTracer );
    }

    public static int count( NodeCursor nodeCursor, CursorFactory cursors, RelationshipSelection selection, ToLongFunction<RelationshipGroupCursor> counter,
            PageCursorTracer cursorTracer )
    {
        if ( nodeCursor.isDense() )
        {
            try ( RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor( cursorTracer ) )
            {
                return countDense( nodeCursor, group, selection, counter );
            }
        }
        else
        {
            try ( RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor( cursorTracer ) )
            {
                return countSparse( nodeCursor, traversal, selection );
            }
        }
    }

    public static int countDense( NodeCursor nodeCursor, RelationshipGroupCursor group, RelationshipSelection selection, Direction direction )
    {
        ToLongFunction<RelationshipGroupCursor> counter;
        switch ( direction )
        {
        case OUTGOING:
            counter = RelationshipGroupCursor::outgoingCount;
            break;
        case INCOMING:
            counter = RelationshipGroupCursor::incomingCount;
            break;
        case BOTH:
            counter = RelationshipGroupCursor::totalCount;
            break;
        default:
            throw new IllegalArgumentException( "Unrecognized direction " + direction );
        }
        return countDense( nodeCursor, group, selection, counter );
    }

    public static int countDense( NodeCursor nodeCursor, RelationshipGroupCursor group, RelationshipSelection selection,
            ToLongFunction<RelationshipGroupCursor> counter )
    {
        assert nodeCursor.isDense();
        nodeCursor.relationshipGroups( group );
        int count = 0;
        while ( group.next() )
        {
            if ( selection.test( group.type() ) )
            {
                count += counter.applyAsLong( group );
            }
        }
        return count;
    }

    public static int countSparse( NodeCursor nodeCursor, RelationshipTraversalCursor traversal, RelationshipSelection selection )
    {
        assert !nodeCursor.isDense();
        int count = 0;
        nodeCursor.relationships( traversal, selection );
        while ( traversal.next() )
        {
            count++;
        }
        return count;
    }
}
