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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.fail;

public class ManagedTestCursors implements CursorFactory
{
    private final List<Cursor> allCursors = new ArrayList<>();

    private final CursorFactory cursors;

    ManagedTestCursors( CursorFactory c )
    {
        this.cursors = c;
    }

    void assertAllClosedAndReset()
    {
        for ( Cursor n : allCursors )
        {
            if ( !n.isClosed() )
            {
                fail( "The Cursor " + n + " was not closed properly." );
            }
        }

        allCursors.clear();
    }

    @Override
    public NodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        NodeCursor n = cursors.allocateNodeCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor( PageCursorTracer cursorTracer )
    {
        NodeCursor n = cursors.allocateFullAccessNodeCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        RelationshipScanCursor n = cursors.allocateRelationshipScanCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        RelationshipScanCursor n = cursors.allocateFullAccessRelationshipScanCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        RelationshipTraversalCursor n = cursors.allocateRelationshipTraversalCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public PropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        PropertyCursor n = cursors.allocatePropertyCursor( cursorTracer, memoryTracker );
        allCursors.add( n );
        return n;
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        PropertyCursor n = cursors.allocateFullAccessPropertyCursor( cursorTracer, memoryTracker );
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor( PageCursorTracer cursorTracer )
    {
        NodeValueIndexCursor n = cursors.allocateNodeValueIndexCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor( PageCursorTracer cursorTracer )
    {
        NodeLabelIndexCursor n = cursors.allocateNodeLabelIndexCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipIndexCursor allocateRelationshipIndexCursor( PageCursorTracer cursorTracer )
    {
        RelationshipIndexCursor n = cursors.allocateRelationshipIndexCursor( cursorTracer );
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor()
    {
        RelationshipTypeIndexCursor n = cursors.allocateRelationshipTypeIndexCursor();
        allCursors.add( n );
        return n;
    }
}
