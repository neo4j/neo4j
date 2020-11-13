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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

public class StubCursorFactory implements CursorFactory
{
    private final boolean continueWithLastItem;
    private final Queue<NodeCursor> nodeCursors = new ArrayDeque<>();
    private final Queue<NodeCursor> fullNodeCursors = new ArrayDeque<>();
    private final Queue<RelationshipScanCursor> relationshipScanCursors = new ArrayDeque<>();
    private final Queue<RelationshipScanCursor> fullRelationshipScanCursors = new ArrayDeque<>();
    private final Queue<RelationshipTraversalCursor> relationshipTraversalCursors = new LinkedList<>();
    private final Queue<RelationshipTraversalCursor> fullRelationshipTraversalCursors = new LinkedList<>();
    private final Queue<PropertyCursor> propertyCursors = new ArrayDeque<>();
    private final Queue<PropertyCursor> fullPropertyCursors = new ArrayDeque<>();
    private final Queue<NodeValueIndexCursor> nodeValueIndexCursors = new ArrayDeque<>();
    private final Queue<NodeValueIndexCursor> fullNodeValueIndexCursors = new ArrayDeque<>();
    private final Queue<NodeLabelIndexCursor> nodeLabelIndexCursors = new ArrayDeque<>();
    private final Queue<NodeLabelIndexCursor> fullNodeLabelIndexCursors = new ArrayDeque<>();
    private final Queue<RelationshipValueIndexCursor> relationshipValueIndexCursors = new LinkedList<>();
    private final Queue<RelationshipTypeIndexCursor> relationshipTypeIndexCursors = new ArrayDeque<>();

    public StubCursorFactory()
    {
        this( false );
    }

    public StubCursorFactory( boolean continueWithLastItem )
    {
        this.continueWithLastItem = continueWithLastItem;
    }

    @Override
    public NodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        return poll( nodeCursors );
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor( PageCursorTracer cursorTracer )
    {
        return poll( fullNodeCursors );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return poll( relationshipScanCursors );
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return poll( fullRelationshipScanCursors );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        return poll( relationshipTraversalCursors );
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        return poll( fullRelationshipTraversalCursors );
    }

    @Override
    public PropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return poll( propertyCursors );
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return poll( fullPropertyCursors );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return poll( nodeValueIndexCursors );
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return poll( fullNodeValueIndexCursors );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor( PageCursorTracer cursorTracer )
    {
        return poll( nodeLabelIndexCursors );
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor( PageCursorTracer cursorTracer )
    {
        return poll( fullNodeLabelIndexCursors );
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return poll( relationshipValueIndexCursors );
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor()
    {
        return poll( relationshipTypeIndexCursors );
    }

    public StubCursorFactory withRelationshipTraversalCursors( RelationshipTraversalCursor... cursors )
    {
        relationshipTraversalCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }

    private <T> T poll( Queue<T> queue )
    {
        T poll = queue.poll();
        if ( continueWithLastItem && queue.isEmpty() )
        {
            queue.offer( poll );
        }
        return poll;
    }
}
