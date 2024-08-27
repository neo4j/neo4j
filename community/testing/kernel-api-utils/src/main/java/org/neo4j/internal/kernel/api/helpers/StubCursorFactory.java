/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers;

import java.util.ArrayDeque;
import java.util.Arrays;
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
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

public class StubCursorFactory implements CursorFactory {
    private final boolean continueWithLastItem;
    private final Queue<NodeCursor> nodeCursors = new ArrayDeque<>();
    private final Queue<NodeCursor> fullNodeCursors = new ArrayDeque<>();
    private final Queue<RelationshipScanCursor> relationshipScanCursors = new ArrayDeque<>();
    private final Queue<RelationshipScanCursor> fullRelationshipScanCursors = new ArrayDeque<>();
    private final Queue<RelationshipTraversalCursor> relationshipTraversalCursors = new ArrayDeque<>();
    private final Queue<RelationshipTraversalCursor> fullRelationshipTraversalCursors = new ArrayDeque<>();
    private final Queue<PropertyCursor> propertyCursors = new ArrayDeque<>();
    private final Queue<PropertyCursor> fullPropertyCursors = new ArrayDeque<>();
    private final Queue<NodeValueIndexCursor> nodeValueIndexCursors = new ArrayDeque<>();
    private final Queue<NodeValueIndexCursor> fullNodeValueIndexCursors = new ArrayDeque<>();
    private final Queue<NodeLabelIndexCursor> nodeLabelIndexCursors = new ArrayDeque<>();
    private final Queue<NodeLabelIndexCursor> fullNodeLabelIndexCursors = new ArrayDeque<>();
    private final Queue<RelationshipValueIndexCursor> relationshipValueIndexCursors = new ArrayDeque<>();
    private final Queue<RelationshipValueIndexCursor> fullRelationshipValueIndexCursors = new ArrayDeque<>();
    private final Queue<RelationshipTypeIndexCursor> relationshipTypeIndexCursors = new ArrayDeque<>();
    private final Queue<RelationshipTypeIndexCursor> fullRelationshipTypeIndexCursors = new ArrayDeque<>();

    public StubCursorFactory() {
        this(false);
    }

    public StubCursorFactory(boolean continueWithLastItem) {
        this.continueWithLastItem = continueWithLastItem;
    }

    @Override
    public NodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(nodeCursors);
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullNodeCursors);
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(relationshipScanCursors);
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullRelationshipScanCursors);
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(relationshipTraversalCursors);
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullRelationshipTraversalCursors);
    }

    @Override
    public PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(propertyCursors);
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullPropertyCursors);
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(nodeValueIndexCursors);
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullNodeValueIndexCursors);
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(nodeLabelIndexCursors);
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        return poll(fullNodeLabelIndexCursors);
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(relationshipValueIndexCursors);
    }

    @Override
    public RelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullRelationshipValueIndexCursors);
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(relationshipTypeIndexCursors);
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return poll(fullRelationshipTypeIndexCursors);
    }

    public StubCursorFactory withRelationshipTraversalCursors(RelationshipTraversalCursor... cursors) {
        relationshipTraversalCursors.addAll(Arrays.asList(cursors));
        return this;
    }

    private <T> T poll(Queue<T> queue) {
        T poll = queue.poll();
        if (continueWithLastItem && queue.isEmpty()) {
            queue.offer(poll);
        }
        return poll;
    }
}
