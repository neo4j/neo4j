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
package org.neo4j.kernel.impl.newapi;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.kernel.api.Cursor;
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

public class ManagedTestCursors implements CursorFactory {
    private final List<Cursor> allCursors = new ArrayList<>();

    private final CursorFactory cursors;

    ManagedTestCursors(CursorFactory c) {
        this.cursors = c;
    }

    void assertAllClosedAndReset() {
        for (Cursor n : allCursors) {
            if (!n.isClosed()) {
                fail("The Cursor " + n + " was not closed properly.");
            }
        }

        allCursors.clear();
    }

    @Override
    public NodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        NodeCursor n = cursors.allocateNodeCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        NodeCursor n = cursors.allocateFullAccessNodeCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipScanCursor n = cursors.allocateRelationshipScanCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipScanCursor n = cursors.allocateFullAccessRelationshipScanCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipTraversalCursor n = cursors.allocateRelationshipTraversalCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipTraversalCursor n =
                cursors.allocateFullAccessRelationshipTraversalCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        PropertyCursor n = cursors.allocatePropertyCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        PropertyCursor n = cursors.allocateFullAccessPropertyCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        NodeValueIndexCursor n = cursors.allocateNodeValueIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        NodeValueIndexCursor n = cursors.allocateFullAccessNodeValueIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        NodeLabelIndexCursor n = cursors.allocateNodeLabelIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        NodeLabelIndexCursor n = cursors.allocateFullAccessNodeLabelIndexCursor(cursorContext);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipValueIndexCursor n = cursors.allocateRelationshipValueIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipValueIndexCursor n =
                cursors.allocateFullAccessRelationshipValueIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipTypeIndexCursor n = cursors.allocateRelationshipTypeIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        RelationshipTypeIndexCursor n =
                cursors.allocateFullAccessRelationshipTypeIndexCursor(cursorContext, memoryTracker);
        allCursors.add(n);
        return n;
    }
}
