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
package org.neo4j.internal.kernel.api;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

/**
 * Allocates Cursors. To read data from the Kernel, Cursors are needed. A Cursor factory let's the Kernel consumer
 * allocate all types of cursors, which can then be reused for multiple read operations.
 */
public interface CursorFactory {
    // entities

    NodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    default NodeCursor allocateNodeCursor(CursorContext cursorContext) {
        return allocateNodeCursor(cursorContext, EmptyMemoryTracker.INSTANCE);
    }

    NodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    RelationshipScanCursor allocateRelationshipScanCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    default RelationshipScanCursor allocateRelationshipScanCursor(CursorContext cursorContext) {
        return allocateRelationshipScanCursor(cursorContext, EmptyMemoryTracker.INSTANCE);
    }

    RelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    // traversal

    RelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    default RelationshipTraversalCursor allocateRelationshipTraversalCursor(CursorContext cursorContext) {
        return allocateRelationshipTraversalCursor(cursorContext, EmptyMemoryTracker.INSTANCE);
    }

    RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    // properties

    PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    // schema indexes

    NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    RelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    default NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext) {
        return allocateNodeLabelIndexCursor(cursorContext, EmptyMemoryTracker.INSTANCE);
    }

    NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext);

    RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    default RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(CursorContext cursorContext) {
        return allocateRelationshipTypeIndexCursor(cursorContext, EmptyMemoryTracker.INSTANCE);
    }

    RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);
}
