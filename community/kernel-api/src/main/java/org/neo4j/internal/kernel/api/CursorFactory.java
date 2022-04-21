/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.kernel.api;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

/**
 * Allocates Cursors. To read data from the Kernel, Cursors are needed. A Cursor factory let's the Kernel consumer
 * allocate all types of cursors, which can then be reused for multiple read operations.
 */
public interface CursorFactory {
    // entities

    NodeCursor allocateNodeCursor(CursorContext cursorContext);

    NodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext);

    RelationshipScanCursor allocateRelationshipScanCursor(CursorContext cursorContext);

    RelationshipScanCursor allocateFullAccessRelationshipScanCursor(CursorContext cursorContext);

    // traversal

    RelationshipTraversalCursor allocateRelationshipTraversalCursor(CursorContext cursorContext);

    RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(CursorContext cursorContext);

    // properties

    PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    // schema indexes

    NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker);

    NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext);

    NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext);

    RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker);

    RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(CursorContext cursorContext);

    RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor();
}
