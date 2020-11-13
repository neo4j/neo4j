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
package org.neo4j.internal.kernel.api;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

/**
 * Allocates Cursors. To read data from the Kernel, Cursors are needed. A Cursor factory let's the Kernel consumer
 * allocate all types of cursors, which can then be reused for multiple read operations.
 */
public interface CursorFactory
{
    // entities

    NodeCursor allocateNodeCursor( PageCursorTracer cursorTracer );

    NodeCursor allocateFullAccessNodeCursor( PageCursorTracer cursorTracer );

    RelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer );

    RelationshipScanCursor allocateFullAccessRelationshipScanCursor( PageCursorTracer cursorTracer );

    // traversal

    RelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer );

    RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor( PageCursorTracer cursorTracer );

    // properties

    PropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    PropertyCursor allocateFullAccessPropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    // schema indexes

    NodeValueIndexCursor allocateNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    NodeLabelIndexCursor allocateNodeLabelIndexCursor( PageCursorTracer cursorTracer );

    NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor( PageCursorTracer cursorTracer );

    RelationshipValueIndexCursor allocateRelationshipValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor();

}
