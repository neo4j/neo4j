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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import org.neo4j.index.internal.gbptree.StructurePropagation.StructureUpdate;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Provides access to internal tree logic for custom tree write operations
 */
public interface InternalAccess<KEY, VALUE> {
    void tryShrinkTree(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            long stableGeneration,
            long unstableGeneration)
            throws IOException;

    void underflowInLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    void createSuccessorIfNeeded(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            StructureUpdate updateMidChild,
            long stableGeneration,
            long unstableGeneration)
            throws IOException;

    void handleStructureChanges(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    boolean moveToCorrectLeaf(
            PageCursor cursor, KEY key, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException;

    boolean cursorIsAtExpectedLocation(PageCursor cursor);

    TreeWriterCoordination coordination();

    LeafNodeBehaviour<KEY, VALUE> leafNode();

    InternalNodeBehaviour<KEY> internalNode();
}
