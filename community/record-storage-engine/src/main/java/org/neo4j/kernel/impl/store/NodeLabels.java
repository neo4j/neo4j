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
package org.neo4j.kernel.impl.store;

import java.util.Collection;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public interface NodeLabels {
    int[] get(NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker);

    Collection<DynamicRecord> put(
            int[] labelIds,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker);

    Collection<DynamicRecord> add(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker);

    Collection<DynamicRecord> remove(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker);

    boolean isInlined();
}
