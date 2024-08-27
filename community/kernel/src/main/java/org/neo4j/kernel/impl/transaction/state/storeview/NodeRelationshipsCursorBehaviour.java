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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.StorageRelationshipByNodeScanCursor;

/**
 * Used by {@link NodeRelationshipsIndexedStoreScan} to access, for a given node, its matching relationships
 */
public class NodeRelationshipsCursorBehaviour
        implements EntityScanCursorBehaviour<StorageRelationshipByNodeScanCursor> {
    private final StorageReader storageReader;
    private final RelationshipSelection relationshipSelection;

    NodeRelationshipsCursorBehaviour(StorageReader storageReader, RelationshipSelection relationshipSelection) {
        this.storageReader = storageReader;
        this.relationshipSelection = relationshipSelection;
    }

    @Override
    public StorageRelationshipByNodeScanCursor allocateEntityScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new StorageRelationshipByNodeScanCursor(
                storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker),
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                relationshipSelection);
    }

    @Override
    public int[] readTokens(StorageRelationshipByNodeScanCursor cursor) {
        return new int[] {cursor.type()};
    }
}
