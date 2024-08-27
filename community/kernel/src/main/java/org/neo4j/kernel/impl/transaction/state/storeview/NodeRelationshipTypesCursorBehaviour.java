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

import org.eclipse.collections.api.factory.primitive.IntLists;
import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.EagerDegrees;

/**
 * Used by {@link NodeRelationshipTypesStoreScan} to yield each node's relationship types.
 */
public class NodeRelationshipTypesCursorBehaviour implements EntityScanCursorBehaviour<StorageNodeCursor> {
    private final StorageReader storageReader;

    NodeRelationshipTypesCursorBehaviour(StorageReader storageReader) {
        this.storageReader = storageReader;
    }

    @Override
    public StorageNodeCursor allocateEntityScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
    }

    @Override
    public int[] readTokens(StorageNodeCursor cursor) {
        var degrees = new EagerDegrees();
        cursor.degrees(RelationshipSelection.selection(Direction.OUTGOING), degrees);
        var types = IntLists.mutable.empty();
        for (int type : degrees.types()) {
            if (degrees.outgoingDegree(type) > 0) {
                types.add(type);
            }
        }
        return types.toSortedArray();
    }
}
