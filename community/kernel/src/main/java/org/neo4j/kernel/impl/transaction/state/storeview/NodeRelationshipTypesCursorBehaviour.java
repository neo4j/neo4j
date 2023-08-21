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

import static org.neo4j.collection.PrimitiveArrays.intsToLongs;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Used by {@link NodeRelationshipTypesStoreScan} to yield each node's relationship types.
 */
public class NodeRelationshipTypesCursorBehaviour implements EntityScanCursorBehaviour<StorageNodeCursor> {
    private final StorageReader storageReader;

    NodeRelationshipTypesCursorBehaviour(StorageReader storageReader) {
        this.storageReader = storageReader;
    }

    @Override
    public StorageNodeCursor allocateEntityScanCursor(CursorContext cursorContext, StoreCursors storeCursors) {
        return storageReader.allocateNodeCursor(cursorContext, storeCursors);
    }

    @Override
    public long[] readTokens(StorageNodeCursor cursor) {
        return intsToLongs(cursor.relationshipTypes());
    }
}
