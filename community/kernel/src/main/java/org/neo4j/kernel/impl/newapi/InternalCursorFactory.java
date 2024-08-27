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

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Allocator of cursor instances that are purely internal to another cursor, e.g. for security checks etc.
 * This class exists so that internal cursor allocation can easily be made lazy w/o cumbersome construction
 * and passing of suppliers to the "external" cursors.
 * A clear differentiator between internal and external cursor is that external cursors are up for pooling
 * (in some scenarios), but internal cursors are owned by its external cursor only.
 */
class InternalCursorFactory {
    private final StorageReader storageReader;
    private final StoreCursors storeCursors;
    private final CursorContext cursorContext;
    private final MemoryTracker memoryTracker;
    private boolean applyAccessModeToTxState;

    InternalCursorFactory(
            StorageReader storageReader,
            StoreCursors storeCursors,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            boolean applyAccessModeToTxState) {
        this.storageReader = storageReader;
        this.storeCursors = storeCursors;
        this.cursorContext = cursorContext;
        this.memoryTracker = memoryTracker;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    StorageNodeCursor allocateStorageNodeCursor() {
        return storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
    }

    StorageRelationshipTraversalCursor allocateStorageRelationshipTraversalCursor() {
        return storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker);
    }

    StoragePropertyCursor allocateStoragePropertyCursor() {
        return storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker);
    }

    DefaultNodeCursor allocateNodeCursor() {
        return new DefaultNodeCursor(c -> {}, allocateStorageNodeCursor(), this, applyAccessModeToTxState);
    }

    FullAccessNodeCursor allocateFullAccessNodeCursor() {
        return new FullAccessNodeCursor(
                c -> {}, storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker));
    }

    FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor() {
        return new FullAccessRelationshipScanCursor(
                c -> {}, storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker));
    }

    DefaultPropertyCursor allocatePropertyCursor() {
        return new DefaultPropertyCursor(
                c -> {},
                storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                this,
                applyAccessModeToTxState);
    }
}
