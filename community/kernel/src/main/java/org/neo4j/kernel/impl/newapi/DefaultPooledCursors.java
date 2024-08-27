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

import java.util.ArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Cursor factory which pools 1 cursor of each kind. Not thread-safe at all.
 */
public class DefaultPooledCursors extends DefaultCursors implements CursorFactory {
    private final StorageReader storageReader;
    private final StoreCursors storeCursors;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private final boolean applyAccessModeToTxState;
    private DefaultNodeCursor nodeCursor;
    private FullAccessNodeCursor fullAccessNodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private FullAccessRelationshipScanCursor fullAccessRelationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private FullAccessRelationshipTraversalCursor fullAccessRelationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private FullAccessPropertyCursor fullAccessPropertyCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private FullAccessNodeValueIndexCursor fullAccessNodeValueIndexCursor;
    private FullAccessRelationshipValueIndexCursor fullAccessRelationshipValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultNodeLabelIndexCursor fullAccessNodeLabelIndexCursor;
    private DefaultRelationshipValueIndexCursor relationshipValueIndexCursor;
    private InternalRelationshipTypeIndexCursor relationshipTypeIndexCursor;
    private InternalRelationshipTypeIndexCursor fullAccessRelationshipTypeIndexCursor;

    public DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState) {
        super(new ArrayList<>(), config);
        this.storageReader = storageReader;
        this.storeCursors = storeCursors;
        this.indexingBehaviour = indexingBehaviour;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeCursor == null) {
            return trace(new DefaultNodeCursor(
                    this::accept,
                    storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker),
                    newInternalCursors(cursorContext, memoryTracker),
                    applyAccessModeToTxState));
        }

        try {
            return acquire(nodeCursor);
        } finally {
            nodeCursor = null;
        }
    }

    private void accept(DefaultNodeCursor cursor) {
        if (nodeCursor != null) {
            nodeCursor.release();
        }
        cursor.removeTracer();
        nodeCursor = cursor;
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessNodeCursor == null) {
            return trace(new FullAccessNodeCursor(
                    this::acceptFullAccess,
                    storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker)));
        }

        try {
            return acquire(fullAccessNodeCursor);
        } finally {
            fullAccessNodeCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeCursor cursor) {
        if (fullAccessNodeCursor != null) {
            fullAccessNodeCursor.release();
        }
        cursor.removeTracer();
        fullAccessNodeCursor = (FullAccessNodeCursor) cursor;
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipScanCursor == null) {
            return trace(new DefaultRelationshipScanCursor(
                    this::accept,
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                    newInternalCursors(cursorContext, memoryTracker),
                    applyAccessModeToTxState));
        }

        try {
            return acquire(relationshipScanCursor);
        } finally {
            relationshipScanCursor = null;
        }
    }

    private void accept(DefaultRelationshipScanCursor cursor) {
        if (relationshipScanCursor != null) {
            relationshipScanCursor.release();
        }
        cursor.removeTracer();
        relationshipScanCursor = cursor;
    }

    @Override
    public DefaultRelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipScanCursor == null) {
            return trace(new FullAccessRelationshipScanCursor(
                    this::acceptFullAccess,
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker)));
        }

        try {
            return acquire(fullAccessRelationshipScanCursor);
        } finally {
            fullAccessRelationshipScanCursor = null;
        }
    }

    private static <C extends TraceableCursor> C acquire(C cursor) {
        cursor.acquire();
        return cursor;
    }

    private void acceptFullAccess(DefaultRelationshipScanCursor cursor) {
        if (fullAccessRelationshipScanCursor != null) {
            fullAccessRelationshipScanCursor.release();
        }
        cursor.removeTracer();
        fullAccessRelationshipScanCursor = (FullAccessRelationshipScanCursor) cursor;
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTraversalCursor == null) {
            return trace(new DefaultRelationshipTraversalCursor(
                    this::accept,
                    storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                    newInternalCursors(cursorContext, memoryTracker),
                    applyAccessModeToTxState));
        }

        try {
            return acquire(relationshipTraversalCursor);
        } finally {
            relationshipTraversalCursor = null;
        }
    }

    void accept(DefaultRelationshipTraversalCursor cursor) {
        if (relationshipTraversalCursor != null) {
            relationshipTraversalCursor.release();
        }
        cursor.removeTracer();
        relationshipTraversalCursor = cursor;
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTraversalCursor == null) {
            return trace(new FullAccessRelationshipTraversalCursor(
                    this::acceptFullAccess,
                    storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker)));
        }

        try {
            return acquire(fullAccessRelationshipTraversalCursor);
        } finally {
            fullAccessRelationshipTraversalCursor = null;
        }
    }

    private void acceptFullAccess(DefaultRelationshipTraversalCursor cursor) {
        if (fullAccessRelationshipTraversalCursor != null) {
            fullAccessRelationshipTraversalCursor.release();
        }
        cursor.removeTracer();
        fullAccessRelationshipTraversalCursor = (FullAccessRelationshipTraversalCursor) cursor;
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (propertyCursor == null) {
            return trace(new DefaultPropertyCursor(
                    this::accept,
                    storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                    newInternalCursors(cursorContext, memoryTracker),
                    applyAccessModeToTxState));
        }

        try {
            return acquire(propertyCursor);
        } finally {
            propertyCursor = null;
        }
    }

    private void accept(DefaultPropertyCursor cursor) {
        if (propertyCursor != null) {
            propertyCursor.release();
        }
        cursor.removeTracer();
        propertyCursor = cursor;
    }

    @Override
    public FullAccessPropertyCursor allocateFullAccessPropertyCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessPropertyCursor == null) {
            return trace(new FullAccessPropertyCursor(
                    this::acceptFullAccess,
                    storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)));
        }

        try {
            return acquire(fullAccessPropertyCursor);
        } finally {
            fullAccessPropertyCursor = null;
        }
    }

    private void acceptFullAccess(DefaultPropertyCursor cursor) {
        if (fullAccessPropertyCursor != null) {
            fullAccessPropertyCursor.release();
        }
        cursor.removeTracer();
        fullAccessPropertyCursor = (FullAccessPropertyCursor) cursor;
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeValueIndexCursor == null) {
            return trace(new DefaultNodeValueIndexCursor(
                    this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState));
        }

        try {
            return acquire(nodeValueIndexCursor);
        } finally {
            nodeValueIndexCursor = null;
        }
    }

    private void accept(DefaultNodeValueIndexCursor cursor) {
        if (nodeValueIndexCursor != null) {
            nodeValueIndexCursor.release();
        }
        cursor.removeTracer();
        nodeValueIndexCursor = cursor;
    }

    @Override
    public FullAccessNodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessNodeValueIndexCursor == null) {
            return trace(new FullAccessNodeValueIndexCursor(this::acceptFullAccess));
        }

        try {
            return acquire(fullAccessNodeValueIndexCursor);
        } finally {
            fullAccessNodeValueIndexCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeValueIndexCursor cursor) {
        if (fullAccessNodeValueIndexCursor != null) {
            fullAccessNodeValueIndexCursor.release();
        }
        cursor.removeTracer();
        fullAccessNodeValueIndexCursor = (FullAccessNodeValueIndexCursor) cursor;
    }

    @Override
    public FullAccessRelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipValueIndexCursor == null) {
            return trace(new FullAccessRelationshipValueIndexCursor(this::acceptFullAccess));
        }

        try {
            return acquire(fullAccessRelationshipValueIndexCursor);
        } finally {
            fullAccessRelationshipValueIndexCursor = null;
        }
    }

    private void acceptFullAccess(DefaultRelationshipValueIndexCursor cursor) {
        if (fullAccessRelationshipValueIndexCursor != null) {
            fullAccessRelationshipValueIndexCursor.release();
        }
        cursor.removeTracer();
        fullAccessRelationshipValueIndexCursor = (FullAccessRelationshipValueIndexCursor) cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeLabelIndexCursor == null) {
            return trace(new DefaultNodeLabelIndexCursor(
                    this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState));
        }

        try {
            return acquire(nodeLabelIndexCursor);
        } finally {
            nodeLabelIndexCursor = null;
        }
    }

    private void accept(DefaultNodeLabelIndexCursor cursor) {
        if (nodeLabelIndexCursor != null) {
            nodeLabelIndexCursor.release();
        }
        cursor.removeTracer();
        nodeLabelIndexCursor = cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        if (fullAccessNodeLabelIndexCursor == null) {
            return trace(new FullAccessNodeLabelIndexCursor(this::acceptFullAccess));
        }

        try {
            return acquire(fullAccessNodeLabelIndexCursor);
        } finally {
            fullAccessNodeLabelIndexCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeLabelIndexCursor cursor) {
        if (fullAccessNodeLabelIndexCursor != null) {
            fullAccessNodeLabelIndexCursor.release();
        }
        fullAccessNodeLabelIndexCursor = cursor;
    }

    @Override
    public DefaultRelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipValueIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            DefaultRelationshipScanCursor relationshipScanCursor = new DefaultRelationshipScanCursor(
                    c -> {},
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                    internalCursors,
                    applyAccessModeToTxState);
            return trace(new DefaultRelationshipValueIndexCursor(
                    this::accept, relationshipScanCursor, internalCursors, applyAccessModeToTxState));
        }

        try {
            return acquire(relationshipValueIndexCursor);
        } finally {
            relationshipValueIndexCursor = null;
        }
    }

    public void accept(DefaultRelationshipValueIndexCursor cursor) {
        if (relationshipValueIndexCursor != null) {
            relationshipValueIndexCursor.release();
        }
        cursor.removeTracer();
        relationshipValueIndexCursor = cursor;
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTypeIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var relationshipTraversalCursor = new DefaultRelationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);
                return trace(new DefaultNodeBasedRelationshipTypeIndexCursor(
                        this::accept, internalCursors.allocateNodeCursor(), relationshipTraversalCursor));
            } else {
                var relationshipScanCursor = new DefaultRelationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);
                return trace(new DefaultRelationshipBasedRelationshipTypeIndexCursor(
                        this::accept, relationshipScanCursor, applyAccessModeToTxState));
            }
        }

        try {
            return acquire(relationshipTypeIndexCursor);
        } finally {
            relationshipTypeIndexCursor = null;
        }
    }

    private void accept(InternalRelationshipTypeIndexCursor cursor) {
        if (relationshipTypeIndexCursor != null) {
            relationshipTypeIndexCursor.release();
        }
        cursor.removeTracer();
        relationshipTypeIndexCursor = cursor;
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTypeIndexCursor == null) {
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var nodeCursor = new FullAccessNodeCursor(
                        c -> {}, storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker));
                var relationshipTraversalCursor = new FullAccessRelationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker));
                return trace(new DefaultNodeBasedRelationshipTypeIndexCursor(
                        this::acceptFullAccess, nodeCursor, relationshipTraversalCursor));
            } else {
                var relationshipScanCursor = new FullAccessRelationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker));
                return trace(new FullAccessRelationshipBasedRelationshipTypeIndexCursor(
                        this::acceptFullAccess, relationshipScanCursor));
            }
        }

        try {
            return acquire(fullAccessRelationshipTypeIndexCursor);
        } finally {
            fullAccessRelationshipTypeIndexCursor = null;
        }
    }

    private void acceptFullAccess(InternalRelationshipTypeIndexCursor cursor) {
        if (fullAccessRelationshipTypeIndexCursor != null) {
            fullAccessRelationshipTypeIndexCursor.release();
        }
        fullAccessRelationshipTypeIndexCursor = cursor;
    }

    public void release() {
        if (nodeCursor != null) {
            nodeCursor.release();
        }
        if (fullAccessNodeCursor != null) {
            fullAccessNodeCursor.release();
        }
        if (relationshipScanCursor != null) {
            relationshipScanCursor.release();
        }
        if (fullAccessRelationshipScanCursor != null) {
            fullAccessRelationshipScanCursor.release();
        }
        if (relationshipTraversalCursor != null) {
            relationshipTraversalCursor.release();
        }
        if (fullAccessRelationshipTraversalCursor != null) {
            fullAccessRelationshipTraversalCursor.release();
        }
        if (propertyCursor != null) {
            propertyCursor.release();
        }
        if (fullAccessPropertyCursor != null) {
            fullAccessPropertyCursor.release();
        }
        if (nodeValueIndexCursor != null) {
            nodeValueIndexCursor.release();
        }
        if (fullAccessNodeValueIndexCursor != null) {
            fullAccessNodeValueIndexCursor.release();
        }
        if (fullAccessRelationshipValueIndexCursor != null) {
            fullAccessRelationshipValueIndexCursor.release();
        }
        if (nodeLabelIndexCursor != null) {
            nodeLabelIndexCursor.release();
        }
        if (fullAccessNodeLabelIndexCursor != null) {
            fullAccessNodeLabelIndexCursor.release();
        }
        if (relationshipValueIndexCursor != null) {
            relationshipValueIndexCursor.release();
        }
        if (relationshipTypeIndexCursor != null) {
            relationshipTypeIndexCursor.release();
        }
        if (fullAccessRelationshipTypeIndexCursor != null) {
            fullAccessRelationshipTypeIndexCursor.release();
        }
        nodeCursor = null;
        fullAccessNodeCursor = null;
        relationshipScanCursor = null;
        fullAccessRelationshipScanCursor = null;
        relationshipTraversalCursor = null;
        fullAccessRelationshipTraversalCursor = null;
        propertyCursor = null;
        fullAccessPropertyCursor = null;
        nodeValueIndexCursor = null;
        fullAccessNodeValueIndexCursor = null;
        nodeLabelIndexCursor = null;
        fullAccessNodeLabelIndexCursor = null;
        relationshipValueIndexCursor = null;
        relationshipTypeIndexCursor = null;
        fullAccessRelationshipTypeIndexCursor = null;
        fullAccessRelationshipValueIndexCursor = null;
    }

    private InternalCursorFactory newInternalCursors(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return new InternalCursorFactory(
                storageReader, storeCursors, cursorContext, memoryTracker, applyAccessModeToTxState);
    }
}
