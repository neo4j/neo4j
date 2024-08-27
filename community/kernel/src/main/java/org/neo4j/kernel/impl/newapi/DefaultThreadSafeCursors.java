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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Cursor factory which simply creates new instances on allocation. As thread-safe as the underlying {@link StorageReader}.
 */
public class DefaultThreadSafeCursors extends DefaultCursors implements CursorFactory {
    private final StorageReader storageReader;
    private final Function<CursorContext, StoreCursors> storeCursorsFactory;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private boolean applyAccessModeToTxState;

    public DefaultThreadSafeCursors(
            StorageReader storageReader,
            Config config,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState) {
        super(new ConcurrentLinkedQueue<>(), config);
        this.storageReader = storageReader;
        this.storeCursorsFactory = storeCursorsFactory;
        this.indexingBehaviour = indexingBehaviour;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultNodeCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessNodeCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker)));
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultRelationshipScanCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessRelationshipScanCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker)));
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultRelationshipTraversalCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessRelationshipTraversalCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker)));
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultPropertyCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessPropertyCursor(
                defaultPropertyCursor -> {
                    defaultPropertyCursor.release();
                    storeCursors.close();
                },
                storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)));
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultNodeValueIndexCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new FullAccessNodeValueIndexCursor(DefaultNodeValueIndexCursor::release));
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultNodeLabelIndexCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        return trace(new FullAccessNodeLabelIndexCursor(DefaultNodeLabelIndexCursor::release));
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultRelationshipValueIndexCursor(
                cursor -> {
                    cursor.release();
                    storeCursors.close();
                },
                allocateRelationshipScanCursor(cursorContext, memoryTracker),
                newInternalCursors(storeCursors, cursorContext, memoryTracker),
                applyAccessModeToTxState));
    }

    @Override
    public RelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new FullAccessRelationshipValueIndexCursor(DefaultRelationshipValueIndexCursor::release));
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
            return trace(new DefaultNodeBasedRelationshipTypeIndexCursor(
                    DefaultNodeBasedRelationshipTypeIndexCursor::release,
                    allocateNodeCursor(cursorContext, memoryTracker),
                    allocateRelationshipTraversalCursor(cursorContext, memoryTracker)));
        } else {
            return trace(new DefaultRelationshipBasedRelationshipTypeIndexCursor(
                    DefaultRelationshipBasedRelationshipTypeIndexCursor::release,
                    allocateRelationshipScanCursor(cursorContext, memoryTracker),
                    applyAccessModeToTxState));
        }
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
            return trace(new DefaultNodeBasedRelationshipTypeIndexCursor(
                    DefaultNodeBasedRelationshipTypeIndexCursor::release,
                    allocateFullAccessNodeCursor(cursorContext, memoryTracker),
                    allocateFullAccessRelationshipTraversalCursor(cursorContext, memoryTracker)));
        } else {
            return trace(new FullAccessRelationshipBasedRelationshipTypeIndexCursor(
                    DefaultRelationshipBasedRelationshipTypeIndexCursor::release,
                    allocateFullAccessRelationshipScanCursor(cursorContext, memoryTracker)));
        }
    }

    public void close() {
        assertClosed();
        storageReader.close();
    }

    private InternalCursorFactory newInternalCursors(
            StoreCursors storeCursors, CursorContext cursorContext, MemoryTracker memoryTracker) {
        return new InternalCursorFactory(
                storageReader, storeCursors, cursorContext, memoryTracker, applyAccessModeToTxState);
    }
}
