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

    public DefaultThreadSafeCursors(
            StorageReader storageReader,
            Config config,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        super(new ConcurrentLinkedQueue<>(), config);
        this.storageReader = storageReader;
        this.storeCursorsFactory = storeCursorsFactory;
        this.indexingBehaviour = indexingBehaviour;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultNodeCursor(
                defaultNodeCursor -> {
                    defaultNodeCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateNodeCursor(cursorContext, storeCursors),
                storageReader.allocateNodeCursor(cursorContext, storeCursors),
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors),
                () -> storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)));
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessNodeCursor(
                defaultNodeCursor -> {
                    defaultNodeCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateNodeCursor(cursorContext, storeCursors)));
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultRelationshipScanCursor(
                defaultRelationshipScanCursor -> {
                    defaultRelationshipScanCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors),
                allocateNodeCursor(cursorContext, memoryTracker)));
    }

    @Override
    public FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor(CursorContext cursorContext) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessRelationshipScanCursor(
                defaultRelationshipScanCursor -> {
                    defaultRelationshipScanCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors)));
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultRelationshipTraversalCursor(
                defaultRelationshipTraversalCursor -> {
                    defaultRelationshipTraversalCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors),
                allocateNodeCursor(cursorContext, memoryTracker)));
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new FullAccessRelationshipTraversalCursor(
                defaultRelationshipTraversalCursor -> {
                    defaultRelationshipTraversalCursor.release();
                    storeCursors.close();
                },
                storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors)));
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        var storeCursors = storeCursorsFactory.apply(cursorContext);
        return trace(new DefaultPropertyCursor(
                defaultPropertyCursor -> {
                    defaultPropertyCursor.release();
                    storeCursors.close();
                },
                storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                () -> storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                allocateFullAccessNodeCursor(cursorContext),
                allocateFullAccessRelationshipScanCursor(cursorContext)));
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
        return trace(new DefaultNodeValueIndexCursor(
                DefaultNodeValueIndexCursor::release,
                allocateNodeCursor(cursorContext, memoryTracker),
                allocatePropertyCursor(cursorContext, memoryTracker),
                memoryTracker));
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new FullAccessNodeValueIndexCursor(DefaultNodeValueIndexCursor::release, memoryTracker));
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new DefaultNodeLabelIndexCursor(
                DefaultNodeLabelIndexCursor::release, allocateNodeCursor(cursorContext, memoryTracker)));
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        return trace(new FullAccessNodeLabelIndexCursor(DefaultNodeLabelIndexCursor::release));
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new DefaultRelationshipValueIndexCursor(
                DefaultRelationshipValueIndexCursor::release,
                allocateRelationshipScanCursor(cursorContext, memoryTracker),
                allocatePropertyCursor(cursorContext, memoryTracker),
                memoryTracker));
    }

    @Override
    public RelationshipValueIndexCursor allocateFullAccessRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        return trace(new FullAccessRelationshipValueIndexCursor(
                DefaultRelationshipValueIndexCursor::release, memoryTracker));
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
                    allocateRelationshipScanCursor(cursorContext, memoryTracker)));
        }
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(CursorContext cursorContext) {
        if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
            return trace(new DefaultNodeBasedRelationshipTypeIndexCursor(
                    DefaultNodeBasedRelationshipTypeIndexCursor::release,
                    allocateFullAccessNodeCursor(cursorContext),
                    allocateFullAccessRelationshipTraversalCursor(cursorContext)));
        } else {
            return trace(new FullAccessRelationshipBasedRelationshipTypeIndexCursor(
                    DefaultRelationshipBasedRelationshipTypeIndexCursor::release,
                    allocateFullAccessRelationshipScanCursor(cursorContext)));
        }
    }

    public void close() {
        assertClosed();
        storageReader.close();
    }
}
