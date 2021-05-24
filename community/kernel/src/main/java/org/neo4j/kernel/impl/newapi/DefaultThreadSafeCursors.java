/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Cursor factory which simply creates new instances on allocation. As thread-safe as the underlying {@link StorageReader}.
 */
public class DefaultThreadSafeCursors extends DefaultCursors implements CursorFactory
{
    private final StorageReader storageReader;
    private final Function<CursorContext, StoreCursors> storeCursorsFactory;

    public DefaultThreadSafeCursors( StorageReader storageReader, Config config, Function<CursorContext, StoreCursors> storeCursorsFactory )
    {
        super( new ConcurrentLinkedQueue<>(), config );
        this.storageReader = storageReader;
        this.storeCursorsFactory = storeCursorsFactory;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new DefaultNodeCursor( defaultNodeCursor ->
        {
            defaultNodeCursor.release();
            storeCursors.close();
        }, storageReader.allocateNodeCursor( cursorContext, storeCursors ),
                storageReader.allocateNodeCursor( cursorContext, storeCursors ) ) );
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new FullAccessNodeCursor( defaultNodeCursor ->
        {
            defaultNodeCursor.release();
            storeCursors.close();
        }, storageReader.allocateNodeCursor( cursorContext, storeCursors ) ) );
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new DefaultRelationshipScanCursor( defaultRelationshipScanCursor -> {
            defaultRelationshipScanCursor.release();
            storeCursors.close();
        },
        storageReader.allocateRelationshipScanCursor( cursorContext, storeCursors ), allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new FullAccessRelationshipScanCursor( defaultRelationshipScanCursor -> {
            defaultRelationshipScanCursor.release();
            storeCursors.close();
        },
        storageReader.allocateRelationshipScanCursor( cursorContext, storeCursors ) ) );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new DefaultRelationshipTraversalCursor( defaultRelationshipTraversalCursor -> {
            defaultRelationshipTraversalCursor.release();
            storeCursors.close();
        },
        storageReader.allocateRelationshipTraversalCursor( cursorContext, storeCursors ), allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor( CursorContext cursorContext )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new FullAccessRelationshipTraversalCursor( defaultRelationshipTraversalCursor -> {
            defaultRelationshipTraversalCursor.release();
            storeCursors.close();
        },
        storageReader.allocateRelationshipTraversalCursor( cursorContext, storeCursors ) ) );
    }

    @Override
    public PropertyCursor allocatePropertyCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new DefaultPropertyCursor( defaultPropertyCursor -> {
            defaultPropertyCursor.release();
            storeCursors.close();
        },
        storageReader.allocatePropertyCursor( cursorContext, storeCursors, memoryTracker ), allocateFullAccessNodeCursor( cursorContext ),
        allocateFullAccessRelationshipScanCursor( cursorContext ) ) );
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        var storeCursors = storeCursorsFactory.apply( cursorContext );
        return trace( new FullAccessPropertyCursor( defaultPropertyCursor -> {
            defaultPropertyCursor.release();
            storeCursors.close();
        },
        storageReader.allocatePropertyCursor( cursorContext, storeCursors, memoryTracker ) ) );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return trace( new DefaultNodeValueIndexCursor(
                DefaultNodeValueIndexCursor::release, allocateNodeCursor( cursorContext ), memoryTracker ) );
    }

    @Override
    public NodeValueIndexCursor allocateFullAccessNodeValueIndexCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return trace( new FullAccessNodeValueIndexCursor( DefaultNodeValueIndexCursor::release, memoryTracker ) );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor( CursorContext cursorContext )
    {
        return trace( new DefaultNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release, allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor( CursorContext cursorContext )
    {
        return trace( new FullAccessNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release ) );
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return trace( new DefaultRelationshipValueIndexCursor(
                DefaultRelationshipValueIndexCursor::release, allocateRelationshipScanCursor( cursorContext ), memoryTracker ) );
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor( CursorContext cursorContext )
    {
        return trace( new DefaultRelationshipTypeIndexCursor( DefaultRelationshipTypeIndexCursor::release, allocateRelationshipScanCursor( cursorContext ) ) );
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor()
    {
        return trace( new FullAccessRelationshipTypeIndexCursor( DefaultRelationshipTypeIndexCursor::release ) );
    }

    public void close()
    {
        assertClosed();
        storageReader.close();
    }
}
