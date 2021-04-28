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

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Cursor factory which simply creates new instances on allocation. As thread-safe as the underlying {@link StorageReader}.
 */
public class DefaultThreadSafeCursors extends DefaultCursors implements CursorFactory
{
    private final StorageReader storageReader;

    public DefaultThreadSafeCursors( StorageReader storageReader, Config config )
    {
        super( new ConcurrentLinkedQueue<>(), config );
        this.storageReader = storageReader;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor( CursorContext cursorContext )
    {
        return trace( new DefaultNodeCursor(
                DefaultNodeCursor::release, storageReader.allocateNodeCursor( cursorContext ), storageReader.allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor( CursorContext cursorContext )
    {
        return trace( new FullAccessNodeCursor( DefaultNodeCursor::release, storageReader.allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor( CursorContext cursorContext )
    {
        return trace( new DefaultRelationshipScanCursor( DefaultRelationshipScanCursor::release,
                storageReader.allocateRelationshipScanCursor( cursorContext ), allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor( CursorContext cursorContext )
    {
        return trace( new FullAccessRelationshipScanCursor( DefaultRelationshipScanCursor::release,
                storageReader.allocateRelationshipScanCursor( cursorContext ) ) );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor( CursorContext cursorContext )
    {
        return trace( new DefaultRelationshipTraversalCursor( DefaultRelationshipTraversalCursor::release,
                storageReader.allocateRelationshipTraversalCursor( cursorContext ), allocateNodeCursor( cursorContext ) ) );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor( CursorContext cursorContext )
    {
        return trace( new FullAccessRelationshipTraversalCursor( DefaultRelationshipTraversalCursor::release,
                storageReader.allocateRelationshipTraversalCursor( cursorContext ) ) );
    }

    @Override
    public PropertyCursor allocatePropertyCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return trace( new DefaultPropertyCursor( DefaultPropertyCursor::release,
                storageReader.allocatePropertyCursor( cursorContext, memoryTracker ), allocateFullAccessNodeCursor( cursorContext ),
                allocateFullAccessRelationshipScanCursor( cursorContext ) ) );
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return trace( new FullAccessPropertyCursor( DefaultPropertyCursor::release,
                storageReader.allocatePropertyCursor( cursorContext, memoryTracker ) ) );
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
