/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Cursor factory which simply creates new instances on allocation. As thread-safe as the underlying {@link StorageReader}.
 */
public class DefaultThreadSafeCursors extends DefaultCursors implements CursorFactory
{
    private final StorageReader storageReader;

    public DefaultThreadSafeCursors( StorageReader storageReader )
    {
        super( new ConcurrentLinkedQueue<>() );
        this.storageReader = storageReader;
    }

    @Override
    public NodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        return trace( new DefaultNodeCursor(
                DefaultNodeCursor::release, storageReader.allocateNodeCursor( cursorTracer ) ) );
    }

    @Override
    public NodeCursor allocateFullAccessNodeCursor( PageCursorTracer cursorTracer )
    {
        return trace( new FullAccessNodeCursor( DefaultNodeCursor::release, storageReader.allocateNodeCursor( cursorTracer ) ) );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return trace( new DefaultRelationshipScanCursor( DefaultRelationshipScanCursor::release,
                storageReader.allocateRelationshipScanCursor( cursorTracer ), cursorTracer ) );
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return trace( new FullAccessRelationshipScanCursor(
                DefaultRelationshipScanCursor::release, storageReader.allocateRelationshipScanCursor( cursorTracer ), cursorTracer ) );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        return trace( new DefaultRelationshipTraversalCursor(
                DefaultRelationshipTraversalCursor::release, storageReader.allocateRelationshipTraversalCursor( cursorTracer ), cursorTracer ) );
    }

    @Override
    public PropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer )
    {
        return trace( new DefaultPropertyCursor(
                DefaultPropertyCursor::release, storageReader.allocatePropertyCursor( cursorTracer ), cursorTracer ) );
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor( PageCursorTracer cursorTracer )
    {
        return trace( new FullAccessPropertyCursor(
                DefaultPropertyCursor::release, storageReader.allocatePropertyCursor( cursorTracer ), cursorTracer ) );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return trace( new DefaultNodeValueIndexCursor(
                DefaultNodeValueIndexCursor::release ) );
    }

    @Override
    public FullAccessNodeValueIndexCursor allocateFullAccessNodeValueIndexCursor()
    {
        return trace( new FullAccessNodeValueIndexCursor( DefaultNodeValueIndexCursor::release ) );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return trace( new DefaultNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release ) );
    }

    @Override
    public NodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor()
    {
        return trace( new FullAccessNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release ) );
    }

    @Override
    public RelationshipIndexCursor allocateRelationshipIndexCursor()
    {
        return new DefaultRelationshipIndexCursor( DefaultRelationshipIndexCursor::release );
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor()
    {
        return trace( new DefaultRelationshipTypeIndexCursor( DefaultRelationshipTypeIndexCursor::release ) );
    }

    public void close()
    {
        assertClosed();
        storageReader.close();
    }
}
