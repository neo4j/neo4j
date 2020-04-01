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
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
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
    public DefaultNodeCursor allocateNodeCursor()
    {
        return trace( new DefaultNodeCursor(
                DefaultNodeCursor::release, storageReader.allocateNodeCursor() ) );
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor()
    {
        return trace( new FullAccessNodeCursor( DefaultNodeCursor::release, storageReader.allocateNodeCursor() ) );
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        return trace( new DefaultRelationshipScanCursor(
                DefaultRelationshipScanCursor::release, storageReader.allocateRelationshipScanCursor(), allocateNodeCursor() ) );
    }

    @Override
    public FullAccessRelationshipScanCursor allocateFullAccessRelationshipScanCursor()
    {
        return trace( new FullAccessRelationshipScanCursor(
                DefaultRelationshipScanCursor::release, storageReader.allocateRelationshipScanCursor(), allocateFullAccessNodeCursor() ) );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return trace( new DefaultRelationshipTraversalCursor(
                DefaultRelationshipTraversalCursor::release, storageReader.allocateRelationshipTraversalCursor(), allocateNodeCursor() ) );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return trace( new DefaultPropertyCursor(
                DefaultPropertyCursor::release, storageReader.allocatePropertyCursor(), allocateFullAccessNodeCursor(),
                allocateFullAccessRelationshipScanCursor() ) );
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor()
    {
        return trace( new FullAccessPropertyCursor(
                DefaultPropertyCursor::release, storageReader.allocatePropertyCursor(), allocateFullAccessNodeCursor(),
                allocateFullAccessRelationshipScanCursor() ) );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return trace( new DefaultRelationshipGroupCursor(
                DefaultRelationshipGroupCursor::release, storageReader.allocateRelationshipGroupCursor(), allocateRelationshipTraversalCursor() ) );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return trace( new DefaultNodeValueIndexCursor(
                DefaultNodeValueIndexCursor::release, allocateNodeCursor() ) );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return trace( new DefaultNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release, allocateNodeCursor() ) );
    }

    @Override
    public RelationshipIndexCursor allocateRelationshipIndexCursor()
    {
        return trace( new DefaultRelationshipIndexCursor( DefaultRelationshipIndexCursor::release, allocateRelationshipScanCursor() ) );
    }

    public void close()
    {
        assertClosed();
        storageReader.close();
    }
}
