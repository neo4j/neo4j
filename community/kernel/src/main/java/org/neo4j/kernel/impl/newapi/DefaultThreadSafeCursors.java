/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Cursor factory which simply creates new instances on allocation. As thread-safe as the underlying {@link StorageReader}.
 */
public class DefaultThreadSafeCursors implements CursorFactory
{
    private final StorageReader storageReader;

    public DefaultThreadSafeCursors( StorageReader storageReader )
    {
        this.storageReader = storageReader;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new DefaultNodeCursor( DefaultNodeCursor::release, storageReader.allocateNodeCursor() );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new DefaultRelationshipScanCursor( DefaultRelationshipScanCursor::release, storageReader.allocateRelationshipScanCursor() );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new DefaultRelationshipTraversalCursor( DefaultRelationshipTraversalCursor::release, storageReader.allocateRelationshipTraversalCursor() );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new DefaultPropertyCursor( DefaultPropertyCursor::release, storageReader.allocatePropertyCursor() );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new DefaultRelationshipGroupCursor( DefaultRelationshipGroupCursor::release, storageReader.allocateRelationshipGroupCursor() );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new DefaultNodeValueIndexCursor( DefaultNodeValueIndexCursor::release );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new DefaultNodeLabelIndexCursor( DefaultNodeLabelIndexCursor::release );
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return new DefaultNodeExplicitIndexCursor( DefaultNodeExplicitIndexCursor::release );
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return new DefaultRelationshipExplicitIndexCursor(
                new DefaultRelationshipScanCursor( null, storageReader.allocateRelationshipScanCursor() ),
                DefaultRelationshipExplicitIndexCursor::release );
    }
}
