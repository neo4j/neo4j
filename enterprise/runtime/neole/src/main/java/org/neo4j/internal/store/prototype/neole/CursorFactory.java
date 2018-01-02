/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.prototype.neole;

import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeManualIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipManualIndexCursor;

class CursorFactory implements org.neo4j.internal.kernel.api.CursorFactory
{
    private final ReadStore store;

    CursorFactory( ReadStore store )
    {
        this.store = store;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new NodeCursor( store );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new RelationshipScanCursor( store );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new RelationshipTraversalCursor( store );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new PropertyCursor();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new RelationshipGroupCursor( store, new RelationshipTraversalCursor( store ) );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public NodeManualIndexCursor allocateNodeManualIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RelationshipManualIndexCursor allocateRelationshipManualIndexCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
