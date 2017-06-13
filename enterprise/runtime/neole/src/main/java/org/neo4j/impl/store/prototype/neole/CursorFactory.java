/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.impl.store.prototype.neole;

import org.neo4j.impl.kernel.api.EdgeSearchStructureCursor;
import org.neo4j.impl.kernel.api.NodeLabelIndexCursor;
import org.neo4j.impl.kernel.api.NodeSearchStructureCursor;
import org.neo4j.impl.kernel.api.NodeValueIndexCursor;

class CursorFactory implements org.neo4j.impl.kernel.api.CursorFactory
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
    public EdgeScanCursor allocateEdgeScanCursor()
    {
        return new org.neo4j.impl.store.prototype.neole.EdgeScanCursor( store );
    }

    @Override
    public EdgeTraversalCursor allocateEdgeTraversalCursor()
    {
        return new EdgeTraversalCursor( store );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new PropertyCursor( store, new ByteBlockCursor( store, store.dynamicStoreRecordSize() ) );
    }

    @Override
    public EdgeGroupCursor allocateEdgeGroupCursor()
    {
        return new EdgeGroupCursor( store, new EdgeTraversalCursor( store ) );
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
    public NodeSearchStructureCursor allocateNodeSearchStructureCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public EdgeSearchStructureCursor allocateEdgeSearchStructureCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
