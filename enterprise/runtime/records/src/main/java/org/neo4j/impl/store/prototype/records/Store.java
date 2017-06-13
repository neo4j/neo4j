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
package org.neo4j.impl.store.prototype.records;

import java.io.File;

import org.neo4j.impl.kernel.api.CursorFactory;
import org.neo4j.impl.kernel.api.EdgeScanCursor;
import org.neo4j.impl.kernel.api.EdgeGroupCursor;
import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.IndexPredicate;
import org.neo4j.impl.kernel.api.IndexReference;
import org.neo4j.impl.kernel.api.NodeCursor;
import org.neo4j.impl.kernel.api.NodeLabelIndexCursor;
import org.neo4j.impl.kernel.api.NodeValueIndexCursor;
import org.neo4j.impl.kernel.api.PropertyCursor;
import org.neo4j.impl.kernel.api.Read;
import org.neo4j.impl.kernel.api.Scan;

public class Store implements Read
{
    public Store( File storeDir )
    {
    }

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexPredicate... predicates )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void singleEdge( long reference, EdgeScanCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allEdgesScan( EdgeScanCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<EdgeScanCursor> allEdgesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void edgeLabelScan( int label, EdgeScanCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<EdgeScanCursor> edgeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void edgeGroups( long nodeReference, long reference, EdgeGroupCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void edges( long nodeReference, long reference, EdgeTraversalCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void edgeProperties( long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureEdgeReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureEdgePropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public CursorFactory cursorFactory()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
