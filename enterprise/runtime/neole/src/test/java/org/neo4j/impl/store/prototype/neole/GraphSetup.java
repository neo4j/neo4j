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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.impl.kernel.api.*;
import org.neo4j.impl.kernel.api.EdgeGroupCursor;
import org.neo4j.impl.kernel.api.EdgeScanCursor;
import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.NodeCursor;
import org.neo4j.impl.kernel.api.PropertyCursor;

public abstract class GraphSetup extends TestResource implements Read, org.neo4j.impl.kernel.api.CursorFactory
{
    private final TemporaryFolder folder = new TemporaryFolder();
    private ReadStore store;
    private CursorFactory cursors;
    private final Map<Setting<?>,String> config = new HashMap<>();

    protected void before( Description description ) throws IOException
    {
        folder.create();
        GraphDatabaseService graphDb = null;
        try
        {
            GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( folder.getRoot() );
            for ( Map.Entry<Setting<?>,String> conf : config.entrySet() )
            {
                builder.setConfig( conf.getKey(), conf.getValue() );
            }
            create( graphDb = builder.newGraphDatabase() );
        }
        finally
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
            }
        }
        store = new ReadStore( folder.getRoot() );
        cursors = new CursorFactory( store );
    }

    public final GraphSetup withConfig( Setting<?> setting, String value )
    {
        config.put( setting, value );
        return this;
    }

    protected abstract void create( GraphDatabaseService graphDb );

    protected void afterFailure( Description description, Throwable failure )
    {
        after();
    }

    protected void afterSuccess( Description description )
    {
        after();
    }

    private void after()
    {
        try
        {
            store.shutdown();
            folder.delete();
            cleanup();
        }
        finally
        {
            cursors = null;
            store = null;
        }
    }

    protected void cleanup()
    {
    }

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexPredicate... predicates )
    {
        store.nodeIndexSeek( index, cursor, predicates );
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor )
    {
        store.nodeIndexScan( index, cursor );
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        store.nodeLabelScan( label, cursor );
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        return store.nodeLabelScan( label );
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        store.allNodesScan( cursor );
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        return store.allNodesScan();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        store.singleNode( reference, cursor );
    }

    @Override
    public void singleEdge( long reference, EdgeScanCursor cursor )
    {
        store.singleEdge( reference, cursor );
    }

    @Override
    public void allEdgesScan( EdgeScanCursor cursor )
    {
        store.allEdgesScan( cursor );
    }

    @Override
    public Scan<EdgeScanCursor> allEdgesScan()
    {
        return store.allEdgesScan();
    }

    @Override
    public void edgeLabelScan( int label, EdgeScanCursor cursor )
    {
        store.edgeLabelScan( label, cursor );
    }

    @Override
    public Scan<EdgeScanCursor> edgeLabelScan( int label )
    {
        return store.edgeLabelScan( label );
    }

    @Override
    public void edgeGroups( long nodeReference, long reference, EdgeGroupCursor cursor )
    {
        store.edgeGroups( nodeReference, reference, cursor );
    }

    @Override
    public void edges( long nodeReference, long reference, EdgeTraversalCursor cursor )
    {
        store.edges( nodeReference, reference, cursor );
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        store.nodeProperties( reference, cursor );
    }

    @Override
    public void edgeProperties( long reference, PropertyCursor cursor )
    {
        store.edgeProperties( reference, cursor );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        store.futureNodeReferenceRead( reference );
    }

    @Override
    public void futureEdgeReferenceRead( long reference )
    {
        store.futureEdgeReferenceRead( reference );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        store.futureNodePropertyReferenceRead( reference );
    }

    @Override
    public void futureEdgePropertyReferenceRead( long reference )
    {
        store.futureEdgePropertyReferenceRead( reference );
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return cursors.allocateNodeCursor();
    }

    @Override
    public EdgeScanCursor allocateEdgeScanCursor()
    {
        return cursors.allocateEdgeScanCursor();
    }

    @Override
    public EdgeTraversalCursor allocateEdgeTraversalCursor()
    {
        return cursors.allocateEdgeTraversalCursor();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return cursors.allocatePropertyCursor();
    }

    @Override
    public EdgeGroupCursor allocateEdgeGroupCursor()
    {
        return cursors.allocateEdgeGroupCursor();
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return cursors.allocateNodeValueIndexCursor();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return cursors.allocateNodeLabelIndexCursor();
    }

    @Override
    public NodeSearchStructureCursor allocateNodeSearchStructureCursor()
    {
        return cursors.allocateNodeSearchStructureCursor();
    }

    @Override
    public EdgeSearchStructureCursor allocateEdgeSearchStructureCursor()
    {
        return cursors.allocateEdgeSearchStructureCursor();
    }
}
