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
package org.neo4j.internal.store.prototype.neole;

import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeManualIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipManualIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;

public abstract class GraphSetup extends TestResource implements Read, org.neo4j.internal.kernel.api.CursorFactory
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
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        store.singleRelationship( reference, cursor );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        store.allRelationshipsScan( cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        return store.allRelationshipsScan();
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        store.relationshipLabelScan( label, cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        return store.relationshipLabelScan( label );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        store.relationshipGroups( nodeReference, reference, cursor );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        store.relationships( nodeReference, reference, cursor );
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        store.nodeProperties( reference, cursor );
    }

    @Override
    public void relationshipProperties( long reference, PropertyCursor cursor )
    {
        store.relationshipProperties( reference, cursor );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        store.futureNodeReferenceRead( reference );
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        store.futureRelationshipsReferenceRead( reference );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        store.futureNodePropertyReferenceRead( reference );
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        store.futureRelationshipPropertyReferenceRead( reference );
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return cursors.allocateNodeCursor();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return cursors.allocateRelationshipScanCursor();
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return cursors.allocateRelationshipTraversalCursor();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return cursors.allocatePropertyCursor();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return cursors.allocateRelationshipGroupCursor();
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
    public NodeManualIndexCursor allocateNodeManualIndexCursor()
    {
        return cursors.allocateNodeManualIndexCursor();
    }

    @Override
    public RelationshipManualIndexCursor allocateRelationshipManualIndexCursor()
    {
        return cursors.allocateRelationshipManualIndexCursor();
    }
}
