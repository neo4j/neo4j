/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.core;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.count;

/**
 * Tests a specific case of cache poisoning that involves the relationship chain of a node, the index the node has on it
 * and a deleted relationship.
 * The bug manifests thus:
 * A Node with at least grab_size+1 relationships is loaded.
 * The first batch of relationships is loaded
 * The relationships are traversed until the last one is reached in this batch.
 * The relationship that is the first of the next batch (the one pointed to by the current one in the iteration) is
 *  deleted without going through the node - a reference is acquired and deleted from there.
 * The cache is now poisoned. The next() call on the relationship iterator of that node will cause it to fail with
 *  a NotFoundException since the relationship chain position for the node points to something that is no longer in
 *  use. Since the load will fail the position is never updated and will always fail.
 * We must also make sure that if the transaction that deleted the relationship rolls back, we should not do anything to
 *  compensate for it.
 */
public class TestConcurrentModificationOfRelationshipChains
{
    private static final RelationshipType TYPE = withName( "POISON" );
    private static final int RelationshipGrabSize = 2, DenseNode = 50;

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.relationship_grab_size, "" + RelationshipGrabSize );
            builder.setConfig( GraphDatabaseSettings.dense_node_threshold, "" + DenseNode );
        }
    };

    private Node node1, node2;
    private Relationship firstFromSecondBatch;

    @Before
    public void given()
    {
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        try ( Transaction tx = graphDb.beginTx() )
        {
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            firstFromSecondBatch = node1.createRelationshipTo( node2, TYPE );

            for ( int i = 0; i < RelationshipGrabSize; i++ )
            {
                node1.createRelationshipTo( graphDb.createNode(), TYPE );
            }

            tx.success();
        }
        clearCaches();
        try ( Transaction tx = graphDb.beginTx() )
        {// make sure the first batch of relationships are loaded
            Iterator<Relationship> relationships = node1.getRelationships().iterator();
            for ( int i = 0; i < RelationshipGrabSize; i++ )
            {
                relationships.next();
            }
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromSameThreadReReadNode()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        deleteRelationshipInSameThread( firstFromSecondBatch );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( RelationshipGrabSize, count( node1.getRelationships() ) );
            tx.success();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreadReReadNode() throws InterruptedException
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        deleteRelationshipInDifferentThread( firstFromSecondBatch );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( RelationshipGrabSize, count( node1.getRelationships() ) );
            tx.success();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromSameThread()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        Iterator<Relationship> rels;
        try ( Transaction tx = graphDb.beginTx() )
        {
            rels = node1.getRelationships().iterator();
            tx.success();
        }
        deleteRelationshipInSameThread( firstFromSecondBatch );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( RelationshipGrabSize, count( rels ) );
            tx.success();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreads() throws InterruptedException
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        Iterator<Relationship> rels;
        try ( Transaction tx = graphDb.beginTx() )
        {
            rels = node1.getRelationships().iterator();
            tx.success();
        }
        deleteRelationshipInDifferentThread( firstFromSecondBatch );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( RelationshipGrabSize, count( rels ) );
            tx.success();
        }
    }

    @Test
    public void shouldNotInvalidateNodeInCacheOnRollback() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            node2.getSingleRelationship( TYPE, Direction.INCOMING ).delete();
            tx.failure();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( RelationshipGrabSize + 1, count( node1.getRelationships() ) );
            tx.success();
        }
    }

    @Test
    public void shouldNotInvalidateDenseNodeInCacheOnRollback() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Node node, other;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode();

            for ( int i = 0; i < DenseNode; i++ )
            {
                node.createRelationshipTo( graphDb.createNode(), TYPE );
            }
            other = graphDb.createNode();

            other.createRelationshipTo( node, TYPE );

            tx.success();
        }
        clearCaches();
        // make sure the node is in cache, but some relationships not loaded
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( Relationship one : node.getRelationships( TYPE, Direction.OUTGOING ) )
            {
                assertNotNull( one );
            }
            tx.success();
        }

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            other.getSingleRelationship( TYPE, Direction.OUTGOING ).delete();
            tx.failure();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals( DenseNode + 1, IteratorUtil.count( node.getRelationships( TYPE ) ) );
            tx.success();
        }
    }

    private static void deleteRelationshipInSameThread( Relationship toDelete )
    {
        try ( Transaction tx = toDelete.getGraphDatabase().beginTx() )
        {
            toDelete.delete();
            tx.success();
        }
    }

    private static void deleteRelationshipInDifferentThread( final Relationship toDelete ) throws InterruptedException
    {
        Runnable deleter = new Runnable()
        {
            @Override
            public void run()
            {
                deleteRelationshipInSameThread( toDelete );
            }
        };
        Thread t = new Thread( deleter );
        t.start();
        t.join();
    }

    private void clearCaches()
    {
        db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
    }
}
