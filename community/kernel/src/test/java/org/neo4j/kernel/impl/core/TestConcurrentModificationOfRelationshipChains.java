/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

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
    private static final int RelationshipGrabSize = 2;

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.relationship_grab_size, "" + RelationshipGrabSize );
        }
    };

    private Node node1, node2;
    private Relationship firstFromSecondBatch;

    @Before
    public void given()
    {
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Transaction tx = graphDb.beginTx();
        try
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
        finally
        {
            tx.finish();
        }
        clearCaches();
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
        // when
        deleteRelationshipInSameThread( firstFromSecondBatch );

        // then
        assertEquals( RelationshipGrabSize, count( node1.getRelationships() ) );
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreadReReadNode() throws InterruptedException
    {
        // when
        deleteRelationshipInDifferentThread( firstFromSecondBatch );

        // then
        assertEquals( RelationshipGrabSize, count( node1.getRelationships() ) );
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromSameThread()
    {
        // when
        Iterator<Relationship> rels = node1.getRelationships().iterator();
        deleteRelationshipInSameThread( firstFromSecondBatch );

        // then
        assertEquals( RelationshipGrabSize, count( rels ) );
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreads() throws InterruptedException
    {
        // when
        Iterator<Relationship> rels = node1.getRelationships().iterator();
        deleteRelationshipInDifferentThread( firstFromSecondBatch );

        // then
        assertEquals( RelationshipGrabSize, count( rels ) );
    }

    @Test
    public void shouldNotInvalidateNodeInCacheOnRollback() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();

        // when
        Transaction tx = graphDb.beginTx();
        try
        {
            node2.getSingleRelationship( TYPE, Direction.INCOMING ).delete();
            tx.failure();
        }
        finally
        {
            tx.finish();
        }

        // then
        assertEquals( RelationshipGrabSize + 1, count( node1.getRelationships() ) );
    }

    private static void deleteRelationshipInSameThread( Relationship toDelete )
    {
        Transaction tx = toDelete.getGraphDatabase().beginTx();
        toDelete.delete();
        tx.success();
        tx.finish();
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
