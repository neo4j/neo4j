/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
import static org.junit.Assert.assertNotNull;

import static java.lang.String.format;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

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
            List<String> found = stringsOf( node1.getRelationships().iterator() );
            assertEquals( join( "\n\t", found ), RelationshipGrabSize, found.size() );
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
            List<String> found = stringsOf( node1.getRelationships().iterator() );
            assertEquals( join( "\n\t", found ), RelationshipGrabSize , found.size() );
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
            List<String> found = stringsOf( rels );
            assertEquals( join( "\n\t", found ), RelationshipGrabSize, found.size() );
            tx.success();
        }
    }

    @Test
    @Ignore("2014-09-17: Ignored because it is questionable if we should support transferring an iterator from one " +
            "transaction to another like this. This issue could still manifest with multiple threads, but at that " +
            "point it is essentially a regular concurrent update issue, and \"expected behaviour.\"\n" +
            "The test fails because the iterator from the first tx references state that gets evicted from the cache " +
            "by the deleting transaction. The transaction in between makes sure that the evicted state is fully " +
            "populated before being evicted, thus manifesting the issue.")
    public void relationshipChainPositionCachePoisoningFromSameThreadWithReadsInBetween()
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
        List<String> before;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = stringsOf( node1.getRelationships().iterator() );
            tx.success();
        }
        deleteRelationshipInSameThread( firstFromSecondBatch );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            List<String> found = stringsOf( rels );
            assertEquals( join( "\n\t", found ) + "\nBefore delete:" + join( "\n\t", before ),
                          RelationshipGrabSize, found.size() );
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
            List<String> found = stringsOf( rels );
            assertEquals( join( "\n\t", found ), RelationshipGrabSize, found.size() );
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
            List<String> found = stringsOf( node1.getRelationships().iterator() );
            assertEquals( join( "\n\t", found ), RelationshipGrabSize + 1, found.size() );
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
            List<String> found = stringsOf( node.getRelationships( TYPE ).iterator() );
            assertEquals( join( "\n\t", found ), DenseNode + 1, found.size() );
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

    private static List<String> stringsOf( Iterator<Relationship> rels )
    {
        List<String> strings = new ArrayList<>();
        while ( rels.hasNext() )
        {
            Relationship rel = rels.next();
            try
            {
                strings.add( format( "(%d)-[%d]->(%d)", rel.getStartNode().getId(), rel.getId(),
                                     rel.getEndNode().getId() ) );
            }
            catch ( Exception e )
            {
                strings.add( rel + " - " + e );
            }
        }
        return strings;
    }

    private static String join( String sep, Collection<?> items )
    {
        StringBuilder result = new StringBuilder();
        for ( Object item : items )
        {
            result.append( sep ).append( item );
        }
        return result.toString();
    }
}
