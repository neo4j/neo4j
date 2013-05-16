/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.ImpermanentDatabaseRule;

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
 *
 *  The tests provided form a matrix. One axis is the deletion happening from the same or different threads. The
 *   other is the read happening from the iterator already acquired or from a node re-read from scratch. A fix should
 *   make all 4 green.
 */
public class TestConcurrentModificationOfRelationshipChains
{
    private static final int RelationshipGrabSize = 2;

    @Rule
    public ImpermanentDatabaseRule graphDb = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.relationship_grab_size, ""+RelationshipGrabSize );
        }
    };

    @Test
    public void relationshipChainPositionCachePoisoningFromSameThreadReReadNode()
    {
        GraphDatabaseAPI db = graphDb.getGraphDatabaseAPI();
        Triplet<Iterable<Relationship>, Long, Relationship> relsCreated = setup( db );
        Relationship firstFromSecondBatch = relsCreated.third();

        deleteRelationshipInSameThread( db, firstFromSecondBatch );

        for ( Relationship rel : db.getNodeById( relsCreated.second() ).getRelationships() )
        {
            rel.getId();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreadReReadNode() throws InterruptedException
    {
        GraphDatabaseAPI db = graphDb.getGraphDatabaseAPI();
        Triplet<Iterable<Relationship>, Long, Relationship> relsCreated = setup( db );
        Relationship firstFromSecondBatch = relsCreated.third();

        deleteRelationshipInDifferentThread( db, firstFromSecondBatch );

        for ( Relationship rel : db.getNodeById( relsCreated.second() ).getRelationships() )
        {
            rel.getId();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromSameThread()
    {
        GraphDatabaseAPI db = graphDb.getGraphDatabaseAPI();
        Triplet<Iterable<Relationship>, Long, Relationship> relsCreated = setup( db );
        Iterator<Relationship> rels = relsCreated.first().iterator();
        Relationship firstFromSecondBatch = relsCreated.third();

        deleteRelationshipInSameThread( db, firstFromSecondBatch );

        while ( rels.hasNext() )
        {
            rels.next();
        }
    }

    @Test
    public void relationshipChainPositionCachePoisoningFromDifferentThreads() throws InterruptedException
    {
        GraphDatabaseAPI db = graphDb.getGraphDatabaseAPI();
        Triplet<Iterable<Relationship>, Long, Relationship> relsCreated = setup( db );
        Iterator<Relationship> rels = relsCreated.first().iterator();
        Relationship firstFromSecondBatch = relsCreated.third();

        deleteRelationshipInDifferentThread( db, firstFromSecondBatch );

        while ( rels.hasNext() )
        {
            rels.next();
        }
    }

    private void deleteRelationshipInSameThread( GraphDatabaseAPI db, Relationship toDelete )
    {
        Transaction tx = db.beginTx();
        toDelete.delete();
        tx.success();
        tx.finish();
    }

    private void deleteRelationshipInDifferentThread( final GraphDatabaseAPI db, final Relationship toDelete ) throws
            InterruptedException
    {
        Runnable deleter = new Runnable()
        {
            @Override
            public void run()
            {
                deleteRelationshipInSameThread( db,  toDelete);
            }
        };
        Thread t = new Thread( deleter );
        t.start();
        t.join();
    }

    private Triplet<Iterable<Relationship>, Long, Relationship> setup( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        RelationshipType relType = DynamicRelationshipType.withName( "POISON" );
        Relationship firstFromSecondBatch = node1.createRelationshipTo( node2, relType );
        for ( int i = 0; i < RelationshipGrabSize; i++ )
        {
            node1.createRelationshipTo( node2, relType );
        }
        tx.success();
        tx.finish();

        db.getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
        return Triplet.of( node1.getRelationships(), node1.getId(), firstFromSecondBatch );
    }
}
