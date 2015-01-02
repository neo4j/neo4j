/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class NodeManagerTest
{
    private GraphDatabaseAPI db;

    @Before
    public void init()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }
    
    @After
    public void stop()
    {
        db.shutdown();
    }

    @Test
    public void shouldRemoveAllPropertiesWhenDeletingNode() throws Exception
    {
        // GIVEN
        Node node = createNodeWith( "wut", "foo" );

        NodeManager nodeManager = getNodeManager();
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN
        delete( node );

        //THEN
        assertThat( tracker.removed, is( "0:wut" ) );
    }

    @Test
    public void shouldNotRemovePropertyTwice() throws Exception
    {
        // GIVEN node with one property
        Node node = createNodeWith( "wut", "foo" );

        NodeManager nodeManager = getNodeManager();
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN in the same tx, remove prop and then delete node
        Transaction tx = db.beginTx();
        node.removeProperty( "wut" );
        node.delete();
        tx.success();
        tx.finish();

        //THEN prop is removed only once
        assertThat( tracker.removed, is( "0:wut" ) );
    }

    @Test
    public void shouldRemoveRelationshipProperties() throws Exception
    {
        // GIVEN relationship with one property
        Relationship relationship = createRelationshipWith( "wut", "foo" );

        NodeManager nodeManager = getNodeManager();
        RelationshipLoggingTracker tracker = new RelationshipLoggingTracker();
        nodeManager.addRelationshipPropertyTracker( tracker );

        // WHEN
        delete( relationship );

        //THEN prop is removed from tracker
        assertThat( tracker.removed, is( "0:wut" ) );
    }
    
    @Test
    public void getAllNodesShouldConsiderTxState() throws Exception
    {
        // GIVEN
        // Three nodes
        Transaction tx = db.beginTx();
        Node firstCommittedNode = db.createNode();
        Node secondCommittedNode = db.createNode();
        Node thirdCommittedNode = db.createNode();
        tx.success();
        tx.finish();
        
        // Second one deleted, just to create a hole
        tx = db.beginTx();
        secondCommittedNode.delete();
        tx.success();
        tx.finish();
        
        // WHEN
        tx = db.beginTx();
        Node firstAdditionalNode = db.createNode();
        Node secondAdditionalNode = db.createNode();
        thirdCommittedNode.delete();
        List<Node> allNodes = addToCollection( getNodeManager().getAllNodes(), new ArrayList<Node>() );
        Set<Node> allNodesSet = new HashSet<>( allNodes );
        tx.finish();

        // THEN
        assertEquals( allNodes.size(), allNodesSet.size() );
        assertEquals( asSet( firstCommittedNode, firstAdditionalNode, secondAdditionalNode ), allNodesSet );
    }
    
    @Test
    public void getAllRelationshipsShouldConsiderTxState() throws Exception
    {
        // GIVEN
        // Three relationships
        Transaction tx = db.beginTx();
        Relationship firstCommittedRelationship = createRelationshipAssumingTxWith( "committed", 1 );
        Relationship secondCommittedRelationship = createRelationshipAssumingTxWith( "committed", 2 );
        Relationship thirdCommittedRelationship = createRelationshipAssumingTxWith( "committed", 3 );
        tx.success();
        tx.finish();
        
        tx = db.beginTx();
        secondCommittedRelationship.delete();
        tx.success();
        tx.finish();
        
        // WHEN
        tx = db.beginTx();
        Relationship firstAdditionalRelationship = createRelationshipAssumingTxWith( "additional", 1 );
        Relationship secondAdditionalRelationship = createRelationshipAssumingTxWith( "additional", 2 );
        thirdCommittedRelationship.delete();
        List<Relationship> allRelationships = addToCollection( getNodeManager().getAllRelationships(),
                new ArrayList<Relationship>() );
        Set<Relationship> allRelationshipsSet = new HashSet<>( allRelationships );
        tx.finish();

        // THEN
        assertEquals( allRelationships.size(), allRelationshipsSet.size() );
        assertEquals( asSet( firstCommittedRelationship, firstAdditionalRelationship, secondAdditionalRelationship ),
                allRelationshipsSet );
    }

    @Test
    public void getAllNodesIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        {
            Transaction tx = db.beginTx();
            db.createNode();
            db.createNode();
            tx.success();
            tx.finish();
        }

        // WHEN iterator is started
        Transaction transaction = db.beginTx();
        Iterator<Node> allNodes = GlobalGraphOperations.at( db ).getAllNodes().iterator();
        allNodes.next();

        // and WHEN another node is then added
        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                Transaction newTx = db.beginTx();
                assertThat( newTx, not( instanceOf( PlaceboTransaction.class ) ) );
                db.createNode();
                newTx.success();
                newTx.finish();
            }
        } );
        thread.start();
        thread.join();


        // THEN the new node is picked up by the iterator
        assertThat( addToCollection( allNodes, new ArrayList<Node>() ).size(), is( 2 ) );
        transaction.finish();
    }
    
    @Test
    public void getAllRelationshipsIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        Relationship relationship1 = createRelationshipAssumingTxWith( "key", 1 );
        Relationship relationship2 = createRelationshipAssumingTxWith( "key", 2 );
        tx.success();
        tx.finish();
        
        // WHEN
        tx = db.beginTx();
        Iterator<Relationship> allRelationships = GlobalGraphOperations.at( db ).getAllRelationships().iterator();
        Relationship relationship3 = createRelationshipAssumingTxWith( "key", 3 );

        // THEN
        assertEquals( asList( relationship1, relationship2, relationship3 ),
                addToCollection( allRelationships, new ArrayList<Relationship>() ) );
        tx.success();
        tx.finish();
    }

    private void delete( Relationship relationship )
    {
        Transaction tx = db.beginTx();
        relationship.delete();
        tx.success();
        tx.finish();
    }

    private Node createNodeWith( String key, Object value )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
        return node;
    }


    private Relationship createRelationshipWith( String key, Object value )
    {
        Transaction tx = db.beginTx();
        Relationship relationship = createRelationshipAssumingTxWith( key, value );
        tx.success();
        tx.finish();
        return relationship;
    }

    private Relationship createRelationshipAssumingTxWith( String key, Object value )
    {
        Node a = db.createNode();
        Node b = db.createNode();
        Relationship relationship = a.createRelationshipTo( b, DynamicRelationshipType.withName( "FOO" ) );
        relationship.setProperty( key, value );
        return relationship;
    }

    private void delete( Node node )
    {
        Transaction tx = db.beginTx();
        node.delete();
        tx.success();
        tx.finish();
    }

    private class NodeLoggingTracker implements PropertyTracker<Node>
    {
        @Override
        public void propertyAdded( Node primitive, String propertyName, Object propertyValue )
        {
        }

        public String removed = "";

        @Override
        public void propertyRemoved( Node primitive, String propertyName, Object propertyValue )
        {
            removed = removed + primitive.getId() + ":" + propertyName;
        }

        @Override
        public void propertyChanged( Node primitive, String propertyName, Object oldValue, Object newValue )
        {
        }
    }

    private class RelationshipLoggingTracker implements PropertyTracker<Relationship>
    {
        @Override
        public void propertyAdded( Relationship primitive, String propertyName, Object propertyValue )
        {
        }

        public String removed = "";

        @Override
        public void propertyRemoved( Relationship primitive, String propertyName, Object propertyValue )
        {
            removed = removed + primitive.getId() + ":" + propertyName;
        }

        @Override
        public void propertyChanged( Relationship primitive, String propertyName, Object oldValue, Object newValue )
        {
        }
    }

    private NodeManager getNodeManager()
    {
        return db.getDependencyResolver().resolveDependency( NodeManager.class );
    }
}
