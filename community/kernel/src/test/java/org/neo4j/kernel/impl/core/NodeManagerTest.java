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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;

public class NodeManagerTest
{
    @Test
    public void shouldRemoveAllPropertiesWhenDeletingNode() throws Exception
    {
        // GIVEN
        GraphDatabaseService database = createEmptyDatabase();
        Node node = createNodeWith( "wut", "foo", database );
        NodeManager nodeManager = getNodeManager( database );
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN
        delete( node, database );

        //THEN
        assertThat( tracker.removed, is( "1:wut" ) );
    }

    @Test
    public void shouldNotRemovePropertyTwice() throws Exception
    {
        // GIVEN node with one property
        GraphDatabaseService database = createEmptyDatabase();;
        Node node = createNodeWith( "wut", "foo", database );
        NodeManager nodeManager = getNodeManager( database );
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN in the same tx, remove prop and then delete node
        Transaction tx = database.beginTx();
        node.removeProperty( "wut" );
        node.delete();
        tx.success();
        tx.finish();

        //THEN prop is removed only once
        assertThat( tracker.removed, is( "1:wut" ) );
    }

    @Test
    public void shouldRemoveRelationshipProperties() throws Exception
    {
        // GIVEN relationship with one property
        GraphDatabaseService database = createEmptyDatabase();;
        Relationship relationship = createRelationshipWith( "wut", "foo", database );

        NodeManager nodeManager = getNodeManager( database );
        RelationshipLoggingTracker tracker = new RelationshipLoggingTracker();
        nodeManager.addRelationshipPropertyTracker( tracker );

        // WHEN
        delete( relationship, database );

        //THEN prop is removed from tracker
        assertThat( tracker.removed, is( "0:wut" ) );
    }

    @Test
    public void shouldRemoveRelationshipsFromTxStateOnDelete()
    {
        // given
        GraphDatabaseService database = createEmptyDatabase();
        Transaction tx = database.beginTx();
        createRelationshipAssumingTxWith( "committed", 1, database );
        Relationship secondCommittedRelationship = createRelationshipAssumingTxWith( "committed", 2, database );
        createRelationshipAssumingTxWith( "committed", 3, database );
        tx.success();
        tx.finish();

        // when
        tx = database.beginTx();
        secondCommittedRelationship.delete();
        tx.success();
        tx.finish();

        //then
        tx = database.beginTx();
        List<Relationship> currentRelationships = addToCollection( getNodeManager( database ).getAllRelationships(),
                new ArrayList<Relationship>() );
        tx.finish();

        assertEquals( 2, currentRelationships.size() );
    }

    @Test
    public void shouldPerformNodeCountOutsideATransaction()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Iterator<Node> allNodes = getNodeManager( database ).getAllNodes();

        // then
        assertEquals( 10, IteratorUtil.count( allNodes ) );
    }


    @Test
    public void shouldTakeTransactionStateWithCreatedNodeIntoAccountWhenPerformingNodeCount()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Transaction transaction = database.beginTx();
        try
        {
            database.createNode();

            // then
            Iterator<Node> allNodes = getNodeManager( database ).getAllNodes();
            assertEquals( 11, IteratorUtil.count( allNodes ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void shouldTakeTransactionStateWithDeletedNodeIntoAccountWhenPerformingNodeCount()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Transaction transaction = database.beginTx();
        try
        {
            database.getNodeById( 1 ).delete();

            // then
            Iterator<Node> allNodes = getNodeManager( database ).getAllNodes();
            assertEquals( 9, IteratorUtil.count( allNodes ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void shouldPerformRelationshipCountOutsideATransaction()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Iterator<Relationship> allRelationships = getNodeManager( database ).getAllRelationships();

        // then
        assertEquals( 9, IteratorUtil.count( allRelationships ) );
    }

    @Test
    public void shouldTakeTransactionStateWithCreatedRelationshipIntoAccountWhenPerformingRelationshipCount()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Transaction transaction = database.beginTx();
        try
        {
            Node node1 = database.getNodeById( 1 );
            Node node2 = database.getNodeById( 2 );

            node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "FOO" ) );

            // then
            Iterator<Relationship> allRelationships = getNodeManager( database ).getAllRelationships();
            assertEquals( 10, IteratorUtil.count( allRelationships ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void shouldTakeTransactionStateWithDeletedRelationshipIntoAccountWhenPerformingRelationshipCount()
    {
        // given
        GraphDatabaseService database = createDatabaseContainingLinkedListOfNodes( 10 );

        // when
        Transaction transaction = database.beginTx();
        try
        {
            database.getRelationshipById( 1 ).delete();

            // then
            Iterator<Relationship> allRelationships = getNodeManager( database ).getAllRelationships();
            assertEquals( 8, IteratorUtil.count( allRelationships ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    private GraphDatabaseService createEmptyDatabase()
    {
        return createDatabaseContainingLinkedListOfNodes( 0 );
    }

    private GraphDatabaseService createDatabaseContainingLinkedListOfNodes( int numberOfNodes )
    {
        ImpermanentGraphDatabase database = new ImpermanentGraphDatabase();

        Transaction transaction = database.beginTx();
        try
        {
            ensureDatabaseIsEmpty( database );

            createASinglyLinkedList( numberOfNodes, database );

            transaction.success();
        }
        finally
        {
            transaction.finish();
        }

        return database;
    }

    private void createASinglyLinkedList( int numberOfNodes, ImpermanentGraphDatabase database )
    {
        Node previous = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            Node current = database.createNode();
            current.setProperty( "id", i );
            if ( previous != null )
            {
                current.createRelationshipTo( previous, DynamicRelationshipType.withName( "PREVIOUS" ) );
            }

            previous = current;
        }
    }

    private void ensureDatabaseIsEmpty( ImpermanentGraphDatabase database )
    {
        for ( Node node : database.getAllNodes() )
        {
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }

            node.delete();
        }
    }


    private void delete( Relationship relationship, GraphDatabaseService database )
    {
        Transaction tx = database.beginTx();
        relationship.delete();
        tx.success();
        tx.finish();
    }

    private Node createNodeWith( String key, Object value, GraphDatabaseService database )
    {
        Transaction tx = database.beginTx();
        Node node = database.createNode();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
        return node;
    }


    private Relationship createRelationshipWith( String key, Object value, GraphDatabaseService database )
    {
        Transaction tx = database.beginTx();
        Relationship relationship = createRelationshipAssumingTxWith( key, value, database );
        tx.success();
        tx.finish();
        return relationship;
    }

    private Relationship createRelationshipAssumingTxWith( String key, Object value, GraphDatabaseService database )
    {
        Node a = database.createNode();
        Node b = database.createNode();
        Relationship relationship = a.createRelationshipTo( b, DynamicRelationshipType.withName( "FOO" ) );
        relationship.setProperty( key, value );
        return relationship;
    }

    private void delete( Node node, GraphDatabaseService database )
    {
        Transaction tx = database.beginTx();
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

    private NodeManager getNodeManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getNodeManager();
    }
}
