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

import java.util.Collection;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.valueOf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

public class TestRelationshipGrabSize
{
    private static final int GRAB_SIZE = 10;
    private static GraphDatabaseAPI db;
    private Transaction tx;

    @BeforeClass
    public static void doBefore() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.relationship_grab_size, valueOf( GRAB_SIZE ) )
                .newGraphDatabase();
    }

    @AfterClass
    public static void doAfter() throws Exception
    {
        db.shutdown();
    }

    private void beginTx()
    {
        tx = db.beginTx();
    }

    private void finishTx( boolean success )
    {
        if ( success )
        {
            tx.success();
        }
        tx.close();
    }

    @Test
    public void deleteRelationshipFromNotFullyLoadedNode() throws Exception
    {
        beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Node node3 = db.createNode();
        RelationshipType type1 = DynamicRelationshipType.withName( "type1" );
        RelationshipType type2 = DynamicRelationshipType.withName( "type2" );
        // This will the last relationship in the chain
        node1.createRelationshipTo( node3, type1 );
        Collection<Relationship> type2Relationships = new HashSet<>();
        // Create exactly grabSize relationships and store them in a set
        for ( int i = 0; i < GRAB_SIZE; i++ )
        {
            type2Relationships.add( node1.createRelationshipTo( node2, type2 ) );
        }
        finishTx( true );


        /*
         * Here node1 has grabSize+1 relationships. The first grabSize to be loaded will be
         * the type2 ones to node2 and the one remaining will be the type1 to node3.
         */

        beginTx();
        node1 = db.getNodeById( node1.getId() );
        node2 = db.getNodeById( node2.getId() );
        node3 = db.getNodeById( node3.getId() );

        // Will load <grabsize> relationships, not all, and not relationships of
        // type1 since it's the last one (the 11'th) in the chain.
        node1.getRelationships().iterator().next();

        // Delete the non-grabbed (from node1 POV) relationship
        node3.getRelationships().iterator().next().delete();
        // Just making sure
        assertFalse( node3.getRelationships().iterator().hasNext() );

        /*
         *  Now all Relationships left on node1 should be of type2
         *  This also checks that deletes on relationships are visible in the same tx.
         */
        assertEquals( type2Relationships, addToCollection( node1.getRelationships(), new HashSet<Relationship>() ) );

        finishTx( true );
        beginTx();
        assertEquals( type2Relationships, addToCollection( node1.getRelationships(), new HashSet<Relationship>() ) );
        finishTx( false );
    }

    @Test
    public void commitToNotFullyLoadedNode() throws Exception
    {
        beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        RelationshipType type = DynamicRelationshipType.withName( "type" );
        for ( int i = 0; i < GRAB_SIZE + 2; i++ )
        {
            node1.createRelationshipTo( node2, type );
        }
        finishTx( true );


        beginTx();


        node1.getRelationships().iterator().next().delete();
        node1.setProperty( "foo", "bar" );
        int relCount = 0;
        for ( Relationship rel : node2.getRelationships() )
        {
            relCount++;
        }
        assertEquals( GRAB_SIZE + 1, relCount );
        relCount = 0;
        for ( Relationship rel : node1.getRelationships() )
        {
            relCount++;
        }
        assertEquals( GRAB_SIZE + 1, relCount );
        assertEquals( "bar", node1.getProperty( "foo" ) );
        finishTx( true );
    }

    @Test
    public void createRelationshipAfterClearedCache()
    {
        // Assumes relationship grab size 100
        beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        int expectedCount = 0;
        for ( int i = 0; i < 150; i++ )
        {
            node1.createRelationshipTo( node2, TEST );
            expectedCount++;
        }
        finishTx( true );
        beginTx();
        for ( int i = 0; i < 50; i++ )
        {
            node1.createRelationshipTo( node2, TEST );
            expectedCount++;
        }
        assertEquals( expectedCount, count( node1.getRelationships() ) );
        finishTx( true );
        beginTx();
        assertEquals( expectedCount, count( node1.getRelationships() ) );
        finishTx( false );
    }

    @Test
    public void grabSizeWithTwoTypesDeleteAndCount()
    {
        beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();

        int count = 0;
        RelationshipType type1 = DynamicRelationshipType.withName( "type" );
        RelationshipType type2 = DynamicRelationshipType.withName( "bar" );
        // Create more than one grab size
        for ( int i = 0; i < 12; i++ )
        {
            Relationship rel = node1.createRelationshipTo( node2, type1 );
            count++;
        }
        for ( int i = 0; i < 11; i++ )
        {
            Relationship rel = node1.createRelationshipTo( node2, type2 );
            count++;
        }
        finishTx( true );

        clearCacheAndCreateDeleteCount( db, node1, node2, type1, type2, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type2, type1, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type1, type1, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type2, type2, count );
    }

    private void clearCacheAndCreateDeleteCount( GraphDatabaseAPI db, Node node1, Node node2,
            RelationshipType createType, RelationshipType deleteType, int expectedCount )
    {
        try ( Transaction tx = db.beginTx() )
        {

            node1.createRelationshipTo( node2, createType );
            node1.getRelationships( deleteType ).iterator().next().delete();

            assertEquals( expectedCount, count( node1.getRelationships() ) );
            assertEquals( expectedCount, count( node2.getRelationships() ) );

            tx.success();
        }


        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( expectedCount, count( node1.getRelationships() ) );
            assertEquals( expectedCount, count( node2.getRelationships() ) );
            tx.success();
        }
    }
}
