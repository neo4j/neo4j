/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestRelationship extends AbstractNeo4jTestCase
{
    private String key1 = "key1";
    private String key2 = "key2";
    private String key3 = "key3";

    private enum RelType implements RelationshipType
    {
        TYPE_GENERIC
    }

    @Test
    public void testSimple()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        Relationship rel2 = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        rel1.delete();
        newTransaction();
        assertTrue( node1.getRelationships().iterator().hasNext() );
        assertTrue( node2.getRelationships().iterator().hasNext() );
        assertTrue( node1.getRelationships(
            MyRelTypes.TEST ).iterator().hasNext() );
        assertTrue( node2.getRelationships(
            MyRelTypes.TEST ).iterator().hasNext() );
        assertTrue( node1.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ).iterator().hasNext() );
        assertTrue( node2.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ).iterator().hasNext() );
    }

    /*
    public static void main( String[] args )
    {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase(
                "target/iAmAwesome" );
        Transaction tx = db.beginTx();
        Node n1 = db.createNode();
        Node n2 = db.createNode();
        n1.createRelationshipTo( n2, TEST );
        tx.success();
        tx.finish();
        db.getConfig().getGraphDbModule().getNodeManager().clearCache();
        // n1.getSingleRelationship( RelType.TYPE_GENERIC, Direction.BOTH );
        for ( Node n : db.getAllNodes() )
        {
            for ( Relationship rel : n.getRelationships() )
            {
                n.getSingleRelationship( rel.getType(), Direction.BOTH );
            }
        }
    }
    */



    @Test
    public void testSimple2()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        for ( int i = 0; i < 3; i++ )
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
            node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        }
        allGetRelationshipMethods( node1, Direction.OUTGOING );
        allGetRelationshipMethods( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods( node1, Direction.OUTGOING );
        allGetRelationshipMethods( node2, Direction.INCOMING );
        node1.getRelationships( MyRelTypes.TEST,
            Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST_TRAVERSAL,
            Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST2,
            Direction.OUTGOING ).iterator().next().delete();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
        node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        allGetRelationshipMethods( node1, Direction.OUTGOING );
        allGetRelationshipMethods( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods( node1, Direction.OUTGOING );
        allGetRelationshipMethods( node2, Direction.INCOMING );
        for ( Relationship rel : node1.getRelationships() )
        {
            rel.delete();
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testSimple3()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        for ( int i = 0; i < 1; i++ )
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
            node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        }
        allGetRelationshipMethods2( node1, Direction.OUTGOING );
        allGetRelationshipMethods2( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods2( node1, Direction.OUTGOING );
        allGetRelationshipMethods2( node2, Direction.INCOMING );
        node1.getRelationships( MyRelTypes.TEST,
            Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST_TRAVERSAL,
            Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST2,
            Direction.OUTGOING ).iterator().next().delete();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
        node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        allGetRelationshipMethods2( node1, Direction.OUTGOING );
        allGetRelationshipMethods2( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods2( node1, Direction.OUTGOING );
        allGetRelationshipMethods2( node2, Direction.INCOMING );
        for ( Relationship rel : node1.getRelationships() )
        {
            rel.delete();
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testSimple4()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        for ( int i = 0; i < 2; i++ )
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
            node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        }
        allGetRelationshipMethods3( node1, Direction.OUTGOING );
        allGetRelationshipMethods3( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods3( node1, Direction.OUTGOING );
        allGetRelationshipMethods3( node2, Direction.INCOMING );
        node1.getRelationships( MyRelTypes.TEST,
            Direction.OUTGOING ).iterator().next().delete();
        int count = 0;
        for ( Relationship rel : node1.getRelationships( MyRelTypes.TEST_TRAVERSAL,
            Direction.OUTGOING ) )
        {
            if ( count == 1 )
            {
                rel.delete();
            }
            count++;
        }
        node1.getRelationships( MyRelTypes.TEST2,
            Direction.OUTGOING ).iterator().next().delete();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
        node1.createRelationshipTo( node2, MyRelTypes.TEST2 );
        allGetRelationshipMethods3( node1, Direction.OUTGOING );
        allGetRelationshipMethods3( node2, Direction.INCOMING );
        newTransaction();
        allGetRelationshipMethods3( node1, Direction.OUTGOING );
        allGetRelationshipMethods3( node2, Direction.INCOMING );
        for ( Relationship rel : node1.getRelationships() )
        {
            rel.delete();
        }
        node1.delete();
        node2.delete();
    }

    private void allGetRelationshipMethods( Node node, Direction dir )
    {
        countRelationships( 9, node.getRelationships() );
        countRelationships( 9, node.getRelationships( dir ) );
        countRelationships( 9, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 6, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2 } ) );
        countRelationships( 6, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 6, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 3, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST } ) );
        countRelationships( 3, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2 } ) );
        countRelationships( 3, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 3, node.getRelationships(
            MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void allGetRelationshipMethods2( Node node, Direction dir )
    {
        countRelationships( 3, node.getRelationships() );
        countRelationships( 3, node.getRelationships( dir ) );
        countRelationships( 3, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2 } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 1, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST } ) );
        countRelationships( 1, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2 } ) );
        countRelationships( 1, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 1, node.getRelationships(
            MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void allGetRelationshipMethods3( Node node, Direction dir )
    {
        countRelationships( 6, node.getRelationships() );
        countRelationships( 6, node.getRelationships( dir ) );
        countRelationships( 6, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 4, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST2 } ) );
        countRelationships( 4, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 4, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST2 } ) );
        countRelationships( 2, node.getRelationships( new RelationshipType[] {
            MyRelTypes.TEST_TRAVERSAL } ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 2, node.getRelationships(
            MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void countRelationships( int expectedCount,
        Iterable<Relationship> rels )
    {
        int count = 0;
        for ( Relationship r : rels )
        {
            count++;
        }
        assertEquals( expectedCount, count );
    }

    @Test
    public void testRelationshipCreateAndDelete()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship relationship = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        Relationship relArray1[] = getRelationshipArray( node1
            .getRelationships() );
        Relationship relArray2[] = getRelationshipArray( node2
            .getRelationships() );
        assertEquals( 1, relArray1.length );
        assertEquals( relationship, relArray1[0] );
        assertEquals( 1, relArray2.length );
        assertEquals( relationship, relArray2[0] );
        relArray1 = getRelationshipArray( node1
            .getRelationships( MyRelTypes.TEST ) );
        assertEquals( 1, relArray1.length );
        assertEquals( relationship, relArray1[0] );
        relArray2 = getRelationshipArray( node2
            .getRelationships( MyRelTypes.TEST ) );
        assertEquals( 1, relArray2.length );
        assertEquals( relationship, relArray2[0] );
        relArray1 = getRelationshipArray( node1.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 1, relArray1.length );
        relArray2 = getRelationshipArray( node2.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ) );
        assertEquals( 1, relArray2.length );
        relArray1 = getRelationshipArray( node1.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ) );
        assertEquals( 0, relArray1.length );
        relArray2 = getRelationshipArray( node2.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 0, relArray2.length );
        relationship.delete();
        node2.delete();
        node1.delete();
    }

    private Relationship[] getRelationshipArray(
        Iterable<Relationship> relsIterable )
    {
        ArrayList<Relationship> relList = new ArrayList<Relationship>();
        for ( Relationship rel : relsIterable )
        {
            relList.add( rel );
        }
        return relList.toArray( new Relationship[relList.size()] );
    }

    @Test
    public void testDeleteWithRelationship()
    {
        // do some evil stuff
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship relationship = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        node1.delete();
        node2.delete();
        try
        {
            getTransaction().success();
            getTransaction().finish();
            fail( "deleting node with relationship should not commit." );
        }
        catch ( Exception e )
        {
            // good
        }
        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    public void testDeletedRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship relationship = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        relationship.delete();
        try
        {
            relationship.setProperty( "key1", new Integer( 1 ) );
            fail( "Adding property to deleted rel should throw exception." );
        }
        catch ( Exception e )
        { // good
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testRelationshipAddProperty()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        Relationship rel2 = node2.createRelationshipTo( node1, MyRelTypes.TEST );
        try
        {
            rel1.setProperty( null, null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        // add property
        rel1.setProperty( key1, int1 );
        rel2.setProperty( key1, string1 );
        rel1.setProperty( key2, string2 );
        rel2.setProperty( key2, int2 );
        assertTrue( rel1.hasProperty( key1 ) );
        assertTrue( rel2.hasProperty( key1 ) );
        assertTrue( rel1.hasProperty( key2 ) );
        assertTrue( rel2.hasProperty( key2 ) );
        assertTrue( !rel1.hasProperty( key3 ) );
        assertTrue( !rel2.hasProperty( key3 ) );
        assertEquals( int1, rel1.getProperty( key1 ) );
        assertEquals( string1, rel2.getProperty( key1 ) );
        assertEquals( string2, rel1.getProperty( key2 ) );
        assertEquals( int2, rel2.getProperty( key2 ) );

        getTransaction().failure();
    }

    @Test
    public void testRelationshipRemoveProperty()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        Relationship rel2 = node2.createRelationshipTo( node1, MyRelTypes.TEST );
        // verify that we can rely on PL to reomve non existing properties
        try
        {
            if ( rel1.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property should return null" );
            }
        }
        catch ( NotFoundException e )
        {
        }
        try
        {
            rel1.removeProperty( null );
            fail( "Remove null property should throw exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }

        rel1.setProperty( key1, int1 );
        rel2.setProperty( key1, string1 );
        rel1.setProperty( key2, string2 );
        rel2.setProperty( key2, int2 );
        try
        {
            rel1.removeProperty( null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }

        // test remove property
        assertEquals( int1, rel1.removeProperty( key1 ) );
        assertEquals( string1, rel2.removeProperty( key1 ) );
        // test remove of non exsisting property
        try
        {
            if ( rel2.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property should return null" );
            }
        }
        catch ( NotFoundException e )
        {
            // have to set rollback only here
            getTransaction().failure();
        }
        rel1.delete();
        rel2.delete();
        node1.delete();
        node2.delete();
    }

    @Test
    public void testRelationshipChangeProperty()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        Relationship rel2 = node2.createRelationshipTo( node1, MyRelTypes.TEST );
        rel1.setProperty( key1, int1 );
        rel2.setProperty( key1, string1 );
        rel1.setProperty( key2, string2 );
        rel2.setProperty( key2, int2 );

        try
        {
            rel1.setProperty( null, null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        catch ( NotFoundException e )
        {
            fail( "wrong exception" );
        }

        // test type change of exsisting property
        // cannot test this for now because of exceptions in PL
        rel2.setProperty( key1, int1 );

        rel1.delete();
        rel2.delete();
        node2.delete();
        node1.delete();
    }

    @Test
    public void testRelationshipChangeProperty2()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );
        Boolean bool1 = new Boolean( true );
        Boolean bool2 = new Boolean( false );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel1.setProperty( key1, int1 );
        rel1.setProperty( key1, int2 );
        assertEquals( int2, rel1.getProperty( key1 ) );
        rel1.removeProperty( key1 );
        rel1.setProperty( key1, string1 );
        rel1.setProperty( key1, string2 );
        assertEquals( string2, rel1.getProperty( key1 ) );
        rel1.removeProperty( key1 );
        rel1.setProperty( key1, bool1 );
        rel1.setProperty( key1, bool2 );
        assertEquals( bool2, rel1.getProperty( key1 ) );
        rel1.removeProperty( key1 );

        rel1.delete();
        node2.delete();
        node1.delete();
    }

    @Test
    public void testRelGetProperties()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string = new String( "3" );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        try
        {
            rel1.getProperty( key1 );
            fail( "get non existing property din't throw exception" );
        }
        catch ( NotFoundException e )
        {
        }
        try
        {
            rel1.getProperty( null );
            fail( "get of null key din't throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        }
        assertTrue( !rel1.hasProperty( key1 ) );
        assertTrue( !rel1.hasProperty( null ) );
        rel1.setProperty( key1, int1 );
        rel1.setProperty( key2, int2 );
        rel1.setProperty( key3, string );
        assertTrue( rel1.hasProperty( key1 ) );
        assertTrue( rel1.hasProperty( key2 ) );
        assertTrue( rel1.hasProperty( key3 ) );
        try
        {
            rel1.removeProperty( key3 );
        }
        catch ( NotFoundException e )
        {
            fail( "Remove of property failed." );
        }
        assertTrue( !rel1.hasProperty( key3 ) );
        assertTrue( !rel1.hasProperty( null ) );
        rel1.delete();
        node2.delete();
        node1.delete();
    }

    @Test
    public void testDirectedRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel2 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        Relationship rel3 = node2.createRelationshipTo( node1, MyRelTypes.TEST );
        Node[] nodes = rel2.getNodes();
        assertEquals( 2, nodes.length );
        assertTrue( nodes[0].equals( node1 ) && nodes[1].equals( node2 ) );
        nodes = rel3.getNodes();
        assertEquals( 2, nodes.length );
        assertTrue( nodes[0].equals( node2 ) && nodes[1].equals( node1 ) );
        assertEquals( node1, rel2.getStartNode() );
        assertEquals( node2, rel2.getEndNode() );
        assertEquals( node2, rel3.getStartNode() );
        assertEquals( node1, rel3.getEndNode() );

        Relationship relArray[] = getRelationshipArray( node1.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel2, relArray[0] );
        relArray = getRelationshipArray( node1.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel3, relArray[0] );

        relArray = getRelationshipArray( node2.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel3, relArray[0] );
        relArray = getRelationshipArray( node2.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel2, relArray[0] );

        rel2.delete();
        rel3.delete();
        node1.delete();
        node2.delete();
    }

    @Test
    public void testRollbackDeleteRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        newTransaction();
        node1.delete();
        rel1.delete();
        getTransaction().failure();
        getTransaction().finish();
        setTransaction( getGraphDb().beginTx() );
        node1.delete();
        node2.delete();
        rel1.delete();
    }

    @Test
    public void testCreateRelationshipWithCommitts()// throws NotFoundException
    {
        Node n1 = getGraphDb().createNode();
        newTransaction();
        clearCache();
        n1 = getGraphDb().getNodeById( n1.getId() );
        Node n2 = getGraphDb().createNode();
        n1.createRelationshipTo( n2, MyRelTypes.TEST );
        newTransaction();
        Relationship[] relArray = getRelationshipArray( n1.getRelationships() );
        assertEquals( 1, relArray.length );
        relArray = getRelationshipArray( n1.getRelationships() );
        relArray[0].delete();
        n1.delete();
        n2.delete();
    }

    @Test
    public void testAddPropertyThenDelete()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", "test" );
        newTransaction();
        rel.setProperty( "test2", "test2" );
        rel.delete();
        node1.delete();
        node2.delete();
        newTransaction();
    }

    @Test
    public void testRelationshipIsType()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        assertTrue( rel.isType( MyRelTypes.TEST ) );
        assertTrue( rel.isType( new RelationshipType()
        {
            public String name()
            {
                return MyRelTypes.TEST.name();
            }
        } ) );
        assertFalse( rel.isType( MyRelTypes.TEST_TRAVERSAL ) );
        rel.delete();
        node1.delete();
        node2.delete();
    }

    @Test
    public void testChangeProperty()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", "test1" );
        newTransaction();
        rel.setProperty( "test", "test2" );
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test3" );
        assertEquals( "test3", rel.getProperty( "test" ) );
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test4" );
        newTransaction();
        assertEquals( "test4", rel.getProperty( "test" ) );
    }

    @Test
    public void testChangeProperty2()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", "test1" );
        newTransaction();
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test3" );
        assertEquals( "test3", rel.getProperty( "test" ) );
        newTransaction();
        assertEquals( "test3", rel.getProperty( "test" ) );
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test4" );
        newTransaction();
        assertEquals( "test4", rel.getProperty( "test" ) );
    }

    @Test
    public void makeSureLazyLoadingRelationshipsWorksEvenIfOtherIteratorAlsoLoadsInTheSameIteration() throws IOException
    {
        String path = "target/var/lazyloadrels";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseService graphDB = new EmbeddedGraphDatabase( path );
        int num_edges = 100;
        Node hub;

        /* create 256 nodes */
        Transaction tx = graphDB.beginTx();
        Node[] nodes = new Node[256];
        for ( int num_nodes = 0; num_nodes < nodes.length; num_nodes += 1 )
        {
            nodes[num_nodes] = graphDB.createNode();
        }
        tx.success();
        tx.finish();

        /* create random outgoing relationships from node 5 */
        hub = nodes[4];
        int nextID = 7;

        tx = graphDB.beginTx();
        for ( int k = 0; k < num_edges; k += 1 )
        {
            Node neighbor = graphDB.getNodeById( nextID );
            nextID += 7;
            nextID &= 255;
            if ( nextID == 0 )
            {
                nextID = 1;
            }
            hub.createRelationshipTo( neighbor, DynamicRelationshipType.withName( "outtie" ) );
        }
        tx.success();
        tx.finish();

        tx = graphDB.beginTx();
        /* create random incoming relationships to node 5 */
        for ( int k = 0; k < num_edges; k += 1 )
        {
            Node neighbor = graphDB.getNodeById( nextID );
            nextID += 7;
            nextID &= 255;
            if ( nextID == 0 )
            {
                nextID = 1;
            }
            neighbor.createRelationshipTo( hub, DynamicRelationshipType.withName( "innie" ) );
        }
        tx.success();
        tx.finish();

        graphDB.shutdown();
        graphDB = new EmbeddedGraphDatabase( path );
        hub = graphDB.getNodeById( hub.getId() );
        int count = 0;
        for ( @SuppressWarnings( "unused" )
        Relationship r1 : hub.getRelationships() )
        {
            for ( @SuppressWarnings( "unused" )
            Relationship r2 : hub.getRelationships() )
            {
                count += 1;
            }
        }
        assertEquals( 40000, count );

        count = 0;
        for ( @SuppressWarnings( "unused" )
        Relationship r1 : hub.getRelationships() )
        {
            for ( @SuppressWarnings( "unused" )
            Relationship r2 : hub.getRelationships() )
            {
                count += 1;
            }
        }
        assertEquals( 40000, count );
        graphDB.shutdown();
    }

    @Test
    public void deleteRelationshipFromNotFullyLoadedNode() throws Exception
    {
        int grabSize = 10;
        GraphDatabaseService db = new ImpermanentGraphDatabase(
                "target/test-data/test-db", stringMap( "relationship_grab_size", "" + grabSize ) );
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Node node3 = db.createNode();
        RelationshipType type1 = DynamicRelationshipType.withName( "type1" );
        RelationshipType type2 = DynamicRelationshipType.withName( "type2" );
        // This will the last relationship in the chain
        node1.createRelationshipTo( node3, type1 );
        Collection<Relationship> type2Relationships = new HashSet<Relationship>();
        // Create exactly grabSize relationships and store them in a set
        for ( int i = 0; i < grabSize; i++ )
        {
            type2Relationships.add( node1.createRelationshipTo( node2, type2 ) );
        }
        tx.success();
        tx.finish();

        ((AbstractGraphDatabase)db).getConfig().getGraphDbModule().getNodeManager().clearCache();

        /*
         * Here node1 has grabSize+1 relationships. The first grabSize to be loaded will be
         * the type2 ones to node2 and the one remaining will be the type1 to node3.
         */

        tx = db.beginTx();
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

        tx.success();
        tx.finish();
        assertEquals( type2Relationships, addToCollection( node1.getRelationships(), new HashSet<Relationship>() ) );
        db.shutdown();
    }

    @Test
    public void commitToNotFullyLoadedNode() throws Exception
    {
        int grabSize = 10;
        GraphDatabaseService db = new ImpermanentGraphDatabase(
                "target/test-data/test-db2", stringMap(
                        "relationship_grab_size", "" + grabSize ) );
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        RelationshipType type = DynamicRelationshipType.withName( "type" );
        for ( int i = 0; i < grabSize + 2; i++ )
        {
            node1.createRelationshipTo( node2, type );
        }
        tx.success();
        tx.finish();

        ( (AbstractGraphDatabase) db ).getConfig().getGraphDbModule().getNodeManager().clearCache();

        tx = db.beginTx();

        node1.getRelationships().iterator().next().delete();
        node1.setProperty( "foo", "bar" );
        int relCount = 0;
        for ( Relationship rel : node2.getRelationships() )
        {
            relCount++;
        }
        assertEquals( relCount, grabSize + 1 );
        relCount = 0;
        for (Relationship rel : node1.getRelationships())
        {
            relCount++;
        }
        assertEquals( relCount, grabSize + 1 );
        assertEquals( "bar", node1.getProperty( "foo" ) );
        tx.success();
        tx.finish();
        db.shutdown();
    }

    @Test
    public void createRelationshipAfterClearedCache()
    {
        // Assumes relationship grab size 100
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        int expectedCount = 0;
        for ( int i = 0; i < 150; i++ )
        {
            node1.createRelationshipTo( node2, TEST );
            expectedCount++;
        }
        newTransaction();
        getNodeManager().clearCache();
        for ( int i = 0; i < 50; i++ )
        {
            node1.createRelationshipTo( node2, TEST );
            expectedCount++;
        }
        assertEquals( expectedCount, count( node1.getRelationships() ) );
        newTransaction();
        assertEquals( expectedCount, count( node1.getRelationships() ) );
    }

    @Test
    public void grabSizeWithTwoTypesDeleteAndCount()
    {
        int grabSize = 2;
        GraphDatabaseService db = new ImpermanentGraphDatabase(
                "target/test-data/test-db4", stringMap(
                        "relationship_grab_size", "" + grabSize ) );
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();

        int count = 0;
        RelationshipType type1 = DynamicRelationshipType.withName( "type" );
        RelationshipType type2 = DynamicRelationshipType.withName( "bar" );
        // Create more than one grab size
        for ( int i = 0; i < 11; i++ )
        {
            node1.createRelationshipTo( node2, type1 );
            count++;
        }
        for ( int i = 0; i < 10; i++ )
        {
            node1.createRelationshipTo( node2, type2 );
            count++;
        }
        tx.success();
        tx.finish();

        clearCacheAndCreateDeleteCount( db, node1, node2, type1, type2, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type2, type1, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type1, type1, count );
        clearCacheAndCreateDeleteCount( db, node1, node2, type2, type2, count );
        db.shutdown();
    }

    private void clearCacheAndCreateDeleteCount( GraphDatabaseService db, Node node1, Node node2,
            RelationshipType createType, RelationshipType deleteType, int expectedCount )
    {
        Transaction tx = db.beginTx();
        ( (AbstractGraphDatabase) db ).getConfig().getGraphDbModule().getNodeManager().clearCache();

        node1.createRelationshipTo( node2, createType );
        Relationship rel1 = node1.getRelationships( deleteType ).iterator().next();
        rel1.delete();

        assertEquals( expectedCount, count( node1.getRelationships() ) );
        assertEquals( expectedCount, count( node2.getRelationships() ) );
        tx.success();
        tx.finish();
        assertEquals( expectedCount, count( node1.getRelationships() ) );
        assertEquals( expectedCount, count( node2.getRelationships() ) );
    }

    @Ignore( "Triggers a bug, enable this test when fixed, https://github.com/neo4j/community/issues/52" )
    @Test
    public void deleteRelsWithCommitInMiddle() throws Exception
    {
        Node node = getGraphDb().createNode();
        Node otherNode = getGraphDb().createNode();
        RelationshipType[] types = new RelationshipType[] {withName( "r1" ), withName( "r2" ), withName( "r3" ), withName( "r4" )}; 
        int count = 30; // 30*4 > 100 (rel grabSize)
        for ( int i = 0; i < types.length*count; i++ )
        {
            node.createRelationshipTo( otherNode, types[i%types.length] );
        }
        newTransaction();
        clearCache();
        int delCount = 0;
        int loopCount = 0;
        while ( delCount < count )
        {
            loopCount++;
            for ( Relationship rel : node.getRelationships( types[1] ) )
            {
                rel.delete();
                if ( ++delCount == count/2 ) newTransaction();
            }
        }
        assertEquals( 1, loopCount );
        assertEquals( count, delCount );
    }
}
