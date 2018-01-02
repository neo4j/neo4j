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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.tooling.GlobalGraphOperations.at;

public class TestRelationship extends AbstractNeo4jTestCase
{
    private final String key1 = "key1";
    private final String key2 = "key2";
    private final String key3 = "key3";

    @Test
    public void testSimple()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.createRelationshipTo( node2,  MyRelTypes.TEST );
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
        node1.getRelationships( MyRelTypes.TEST, Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST_TRAVERSAL, Direction.OUTGOING ).iterator().next().delete();
        node1.getRelationships( MyRelTypes.TEST2, Direction.OUTGOING ).iterator().next().delete();
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
        countRelationships( 9, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 6, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2 ) );
        countRelationships( 6, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 6, node.getRelationships( MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST2 ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void allGetRelationshipMethods2( Node node, Direction dir )
    {
        countRelationships( 3, node.getRelationships() );
        countRelationships( 3, node.getRelationships( dir ) );
        countRelationships( 3, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2 ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST2 ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 1, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 1, node.getRelationships(
            MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void allGetRelationshipMethods3( Node node, Direction dir )
    {
        countRelationships( 6, node.getRelationships() );
        countRelationships( 6, node.getRelationships( dir ) );
        countRelationships( 6, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 4, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST2 ) );
        countRelationships( 4, node.getRelationships( MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 4, node.getRelationships( MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST2 ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST_TRAVERSAL ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST, dir ) );
        countRelationships( 2, node.getRelationships( MyRelTypes.TEST2, dir ) );
        countRelationships( 2, node.getRelationships(
            MyRelTypes.TEST_TRAVERSAL, dir ) );
    }

    private void countRelationships( int expectedCount, Iterable<Relationship> rels )
    {
        int count = 0;
        for ( @SuppressWarnings( "unused" ) Relationship ignored : rels )
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
        ArrayList<Relationship> relList = new ArrayList<>();
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
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.delete();
        node2.delete();
        try
        {
            getTransaction().success();
            getTransaction().close();
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
        {   // OK
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
        {   // OK
        }
        try
        {
            rel1.removeProperty( null );
            fail( "Remove null property should throw exception." );
        }
        catch ( IllegalArgumentException e )
        {   // OK
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
        {   // OK
        }

        // test remove property
        assertEquals( int1, rel1.removeProperty( key1 ) );
        assertEquals( string1, rel2.removeProperty( key1 ) );
        // test remove of non existing property
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
        {   // OK
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
        {   // OK
        }
        try
        {
            rel1.getProperty( null );
            fail( "get of null key din't throw exception" );
        }
        catch ( IllegalArgumentException e )
        {   // OK
        }
        assertTrue( !rel1.hasProperty( key1 ) );
        assertTrue( !rel1.hasProperty( null ) );
        rel1.setProperty( key1, int1 );
        rel1.setProperty( key2, int2 );
        rel1.setProperty( key3, string );
        assertTrue( rel1.hasProperty( key1 ) );
        assertTrue( rel1.hasProperty( key2 ) );
        assertTrue( rel1.hasProperty( key3 ) );

        Map<String, Object> properties = rel1.getAllProperties();
        assertTrue( properties.get( key1 ).equals( int1 ) );
        assertTrue( properties.get( key2 ).equals( int2 ) );
        assertTrue( properties.get( key3 ).equals( string ) );
        properties = rel1.getProperties( key1, key2 );
        assertTrue( properties.get( key1 ).equals( int1 ) );
        assertTrue( properties.get( key2 ).equals( int2 ) );
        assertFalse( properties.containsKey( key3 ) );


        properties = node1.getProperties();
        assertTrue( properties.isEmpty() );

        try
        {
            String[] names = null;
            node1.getProperties( names );
            fail();
        }
        catch ( NullPointerException e )
        {
            // Ok
        }

        try
        {
            String[] names = new String[]{null};
            node1.getProperties( names );
            fail();
        }
        catch ( NullPointerException e )
        {
            // Ok
        }

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
        getTransaction().close();
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
            @Override
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
        // Create relationship with "test"="test1"
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", "test1" );
        newTransaction(); // commit

        // Remove "test" and set "test"="test3" instead
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test3" );
        assertEquals( "test3", rel.getProperty( "test" ) );
        newTransaction(); // commit

        // Remove "test" and set "test"="test4" instead
        assertEquals( "test3", rel.getProperty( "test" ) );
        rel.removeProperty( "test" );
        rel.setProperty( "test", "test4" );
        newTransaction(); // commit

        // Should still be "test4"
        assertEquals( "test4", rel.getProperty( "test" ) );
    }

    @Test
    public void makeSureLazyLoadingRelationshipsWorksEvenIfOtherIteratorAlsoLoadsInTheSameIteration()
    {
        int numEdges = 100;

        /* create 256 nodes */
        GraphDatabaseService graphDB = getGraphDb();
        Node[] nodes = new Node[256];
        for ( int num_nodes = 0; num_nodes < nodes.length; num_nodes += 1 )
        {
            nodes[num_nodes] = graphDB.createNode();
        }
        newTransaction();

        /* create random outgoing relationships from node 5 */
        Node hub = nodes[4];
        int nextID = 7;

        RelationshipType outtie = withName( "outtie" );
        RelationshipType innie = withName( "innie" );
        for ( int k = 0; k < numEdges; k++ )
        {
            Node neighbor = nodes[nextID];
            nextID += 7;
            nextID &= 255;
            if ( nextID == 0 )
            {
                nextID = 1;
            }
            hub.createRelationshipTo( neighbor, outtie );
        }
        newTransaction();

        /* create random incoming relationships to node 5 */
        for ( int k = 0; k < numEdges; k += 1 )
        {
            Node neighbor = nodes[nextID];
            nextID += 7;
            nextID &= 255;
            if ( nextID == 0 )
            {
                nextID = 1;
            }
            neighbor.createRelationshipTo( hub, innie );
        }
        commit();

        newTransaction();
        hub = graphDB.getNodeById( hub.getId() );

        int count = 0;
        for ( @SuppressWarnings( "unused" ) Relationship r1 : hub.getRelationships() )
        {
            count += count( hub.getRelationships() );
        }
        assertEquals( 40000, count );

        count = 0;
        for ( @SuppressWarnings( "unused" )
        Relationship r1 : hub.getRelationships() )
        {
            count += count( hub.getRelationships() );
        }
        assertEquals( 40000, count );
        commit();
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
        int delCount = 0;
        int loopCount = 0;
        while ( delCount < count )
        {
            loopCount++;
            for ( Relationship rel : node.getRelationships( types[1] ) )
            {
                rel.delete();
                if ( ++delCount == count/2 )
                {
                    newTransaction();
                }
            }
        }
        assertEquals( 1, loopCount );
        assertEquals( count, delCount );
    }

    @Test
    public void getAllRelationships() throws Exception
    {
        Set<Relationship> existingRelationships = addToCollection( at( getGraphDb() ).getAllRelationships(), new HashSet<Relationship>() );

        Set<Relationship> createdRelationships = new HashSet<>();
        Node node = getGraphDb().createNode();
        for ( int i = 0; i < 100; i++ )
        {
            createdRelationships.add( node.createRelationshipTo( getGraphDb().createNode(), MyRelTypes.TEST ) );
        }
        newTransaction();

        Set<Relationship> allRelationships = new HashSet<>();
        allRelationships.addAll( existingRelationships );
        allRelationships.addAll( createdRelationships );

        int count = 0;
        for ( Relationship rel : at( getGraphDb() ).getAllRelationships() )
        {
            assertTrue( "Unexpected rel " + rel + ", expected one of " + allRelationships, allRelationships.contains( rel ) );
            count++;
        }
        assertEquals( allRelationships.size(), count );
    }

    @Test
    public void createAndClearCacheBeforeCommit()
    {
        Node node = getGraphDb().createNode();
        node.createRelationshipTo( getGraphDb().createNode(), TEST );
        assertEquals( 1, IteratorUtil.count( node.getRelationships() ) );
    }

    @Test
    public void setPropertyAndClearCacheBeforeCommit() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "name", "Test" );
        assertEquals( "Test", node.getProperty( "name" ) );
    }

    @Test
    public void shouldNotGetTheSameRelationshipMoreThanOnceWhenAskingForTheSameTypeMultipleTimes() throws Exception
    {
        // given
        Node node = getGraphDb().createNode();
        node.createRelationshipTo( getGraphDb().createNode(), withName( "FOO" ) );

        // when
        int relationships = count( node.getRelationships( withName( "FOO" ), withName( "FOO" ) ) );

        // then
        assertEquals( 1, relationships );
    }

    @Test
    public void shouldLoadAllRelationships() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = getGraphDbAPI();
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < 112; i++ )
            {
                node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
                db.createNode().createRelationshipTo( node, MyRelTypes.TEST );
            }
            tx.success();
        }
        // WHEN
        int one, two;
        try ( Transaction tx = db.beginTx() )
        {
            one = count( node.getRelationships( MyRelTypes.TEST, Direction.OUTGOING ) );
            two = count( node.getRelationships( MyRelTypes.TEST, Direction.OUTGOING ) );
            tx.success();
        }

        // THEN
        assertEquals( two, one );
    }

    @Test( expected = NotFoundException.class )
    public void deletionOfSameRelationshipTwiceInOneTransactionShouldNotRollbackIt()
    {
        // Given
        GraphDatabaseService db = getGraphDb();

        // transaction is opened by test
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Relationship relationship = node1.createRelationshipTo( node2, TEST );
        commit();

        // When
        Exception exceptionThrownBySecondDelete = null;

        try ( Transaction tx = db.beginTx() )
        {
            relationship.delete();
            try
            {
                relationship.delete();
            }
            catch ( IllegalStateException e )
            {
                exceptionThrownBySecondDelete = e;
            }
            tx.success();
        }

        // Then
        assertNotNull( exceptionThrownBySecondDelete );

        try ( Transaction tx = db.beginTx() )
        {
            db.getRelationshipById( relationship.getId() ); // should throw NotFoundException
            tx.success();
        }
    }

    @Test(expected = NotFoundException.class)
    public void deletionOfAlreadyDeletedRelationshipShouldThrow()
    {
        // Given
        GraphDatabaseService db = getGraphDb();

        // transaction is opened by test
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Relationship relationship = node1.createRelationshipTo( node2, TEST );
        commit();

        try ( Transaction tx = db.beginTx() )
        {
            relationship.delete();
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            relationship.delete();
            tx.success();
        }
    }
}
