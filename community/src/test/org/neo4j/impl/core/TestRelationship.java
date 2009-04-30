/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;

public class TestRelationship extends AbstractNeoTestCase
{
    private String key1 = "key1";
    private String key2 = "key2";
    private String key3 = "key3";

    public TestRelationship( String testName )
    {
        super( testName );
    }
    
    public void testSimple()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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
    
    public void testSimple2()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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
    
    public void testSimple3()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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
    
    public void testSimple4()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRelationshipCreateAndDelete()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testDeleteWithRelationship()
    {
        // do some evil stuff
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship relationship = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        node1.delete();
        node2.delete();
        Logger log = Logger
            .getLogger( "org.neo4j.impl.core.NeoConstraintsListener" );
        Level level = log.getLevel();
        log.setLevel( Level.OFF );
        try
        {
            getTransaction().success();
            getTransaction().finish();
            fail( "deleting node with relaitonship should not commit." );
        }
        catch ( Exception e )
        {
            // good
        }
        log.setLevel( level );
        setTransaction( getNeo().beginTx() );
    }

    public void testDeletedRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship relationship = node1.createRelationshipTo( node2,
            MyRelTypes.TEST );
        relationship.delete();
        Logger log = Logger
            .getLogger( "org.neo4j.impl.core.NeoConstraintsListener" );
        Level level = log.getLevel();
        log.setLevel( Level.OFF );
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
        log.setLevel( level );
    }

    public void testRelationshipAddProperty()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRelationshipRemoveProperty()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRelationshipChangeProperty()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRelationshipChangeProperty2()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );
        Boolean bool1 = new Boolean( true );
        Boolean bool2 = new Boolean( false );

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRelGetProperties()
    {
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string = new String( "3" );

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testDirectedRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testRollbackDeleteRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        newTransaction();
        node1.delete();
        rel1.delete();
        getTransaction().failure();
        getTransaction().finish();
        setTransaction( getNeo().beginTx() );
        node1.delete();
        node2.delete();
        rel1.delete();
    }

    private void clearCache()
    {
        NeoModule neoModule = ((EmbeddedNeo) getNeo()).getConfig()
            .getNeoModule();
        neoModule.getNodeManager().clearCache();
    }

    public void testCreateRelationshipWithCommitts()// throws NotFoundException
    {
        Node n1 = getNeo().createNode();
        newTransaction();
        clearCache();
        n1 = getNeo().getNodeById( (int) n1.getId() );
        Node n2 = getNeo().createNode();
        n1.createRelationshipTo( n2, MyRelTypes.TEST );
        newTransaction();
        Relationship[] relArray = getRelationshipArray( n1.getRelationships() );
        assertEquals( 1, relArray.length );
        relArray = getRelationshipArray( n1.getRelationships() );
        relArray[0].delete();
        n1.delete();
        n2.delete();
    }

    public void testAddPropertyThenDelete()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", "test" );
        newTransaction();
        rel.setProperty( "test2", "test2" );
        rel.delete();
        node1.delete();
        node2.delete();
        newTransaction();
    }

    public void testRelationshipIsType()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testChangeProperty()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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
    
    public void testChangeProperty2()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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
}