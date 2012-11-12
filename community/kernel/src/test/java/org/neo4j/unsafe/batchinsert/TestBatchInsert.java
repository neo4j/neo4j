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
package org.neo4j.unsafe.batchinsert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.util.StringLogger;

public class TestBatchInsert
{
    private static Map<String,Object> properties = new HashMap<String,Object>();

    private static enum RelTypes implements RelationshipType
    {
        BATCH_TEST,
        REL_TYPE1,
        REL_TYPE2,
        REL_TYPE3,
        REL_TYPE4,
        REL_TYPE5
    }

    private static RelationshipType[] relTypeArray = {
        RelTypes.REL_TYPE1, RelTypes.REL_TYPE2, RelTypes.REL_TYPE3,
        RelTypes.REL_TYPE4, RelTypes.REL_TYPE5 };

    static
    {
        properties.put( "key0", "SDSDASSDLKSDSAKLSLDAKSLKDLSDAKLDSLA" );
        properties.put( "key1", 1 );
        properties.put( "key2", (short) 2 );
        properties.put( "key3", 3L );
        properties.put( "key4", 4.0f );
        properties.put( "key5", 5.0d );
        properties.put( "key6", (byte) 6 );
        properties.put( "key7", true );
        properties.put( "key8", (char) 8 );
        properties.put( "key10", new String[] {
            "SDSDASSDLKSDSAKLSLDAKSLKDLSDAKLDSLA", "dsasda", "dssadsad"
        } );
        properties.put( "key11", new int[] {1,2,3,4,5,6,7,8,9 } );
        properties.put( "key12", new short[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key13", new long[] {1,2,3,4,5,6,7,8,9 } );
        properties.put( "key14", new float[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key15", new double[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key16", new byte[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key17", new boolean[] {true,false,true,false} );
        properties.put( "key18", new char[] {1,2,3,4,5,6,7,8,9} );
    }

    private BatchInserter newBatchInserter()
    {
        return newBatchInserter( true );
    }

    private GraphDatabaseService newBatchGraphDatabase()
    {
        return newBatchGraphDatabase( true );
    }

    private BatchInserter newBatchInserter( boolean eraseOld )
    {
        String storePath = AbstractNeo4jTestCase.getStorePath( "neo-batch" );
        if ( eraseOld )
        {
            AbstractNeo4jTestCase.deleteFileOrDirectory( new File( storePath ) );
        }
        return BatchInserters.inserter( storePath );
    }

    private GraphDatabaseService newBatchGraphDatabase( boolean eraseOld )
    {
        String storePath = AbstractNeo4jTestCase.getStorePath( "neo-batch-db" );
        if ( eraseOld )
        {
            AbstractNeo4jTestCase.deleteFileOrDirectory( new File( storePath ) );
        }
        return BatchInserters.batchDatabase( storePath );
    }

    @Test
    public void testSimple()
    {
        BatchInserter graphDb = newBatchInserter();
        long node1 = graphDb.createNode( null );
        long node2 = graphDb.createNode( null );
        long rel1 = graphDb.createRelationship( node1, node2, RelTypes.BATCH_TEST,
            null );
        BatchRelationship rel = graphDb.getRelationshipById( rel1 );
        assertEquals( rel.getStartNode(), node1 );
        assertEquals( rel.getEndNode(), node2 );
        assertEquals( RelTypes.BATCH_TEST.name(), rel.getType().name() );
        graphDb.shutdown();
    }

    @Test
    public void testPropertySetFromGraphDbIsPersisted()
    {
        GraphDatabaseService gds = newBatchGraphDatabase();

        Node from = gds.createNode();
        long fromId = from.getId();

        Node to = gds.createNode();
        long toId = to.getId();

        Relationship rel = from.createRelationshipTo( to,
                DynamicRelationshipType.withName( "PROP_TEST" ) );
        long relId = rel.getId();

        from.setProperty( "1", "one" );
        to.setProperty( "2", "two" );
        rel.setProperty( "3", "three" );

        gds.shutdown();

        GraphDatabaseService db = newBatchGraphDatabase( false /*delete old dir*/);
        from = db.getNodeById( fromId );
        assertEquals( "one", from.getProperty( "1" ) );
        to = db.getNodeById( toId );
        assertEquals( "two", to.getProperty( "2" ) );
        rel = db.getRelationshipById( relId );
        assertEquals( "three", rel.getProperty( "3" ) );
        db.shutdown();
    }

    @Test
    public void testSetAndAddNodeProperties()
    {
        BatchInserter inserter = newBatchInserter();

        long tehNode = inserter.createNode( MapUtil.map( "one", "one" ,"two","two","three","three") );
        inserter.setNodeProperty( tehNode, "four", "four" );
        inserter.setNodeProperty( tehNode, "five", "five" );
        Map<String, Object> props = inserter.getNodeProperties( tehNode );
        assertEquals( 5, props.size() );
        assertEquals( "one", props.get( "one" ) );
        assertEquals( "five", props.get( "five" ) );

        inserter.shutdown();
    }
    
    @Test
    public void setSingleProperty() throws Exception
    {
        BatchInserter inserter = newBatchInserter();
        long node = inserter.createNode( null );

        String value = "Something";
        String key = "name";
        inserter.setNodeProperty( node, key, value );
        
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter );
        assertEquals( value, db.getNodeById( node ).getProperty( key ) );
        db.shutdown();
    }

    private GraphDatabaseService switchToEmbeddedGraphDatabaseService( BatchInserter inserter )
    {
        inserter.shutdown();
        return new EmbeddedGraphDatabase( inserter.getStoreDir() );
    }

    @Test
    public void testSetAndKeepNodeProperty()
    {
        BatchInserter inserter = newBatchInserter();

        long tehNode = inserter.createNode( MapUtil.map( "foo", "bar" ) );
        inserter.setNodeProperty( tehNode, "foo2", "bar2" );
        Map<String, Object> props = inserter.getNodeProperties( tehNode );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter( false /*delete old dir*/);

        props = inserter.getNodeProperties( tehNode );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.setNodeProperty( tehNode, "foo", "bar3" );

        props = inserter.getNodeProperties( tehNode );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
        inserter = newBatchInserter( false /*delete old dir*/);

        props = inserter.getNodeProperties( tehNode );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
    }

    @Test
    public void testSetAndKeepRelationshipProperty()
    {
        BatchInserter inserter = newBatchInserter();

        long from = inserter.createNode( Collections.EMPTY_MAP );
        long to = inserter.createNode( Collections.EMPTY_MAP );
        long theRel = inserter.createRelationship( from, to,
                DynamicRelationshipType.withName( "TestingPropsHere" ),
                MapUtil.map( "foo", "bar" ) );
        inserter.setRelationshipProperty( theRel, "foo2", "bar2" );
        Map<String, Object> props = inserter.getRelationshipProperties( theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter( false /*delete old dir*/);

        props = inserter.getRelationshipProperties( theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.setRelationshipProperty( theRel, "foo", "bar3" );

        props = inserter.getRelationshipProperties( theRel );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
        inserter = newBatchInserter( false /*delete old dir*/);

        props = inserter.getRelationshipProperties( theRel );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
    }

    @Test
    public void testNodeHasProperty()
    {
        BatchInserter inserter = newBatchInserter();

        long theNode = inserter.createNode( properties );
        long anotherNode = inserter.createNode( Collections.EMPTY_MAP );
        long relationship = inserter.createRelationship( theNode, anotherNode,
                DynamicRelationshipType.withName( "foo" ), properties );
        for ( String key : properties.keySet() )
        {
            assertTrue( inserter.nodeHasProperty( theNode, key ) );
            assertFalse( inserter.nodeHasProperty( theNode, key + "-" ) );
            assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            assertFalse( inserter.relationshipHasProperty( relationship, key
                                                                         + "-" ) );
        }

        inserter.shutdown();
    }

    @Test
    public void testRemoveProperties()
    {
        BatchInserter inserter = newBatchInserter();

        long theNode = inserter.createNode( properties );
        long anotherNode = inserter.createNode( Collections.EMPTY_MAP );
        long relationship = inserter.createRelationship( theNode, anotherNode,
                DynamicRelationshipType.withName( "foo" ), properties );

        inserter.removeNodeProperty( theNode, "key0" );
        inserter.removeRelationshipProperty( relationship, "key1" );

        for ( String key : properties.keySet() )
        {
            if ( key.equals( "key0" ) )
            {
                assertFalse( inserter.nodeHasProperty( theNode, key ) );
                assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            }
            else if ( key.equals( "key1" ) )
            {
                assertTrue( inserter.nodeHasProperty( theNode, key ) );
                assertFalse( inserter.relationshipHasProperty( relationship,
                        key ) );
            }
            else
            {
                assertTrue( inserter.nodeHasProperty( theNode, key ) );
                assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            }
        }
        inserter.shutdown();
        inserter = newBatchInserter( false /*delete old dir*/);

        for ( String key : properties.keySet() )
        {
            if ( key.equals( "key0" ) )
            {
                assertFalse( inserter.nodeHasProperty( theNode, key ) );
                assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            }
            else if ( key.equals( "key1" ) )
            {
                assertTrue( inserter.nodeHasProperty( theNode, key ) );
                assertFalse( inserter.relationshipHasProperty( relationship,
                        key ) );
            }
            else
            {
                assertTrue( inserter.nodeHasProperty( theNode, key ) );
                assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            }
        }
        inserter.shutdown();
    }

    @Test
    public void testMore()
    {
        BatchInserter graphDb = newBatchInserter();
        long startNode = graphDb.createNode( properties );
        long endNodes[] = new long[25];
        Set<Long> rels = new HashSet<Long>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = graphDb.createNode( properties );
            rels.add( graphDb.createRelationship( startNode, endNodes[i],
                relTypeArray[i % 5], properties ) );
        }
        for ( BatchRelationship rel : graphDb.getRelationships( startNode ) )
        {
            assertTrue( rels.contains( rel.getId() ) );
            assertEquals( rel.getStartNode(), startNode );
        }
        graphDb.setNodeProperties( startNode, properties );
        graphDb.shutdown();
    }

    @Test
    public void testPropSetAndReset()
    {
        BatchInserter graphDb = newBatchInserter();
        BatchGraphDatabaseImpl gds = new BatchGraphDatabaseImpl( graphDb );
        long startNode = graphDb.createNode( properties );
        assertProperties( gds.getNodeById( startNode ) );
        graphDb.setNodeProperties( startNode, properties );
        assertProperties( gds.getNodeById( startNode ) );
        graphDb.setNodeProperties( startNode, properties );
        assertProperties( gds.getNodeById( startNode ) );
        gds.shutdown();
    }

    @Test
    public void makeSureLoopsCanBeCreated()
    {
        BatchInserter graphDb = newBatchInserter();
        long startNode = graphDb.createNode( properties );
        long otherNode = graphDb.createNode( properties );
        long selfRelationship = graphDb.createRelationship( startNode, startNode,
                relTypeArray[0], properties );
        long relationship = graphDb.createRelationship( startNode, otherNode,
                relTypeArray[0], properties );
        for ( BatchRelationship rel : graphDb.getRelationships( startNode ) )
        {
            if ( rel.getId() == selfRelationship )
            {
                assertEquals( startNode, rel.getStartNode() );
                assertEquals( startNode, rel.getEndNode() );
            }
            else if ( rel.getId() == relationship )
            {
                assertEquals( startNode, rel.getStartNode() );
                assertEquals( otherNode, rel.getEndNode() );
            }
            else
            {
                fail( "Unexpected relationship " + rel.getId() );
            }
        }
        String storeDir = ( (BatchInserterImpl) graphDb ).getStoreDir();
        graphDb.shutdown();

        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir );
        Node realStartNode = db.getNodeById( startNode );
        Relationship realSelfRelationship = db.getRelationshipById( selfRelationship );
        Relationship realRelationship = db.getRelationshipById( relationship );
        assertEquals( realSelfRelationship, realStartNode.getSingleRelationship( RelTypes.REL_TYPE1, Direction.INCOMING ) );
        assertEquals( asSet( realSelfRelationship, realRelationship ), asSet( realStartNode.getRelationships( Direction.OUTGOING ) ) );
        assertEquals( asSet( realSelfRelationship, realRelationship ), asSet( realStartNode.getRelationships() ) );
        db.shutdown();
    }

    private static <T> Set<T> asSet( T... items )
    {
        return new HashSet<T>( Arrays.asList( items ) );
    }

    private static <T> Set<T> asSet( Iterable<T> items )
    {
        return new HashSet<T>( IteratorUtil.asCollection( items ) );
    }

    private void setProperties( Node node )
    {
        for ( String key : properties.keySet() )
        {
            node.setProperty( key, properties.get( key ) );
        }
    }

    private void setProperties( Relationship rel )
    {
        for ( String key : properties.keySet() )
        {
            rel.setProperty( key, properties.get( key ) );
        }
    }

    private void assertProperties( Node node )
    {
        for ( String key : properties.keySet() )
        {
            if ( properties.get( key ).getClass().isArray() )
            {
                Class<?> component = properties.get( key ).getClass().getComponentType();
                if ( !component.isPrimitive() ) // then it is String, cast to
                                                // Object[] is safe
                {
                    assertTrue( Arrays.equals(
                            (Object[]) properties.get( key ),
                            (Object[]) node.getProperty( key ) ) );
                }
                else
                {
                    if ( component == Integer.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (int[]) properties.get( key ),
                                    (int[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Boolean.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (boolean[]) properties.get( key ),
                                    (boolean[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Byte.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (byte[]) properties.get( key ),
                                    (byte[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Character.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (char[]) properties.get( key ),
                                    (char[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Long.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (long[]) properties.get( key ),
                                    (long[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Float.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (float[]) properties.get( key ),
                                    (float[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Double.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (double[]) properties.get( key ),
                                    (double[]) node.getProperty( key ) ) );
                        }
                    }
                    else if ( component == Short.TYPE )
                    {
                        if ( component.isPrimitive() )
                        {
                            assertTrue( Arrays.equals(
                                    (short[]) properties.get( key ),
                                    (short[]) node.getProperty( key ) ) );
                        }
                    }
                }
            }
            else
            {
                assertEquals( properties.get( key ), node.getProperty( key ) );
            }
        }
        for ( String stored : node.getPropertyKeys() )
        {
            assertTrue( properties.containsKey( stored ) );
        }
    }

    @Test
    public void testWithGraphDbService()
    {
        GraphDatabaseService graphDb = newBatchGraphDatabase();
        Node startNode = graphDb.createNode();
        setProperties( startNode );
        Node endNodes[] = new Node[25];
        Set<Relationship> rels = new HashSet<Relationship>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = graphDb.createNode();
            setProperties( endNodes[i] );
            Relationship rel = startNode.createRelationshipTo( endNodes[i],
                relTypeArray[i % 5] );
            rels.add( rel );
            setProperties( rel );
        }
        for ( Relationship rel : startNode.getRelationships() )
        {
            assertTrue( rels.contains( rel ) );
            assertEquals( rel.getStartNode(), startNode );
        }
        setProperties( startNode );
        graphDb.shutdown();
    }

    @Test
    public void testGraphDbServiceGetRelationships()
    {
        GraphDatabaseService graphDb = newBatchGraphDatabase();
        Node startNode = graphDb.createNode();
        for ( int i = 0; i < 5; i++ )
        {
            Node endNode = graphDb.createNode();
            startNode.createRelationshipTo( endNode, relTypeArray[i] );
        }
        for ( int i = 0; i < 5; i++ )
        {
            assertTrue( startNode.getSingleRelationship(
                relTypeArray[i], Direction.OUTGOING ) != null );
        }
        for ( int i = 0; i < 5; i++ )
        {
            Iterator<Relationship> relItr =
                startNode.getRelationships( relTypeArray[i],
                    Direction.OUTGOING ).iterator();
            relItr.next();
            assertTrue( !relItr.hasNext() );
        }
        for ( int i = 0; i < 5; i++ )
        {
            Iterator<Relationship> relItr =
                startNode.getRelationships( relTypeArray[i] ).iterator();
            relItr.next();
            assertTrue( !relItr.hasNext() );
        }
        graphDb.shutdown();
    }

    @Test
    public void createBatchNodeAndRelationshipsDeleteAllInEmbedded() throws Exception
    {
        /*
         *    ()--[REL_TYPE1]-->(node)--[BATCH_TEST]->()
         */

        BatchInserter inserter = newBatchInserter();
        String storeDir = ( (BatchInserterImpl) inserter ).getStoreDir();
        long nodeId = inserter.createNode( null );
        inserter.createRelationship( nodeId, inserter.createNode( null ),
                RelTypes.BATCH_TEST, null );
        inserter.createRelationship( inserter.createNode( null ), nodeId,
                RelTypes.REL_TYPE1, null );
        inserter.shutdown();

        // Delete node and all its relationships
        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.getNodeById( nodeId );
        for ( Relationship relationship : node.getRelationships() )
        {
            relationship.delete();
        }
        node.delete();
        tx.success();
        tx.finish();
        db.shutdown();
    }

    @Test
    public void messagesLogGetsClosed() throws Exception
    {
        BatchInserter inserter = newBatchInserter();
        String storeDir = inserter.getStoreDir();
        inserter.shutdown();
        assertTrue( new File( storeDir, StringLogger.DEFAULT_NAME ).delete() );
    }

    @Test
    public void createEntitiesWithEmptyPropertiesMap() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        // Assert for node
        long nodeId = inserter.createNode( map() );
        inserter.getNodeProperties( nodeId );
        //cp=N U http://www.w3.org/1999/02/22-rdf-syntax-ns#type, c=N

        // Assert for relationship
        long anotherNodeId = inserter.createNode( null );
        long relId = inserter.createRelationship( nodeId, anotherNodeId, RelTypes.BATCH_TEST, map() );
        inserter.getRelationshipProperties( relId );
        inserter.shutdown();
    }

    @Test
    public void createEntitiesWithDynamicPropertiesMap() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        setAndGet( inserter, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" );
        setAndGet( inserter, intArray( 20 ) );

        inserter.shutdown();
    }

    private void setAndGet( BatchInserter inserter, Object value )
    {
        long nodeId = inserter.createNode( map( "key", value ) );
        Object readValue = inserter.getNodeProperties( nodeId ).get( "key" );
        if ( readValue.getClass().isArray() )
        {
            assertTrue( Arrays.equals( (int[])value, (int[])readValue ) );
        }
        else
        {
            assertEquals( value, readValue );
        }
    }

    private int[] intArray( int length )
    {
        int[] array = new int[length];
        for ( int i = 0, startValue = (int)Math.pow( 2, 30 ); i < length; i++ ) array[i] = startValue+i;
        return array;
    }
}
