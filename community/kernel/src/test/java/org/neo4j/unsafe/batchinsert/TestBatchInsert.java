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
package org.neo4j.unsafe.batchinsert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStoreExtension;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

public class TestBatchInsert
{
    private static Map<String,Object> properties = new HashMap<>();

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

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private BatchInserter newBatchInserter()
    {
        return BatchInserters.inserter( "neo-batch-db", fs.get(), stringMap() );
    }

    private BatchInserter newBatchInserterWithSchemaIndexProvider( KernelExtensionFactory<?> provider )
    {
        List<KernelExtensionFactory<?>> extensions = Arrays.asList(
                provider, new InMemoryLabelScanStoreExtension() );
        return BatchInserters.inserter( "neo-batch-db", fs.get(), stringMap(), extensions );
    }

    private BatchInserter newBatchInserterWithLabelScanStore( KernelExtensionFactory<?> provider )
    {
        List<KernelExtensionFactory<?>> extensions = Arrays.asList(
                new InMemoryIndexProviderFactory(), provider );
        return BatchInserters.inserter( "neo-batch-db", fs.get(), stringMap(), extensions );
    }

    private GraphDatabaseService newBatchGraphDatabase()
    {
        return BatchInserters.batchDatabase( "neo-batch-db", fs.get() );
    }

    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1()
    {
        // Given
        BatchInserter batchInserter = newBatchInserter();

        String[] array1 = { "1" };
        String[] array2 = { "a" };

        long id1 = batchInserter.createNode(map("array", array1));
        long id2 = batchInserter.createNode(map());

        // When
        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array2 );

        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array2 );

        // Then
        assertThat( (String[])batchInserter.getNodeProperties( id1 ).get( "array" ), equalTo(array1) );

        batchInserter.shutdown();

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

        GraphDatabaseService db = newBatchGraphDatabase();
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
        assertThat( getNodeInTx( node, db ), inTx( db, hasProperty( key ).withValue( value )  ) );
        db.shutdown();
    }

    private GraphDatabaseService switchToEmbeddedGraphDatabaseService( BatchInserter inserter )
    {
        inserter.shutdown();
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        return factory.newImpermanentDatabase( inserter.getStoreDir() );
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

        inserter = newBatchInserter();

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
        inserter = newBatchInserter();

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

        long from = inserter.createNode( Collections.<String,Object>emptyMap() );
        long to = inserter.createNode( Collections.<String,Object>emptyMap() );
        long theRel = inserter.createRelationship( from, to,
                DynamicRelationshipType.withName( "TestingPropsHere" ),
                MapUtil.map( "foo", "bar" ) );
        inserter.setRelationshipProperty( theRel, "foo2", "bar2" );
        Map<String, Object> props = inserter.getRelationshipProperties( theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter();

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
        inserter = newBatchInserter();

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
        long anotherNode = inserter.createNode( Collections.<String,Object>emptyMap() );
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
        long anotherNode = inserter.createNode( Collections.<String,Object>emptyMap() );
        long relationship = inserter.createRelationship( theNode, anotherNode,
                DynamicRelationshipType.withName( "foo" ), properties );

        inserter.removeNodeProperty( theNode, "key0" );
        inserter.removeRelationshipProperty( relationship, "key1" );

        for ( String key : properties.keySet() )
        {
            switch ( key )
            {
                case "key0":
                    assertFalse( inserter.nodeHasProperty( theNode, key ) );
                    assertTrue( inserter.relationshipHasProperty( relationship, key ) );
                    break;
                case "key1":
                    assertTrue( inserter.nodeHasProperty( theNode, key ) );
                    assertFalse( inserter.relationshipHasProperty( relationship,
                            key ) );
                    break;
                default:
                    assertTrue( inserter.nodeHasProperty( theNode, key ) );
                    assertTrue( inserter.relationshipHasProperty( relationship, key ) );
                    break;
            }
        }
        inserter.shutdown();
        inserter = newBatchInserter();

        for ( String key : properties.keySet() )
        {
            switch ( key )
            {
                case "key0":
                    assertFalse( inserter.nodeHasProperty( theNode, key ) );
                    assertTrue( inserter.relationshipHasProperty( relationship, key ) );
                    break;
                case "key1":
                    assertTrue( inserter.nodeHasProperty( theNode, key ) );
                    assertFalse( inserter.relationshipHasProperty( relationship,
                            key ) );
                    break;
                default:
                    assertTrue( inserter.nodeHasProperty( theNode, key ) );
                    assertTrue( inserter.relationshipHasProperty( relationship, key ) );
                    break;
            }
        }
        inserter.shutdown();
    }

    @Test
    public void shouldBeAbleToRemoveDynamicProperty()
    {
        // Only triggered if assertions are enabled

        // GIVEN
        BatchInserter batchInserter = newBatchInserter();
        String key = "tags";
        long nodeId = batchInserter.createNode( MapUtil.map( key, new String[] { "one", "two", "three" } ) );

        // WHEN
        batchInserter.removeNodeProperty( nodeId, key );

        // THEN
        assertFalse( batchInserter.getNodeProperties( nodeId ).containsKey( key ) );
    }

    @Test
    public void shouldBeAbleToOverwriteDynamicProperty()
    {
        // Only triggered if assertions are enabled

        // GIVEN
        BatchInserter batchInserter = newBatchInserter();
        String key = "tags";
        long nodeId = batchInserter.createNode( MapUtil.map( key, new String[] { "one", "two", "three" } ) );

        // WHEN
        String[] secondValue = new String[] { "four", "five", "six" };
        batchInserter.setNodeProperty( nodeId, key, secondValue );

        // THEN
        assertTrue( Arrays.equals( secondValue, (String[]) batchInserter.getNodeProperties( nodeId ).get( key ) ) );
    }

    @Test
    public void testMore()
    {
        BatchInserter graphDb = newBatchInserter();
        long startNode = graphDb.createNode( properties );
        long endNodes[] = new long[25];
        Set<Long> rels = new HashSet<>();
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

        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( graphDb );

        try ( Transaction ignored = db.beginTx() )
        {
            Node realStartNode = db.getNodeById( startNode );
            Relationship realSelfRelationship = db.getRelationshipById( selfRelationship );
            Relationship realRelationship = db.getRelationshipById( relationship );
            assertEquals( realSelfRelationship, realStartNode.getSingleRelationship( RelTypes.REL_TYPE1, Direction.INCOMING ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ), asSet( realStartNode.getRelationships( Direction.OUTGOING ) ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ), asSet( realStartNode.getRelationships() ) );
        }
        finally {
            db.shutdown();
        }
    }

    private Node getNodeInTx( long nodeId, GraphDatabaseService db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return db.getNodeById( nodeId );
        }
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
        Set<Relationship> rels = new HashSet<>();
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
        long nodeId = inserter.createNode( null );
        inserter.createRelationship( nodeId, inserter.createNode( null ),
                RelTypes.BATCH_TEST, null );
        inserter.createRelationship( inserter.createNode( null ), nodeId,
                RelTypes.REL_TYPE1, null );

        // Delete node and all its relationships
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void batchDbShouldBeAbleToSetPropertyOnNodeWithNoProperties()
    {
        // GIVEN
        GraphDatabaseService database = newBatchGraphDatabase();
        Node node = database.createNode();
        database.shutdown();
        database = newBatchGraphDatabase();
        node = database.getNodeById( node.getId() );

        // WHEN
        node.setProperty( "test", "test" );

        // THEN
        assertEquals( "test", node.getProperty( "test" ) );
    }

    @Test
    public void batchDbShouldBeAbleToSetPropertyOnRelationshipWithNoProperties()
    {
        // GIVEN
        GraphDatabaseService database = newBatchGraphDatabase();
        Relationship relationship = database.createNode().createRelationshipTo(
                database.createNode(), MyRelTypes.TEST );
        database.shutdown();
        database = newBatchGraphDatabase();
        relationship = database.getRelationshipById( relationship.getId() );

        // WHEN
        relationship.setProperty( "test", "test" );

        // THEN
        assertEquals( "test", relationship.getProperty( "test" ) );
    }

    @Test
    public void messagesLogGetsClosed() throws Exception
    {
        String storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
        BatchInserter inserter = BatchInserters.inserter( storeDir, new DefaultFileSystemAbstraction(),
                stringMap() );
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

    @Test
    public void shouldAddInitialLabelsToCreatedNode() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        long node = inserter.createNode( map(), Labels.FIRST, Labels.SECOND );

        // THEN
        assertTrue( inserter.nodeHasLabel( node, Labels.FIRST ) );
        assertTrue( inserter.nodeHasLabel( node, Labels.SECOND ) );
        assertFalse( inserter.nodeHasLabel( node, Labels.THIRD ) );
    }

    @Test
    public void shouldGetNodeLabels() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        long node = inserter.createNode( map(), Labels.FIRST, Labels.THIRD );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( asSet( Labels.FIRST.name(), Labels.THIRD.name() ), asSet( labelNames ) );
    }

    @Test
    public void shouldAddManyInitialLabelsAsDynamicRecords() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        Pair<Label[], Set<String>> labels = manyLabels( 200 );
        long node = inserter.createNode( map(), labels.first() );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( labels.other(), asSet( labelNames ) );
    }

    @Test
    public void shouldReplaceExistingInlinedLabelsWithDynamic() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        long node = inserter.createNode( map(), Labels.FIRST );

        // WHEN
        Pair<Label[], Set<String>> labels = manyLabels( 100 );
        inserter.setNodeLabels( node, labels.first() );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( labels.other(), asSet( labelNames ) );
    }

    @Test
    public void shouldReplaceExistingDynamicLabelsWithInlined() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        long node = inserter.createNode( map(), manyLabels( 150 ).first() );

        // WHEN
        inserter.setNodeLabels( node, Labels.FIRST );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( asSet( Labels.FIRST.name() ), asSet( labelNames ) );
    }

    @Test
    public void shouldCreateDeferredSchemaIndexesInEmptyDatabase() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        IndexDefinition definition = inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        // THEN
        assertEquals( "Hacker", definition.getLabel().name() );
        assertEquals( asCollection( iterator( "handle" ) ), asCollection( definition.getPropertyKeys() ) );
    }

    @Test
    public void shouldCreateDeferredUniquenessConstraintInEmptyDatabase() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        ConstraintDefinition definition =
                inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        // THEN
        assertEquals( "Hacker", definition.getLabel().name() );
        assertEquals( ConstraintType.UNIQUENESS, definition.getConstraintType() );
        assertEquals( asSet( "handle" ), asSet( definition.getPropertyKeys() ) );
    }

    @Test
    public void shouldRunIndexPopulationJobAtShutdown() throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label("Hacker") ).on( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( populator ).create();
        verify( populator ).add( nodeId, "Jakewins" );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldRunConstraintPopulationJobAtShutdown() throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredConstraint( label("Hacker") ).assertPropertyIsUnique( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( populator ).create();
        verify( populator ).add( nodeId, "Jakewins" );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldRepopulatePreexistingIndexed() throws Throwable
    {
        // GIVEN
        long jakewins = dbWithIndexAndSingleIndexedNode();

        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        long boggle = inserter.createNode( map( "handle", "b0ggl3" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( populator ).create();
        verify( populator ).add( jakewins, "Jakewins" );
        verify( populator ).add( boggle, "b0ggl3" );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
        verifyNoMoreInteractions( populator );
    }

    private long dbWithIndexAndSingleIndexedNode()
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label("Hacker") ).on( "handle" ).create();
        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );
        inserter.shutdown();
        return nodeId;
    }

    @Test
    public void shouldCorrectlyJudgeRelationshipType()
    {
        // GIVEN
        GraphDatabaseService database = newBatchGraphDatabase();
        DynamicRelationshipType type = DynamicRelationshipType.withName( "TEST" );
        long relationshipId = database.createNode().createRelationshipTo( database.createNode(), type ).getId();

        // WHEN restarting (guaranteeing new RelationshipType instances internally)
        database.shutdown();
        database = newBatchGraphDatabase();
        Relationship relationship = database.getRelationshipById( relationshipId );

        // THEN
        assertTrue( "Relationship#isType returned false for the correct type", relationship.isType( type ) );
    }

    @Test
    public void shouldPopulateLabelScanStoreOnShutdown() throws Exception
    {
        // GIVEN
        // -- a database and a mocked label scan store
        UpdateTrackingLabelScanStore labelScanStore = new UpdateTrackingLabelScanStore();
        BatchInserter inserter = newBatchInserterWithLabelScanStore( new ControlledLabelScanStore( labelScanStore ) );

        // -- and some data that we insert
        long node1 = inserter.createNode( null, Labels.FIRST );
        long node2 = inserter.createNode( null, Labels.SECOND );
        long node3 = inserter.createNode( null, Labels.THIRD );
        long node4 = inserter.createNode( null, Labels.FIRST, Labels.SECOND );
        long node5 = inserter.createNode( null, Labels.FIRST, Labels.THIRD );

        // WHEN we shut down the batch inserter
        inserter.shutdown();

        // THEN the label scan store should receive all the updates.
        // of course, we don't know the label ids at this point, but we're assuming 0..2 (bad boy)
        labelScanStore.assertRecivedUpdate( node1, 0 );
        labelScanStore.assertRecivedUpdate( node2, 1 );
        labelScanStore.assertRecivedUpdate( node3, 2 );
        labelScanStore.assertRecivedUpdate( node4, 0, 1 );
        labelScanStore.assertRecivedUpdate( node5, 0, 2 );
    }
    
    @Test
    public void shouldSkipStoreScanIfNoLabelsAdded() throws Exception
    {
        // GIVEN
        UpdateTrackingLabelScanStore labelScanStore = new UpdateTrackingLabelScanStore();
        BatchInserter inserter = newBatchInserterWithLabelScanStore( new ControlledLabelScanStore( labelScanStore ) );
        
        // WHEN
        inserter.createNode( null );
        inserter.createNode( null );
        inserter.shutdown();
        
        // THEN
        assertEquals( 0, labelScanStore.writersCreated );
    }
    
    @Test
    public void propertiesCanBeReSetUsingBatchInserter()
    {
        // GIVEN
        BatchInserter batchInserter = newBatchInserter();
        Map<String, Object> props = new HashMap<>();
        props.put( "name", "One" );
        props.put( "count", 1 );
        props.put( "tags", new String[] { "one", "two" } );
        props.put( "something", "something" );
        batchInserter.createNode( 1, props );
        batchInserter.setNodeProperty( 1, "name", "NewOne" );
        batchInserter.removeNodeProperty( 1, "count" );
        batchInserter.removeNodeProperty( 1, "something" );
        
        // WHEN setting new properties
        batchInserter.setNodeProperty( 1, "name", "YetAnotherOne" );
        batchInserter.setNodeProperty( 1, "additional", "something" );
        
        // THEN there should be no problems doing so
        assertEquals( "YetAnotherOne", batchInserter.getNodeProperties( 1 ).get( "name" ) );
        assertEquals( "something", batchInserter.getNodeProperties( 1 ).get( "additional" ) );
        
        batchInserter.shutdown();
    }

    @Test
    public void propertiesCanBeReSetUsingBatchInserter2()
    {
        // GIVEN
        BatchInserter batchInserter = newBatchInserter();
        long id = batchInserter.createNode( new HashMap<String, Object>() );
        
        // WHEN
        batchInserter.setNodeProperty( id, "test", "looooooooooong test" );
        batchInserter.setNodeProperty( id, "test", "small test" );
        
        // THEN
        assertEquals( "small test", batchInserter.getNodeProperties( id ).get( "test" ) );
        
        batchInserter.shutdown();
    }

    @Test
    public void replaceWithBiggerPropertySpillsOverIntoNewPropertyRecord()
    {
        // GIVEN
        BatchInserter batchInserter = newBatchInserter();
        Map<String, Object> props = new HashMap<>();
        props.put( "name", "One" );
        props.put( "count", 1 );
        props.put( "tags", new String[] { "one", "two" } );
        long id = batchInserter.createNode( props );
        batchInserter.setNodeProperty( id, "name", "NewOne" );
        
        // WHEN
        batchInserter.setNodeProperty( id, "count", "something" );
        
        // THEN
        assertEquals( "something", batchInserter.getNodeProperties( id ).get( "count" ) );
        batchInserter.shutdown();
    }

    private static class UpdateTrackingLabelScanStore implements LabelScanStore
    {
        private final List<NodeLabelUpdate> allUpdates = new ArrayList<>();
        int writersCreated;

        public void assertRecivedUpdate( long node, long... labels )
        {
            for ( NodeLabelUpdate update : allUpdates )
            {
                if ( update.getNodeId() == node &&
                        Arrays.equals( update.getLabelsAfter(), labels ) )
                {
                    return;
                }
            }

            fail( "No update matching [nodeId:" + node + ", labels:" + Arrays.toString( labels ) + " found among: " +
                    allUpdates );
        }

        @Override
        public void recover( Iterator<NodeLabelUpdate> updates ) throws IOException
        {
        }

        @Override
        public void force() throws UnderlyingStorageException
        {
        }

        @Override
        public LabelScanReader newReader()
        {
            return null;
        }

        @Override
        public AllEntriesLabelScanReader newAllEntriesReader()
        {
            return null;
        }

        @Override
        public ResourceIterator<File> snapshotStoreFiles() throws IOException
        {
            return null;
        }

        @Override
        public void init() throws IOException
        {
        }

        @Override
        public void start() throws IOException
        {
        }

        @Override
        public void stop() throws IOException
        {
        }

        @Override
        public void shutdown() throws IOException
        {
        }

        @Override public LabelScanWriter newWriter()
        {
            writersCreated++;
            return new LabelScanWriter()
            {
                @Override
                public void write( NodeLabelUpdate update ) throws IOException
                {
                    addToCollection( Collections.singletonList( update ).iterator(), allUpdates );
                }

                @Override
                public void close() throws IOException
                {
                }
            };
        }
    }

    private static class ControlledLabelScanStore extends KernelExtensionFactory<InMemoryLabelScanStoreExtension.NoDependencies>
    {
        private final LabelScanStore labelScanStore;

        public ControlledLabelScanStore( LabelScanStore labelScanStore )
        {
            super( "batch" );
            this.labelScanStore = labelScanStore;
        }

        @Override
        public Lifecycle newKernelExtension( InMemoryLabelScanStoreExtension.NoDependencies dependencies ) throws Throwable
        {
            return new LabelScanStoreProvider( labelScanStore, 100 );
        }
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
        for ( int i = 0, startValue = (int)Math.pow( 2, 30 ); i < length; i++ )
        {
            array[i] = startValue+i;
        }
        return array;
    }

    private static enum Labels implements Label
    {
        FIRST,
        SECOND,
        THIRD
    }

    private Iterable<String> asNames( Iterable<Label> nodeLabels )
    {
        return map( new Function<Label,String>()
        {
            @Override
            public String apply( Label from )
            {
                return from.name();
            }
        }, nodeLabels );
    }

    private Pair<Label[],Set<String>> manyLabels( int count )
    {
        Label[] labels = new Label[count];
        Set<String> expectedLabelNames = new HashSet<>();
        for ( int i = 0; i < labels.length; i++ )
        {
            String labelName = "bach label " + i;
            labels[i] = label( labelName );
            expectedLabelNames.add( labelName );
        }
        return Pair.of( labels, expectedLabelNames );
    }
}
