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
package org.neo4j.unsafe.batchinsert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStoreExtension;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Integer.parseInt;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
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
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

@RunWith( Parameterized.class )
public class BatchInsertTest
{
    private final int denseNodeThreshold;

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        Collection<Object[]> result = new ArrayList<>();
        result.add( new Object[] { 3 } );
        result.add( new Object[] { 20 } );
        result.add( new Object[] { parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() ) } );
        return result;
    }

    public BatchInsertTest( int denseNodeThreshold )
    {
        this.denseNodeThreshold = denseNodeThreshold;
    }

    private static Map<String,Object> properties = new HashMap<>();

    private enum RelTypes implements RelationshipType
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

    private final FileSystemAbstraction fs = new org.neo4j.io.fs.DefaultFileSystemAbstraction();

    @Rule
    public TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private Map<String, String> configuration()
    {
        return stringMap( GraphDatabaseSettings.dense_node_threshold.name(), String.valueOf( denseNodeThreshold ) );
    }

    private BatchInserter newBatchInserter() throws Exception
    {
        return BatchInserters.inserter( storeDir.absolutePath(), fs, configuration() );
    }

    private BatchInserter newBatchInserterWithSchemaIndexProvider( KernelExtensionFactory<?> provider ) throws Exception
    {
        List<KernelExtensionFactory<?>> extensions = Arrays.asList(
                provider, new InMemoryLabelScanStoreExtension() );
        return BatchInserters.inserter( storeDir.absolutePath(), fs, configuration(), extensions );
    }

    private BatchInserter newBatchInserterWithLabelScanStore( KernelExtensionFactory<?> provider ) throws Exception
    {
        List<KernelExtensionFactory<?>> extensions = Arrays.asList(
                new InMemoryIndexProviderFactory(), provider );
        return BatchInserters.inserter( storeDir.absolutePath(), fs, configuration(), extensions );
    }

    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1() throws Exception
    {
        // Given
        BatchInserter batchInserter = newBatchInserter();

        String[] array1 = { "1" };
        String[] array2 = { "a" };

        long id1 = batchInserter.createNode(map("array", array1));
        long id2 = batchInserter.createNode( map() );

        // When
        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array2 );

        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array2 );

        // Then
        assertThat( (String[]) batchInserter.getNodeProperties( id1 ).get( "array" ), equalTo( array1 ) );

        batchInserter.shutdown();

    }

    @Test
    public void testSimple() throws Exception
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
    public void testSetAndAddNodeProperties() throws Exception
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
        assertThat( getNodeInTx( node, db ), inTx( db, hasProperty( key ).withValue( value ) ) );
        db.shutdown();
    }

    private GraphDatabaseService switchToEmbeddedGraphDatabaseService( BatchInserter inserter )
    {
        inserter.shutdown();
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        GraphDatabaseService db = factory.newImpermanentDatabaseBuilder( new File( inserter.getStoreDir() ) )
                // Shouldn't be necessary to set dense node threshold since it's a stick config
                .setConfig( configuration() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }

        return db;
    }

    private NeoStores switchToNeoStores( BatchInserter inserter )
    {
        inserter.shutdown();
        File dir = new File( inserter.getStoreDir() );
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreFactory storeFactory = new StoreFactory( fs, dir, pageCache, NullLogProvider.getInstance() );
        return storeFactory.openAllNeoStores();
    }

    @Test
    public void testSetAndKeepNodeProperty() throws Exception
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
    public void testSetAndKeepRelationshipProperty() throws Exception
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
    public void testNodeHasProperty() throws Exception
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
            assertFalse( inserter.relationshipHasProperty( relationship, key + "-" ) );
        }

        inserter.shutdown();
    }

    @Test
    public void testRemoveProperties() throws Exception
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
    public void shouldBeAbleToRemoveDynamicProperty() throws Exception
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
        batchInserter.shutdown();
    }

    @Test
    public void shouldBeAbleToOverwriteDynamicProperty() throws Exception
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
        batchInserter.shutdown();
    }

    @Test
    public void testMore() throws Exception
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
    public void makeSureLoopsCanBeCreated() throws Exception
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
        finally
        {
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
    public void messagesLogGetsClosed() throws Exception
    {
        File storeDir = this.storeDir.graphDbDir();
        BatchInserter inserter = BatchInserters.inserter( storeDir, stringMap() );
        inserter.shutdown();
        assertTrue( new File( storeDir, StoreLogService.INTERNAL_LOG_NAME ).delete() );
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
        inserter.shutdown();
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
        inserter.shutdown();
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
        inserter.shutdown();
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
        inserter.shutdown();
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
        inserter.shutdown();
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
        inserter.shutdown();
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
        inserter.shutdown();
    }

    @Test
    public void shouldCreateConsistentUniquenessConstraint() throws Exception
    {
        // given
        BatchInserter inserter = newBatchInserter();

        // when
        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        // then
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) switchToEmbeddedGraphDatabaseService( inserter );
        try
        {
            NeoStores neoStores = graphdb.getDependencyResolver().resolveDependency( NeoStoresSupplier.class ).get();
            SchemaStore store = neoStores.getSchemaStore();
            SchemaStorage storage = new SchemaStorage( store );
            List<Long> inUse = new ArrayList<>();
            for ( long i = 1, high = store.getHighestPossibleIdInUse(); i <= high; i++ )
            {
                DynamicRecord record = store.forceGetRecord( i );
                if ( record.inUse() && record.isStartRecord() )
                {
                    inUse.add( i );
                }
            }
            assertEquals( "records in use", 2, inUse.size() );
            SchemaRule rule0 = storage.loadSingleSchemaRule( inUse.get( 0 ) );
            SchemaRule rule1 = storage.loadSingleSchemaRule( inUse.get( 1 ) );
            IndexRule indexRule;
            UniquePropertyConstraintRule constraintRule;
            if ( rule0 instanceof IndexRule )
            {
                indexRule = (IndexRule) rule0;
                constraintRule = (UniquePropertyConstraintRule) rule1;
            }
            else
            {
                constraintRule = (UniquePropertyConstraintRule) rule0;
                indexRule = (IndexRule) rule1;
            }
            assertEquals( "index should reference constraint",
                          constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
            assertEquals( "constraint should reference index",
                          indexRule.getId(), constraintRule.getOwnedIndex() );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @Test
    public void shouldNotAllowCreationOfDuplicateIndex() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        try
        {
            inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // Good
        }

        // THEN
        inserter.shutdown();
    }

    @Test
    public void shouldNotAllowCreationOfDuplicateConstraint() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        try
        {
            inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // Good
        }

        // THEN
        inserter.shutdown();
    }

    @Test
    public void shouldNotAllowCreationOfDeferredSchemaConstraintAfterIndexOnSameKeys() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        try
        {
            inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // Good
        }

        // THEN
        inserter.shutdown();
    }

    @Test
    public void shouldNotAllowCreationOfDeferredSchemaIndexAfterConstraintOnSameKeys() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        try
        {
            inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // Good
        }

        // THEN
        inserter.shutdown();
    }

    @Test
    public void shouldRunIndexPopulationJobAtShutdown() throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexDescriptor.class ),
                any( IndexConfiguration.class ), any( IndexSamplingConfig.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( nodeId, "Jakewins" );
        verify( populator ).verifyDeferredConstraints( any( PropertyAccessor.class ) );
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
        when( provider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( nodeId, "Jakewins" );
        verify( populator ).verifyDeferredConstraints( any( PropertyAccessor.class ) );
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
        when( provider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        long boggle = inserter.createNode( map( "handle", "b0ggl3" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( jakewins, "Jakewins" );
        verify( populator ).add( boggle, "b0ggl3" );
        verify( populator ).verifyDeferredConstraints( any( PropertyAccessor.class ) );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
        verifyNoMoreInteractions( populator );
    }

    private long dbWithIndexAndSingleIndexedNode() throws Exception
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithSchemaIndexProvider(
                singleInstanceSchemaIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label("Hacker") ).on( "handle" ).create();
        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );
        inserter.shutdown();
        return nodeId;
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
    public void propertiesCanBeReSetUsingBatchInserter() throws Exception
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

    /**
     * Test checks that during node property set we will cleanup not used property records
     * During initial node creation properties will occupy 5 property records.
     * Last property record will have only empty array for email.
     * During first update email property will be migrated to dynamic property and last property record will become
     * empty. That record should be deleted form property chain or otherwise on next node load user will get an
     * property record not in use exception.
     * @throws Exception
     */
    @Test
    public void testCleanupEmptyPropertyRecords() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        Map<String, Object> properties = new HashMap<>();
        properties.put("id", 1099511659993l);
        properties.put("firstName", "Edward");
        properties.put("lastName", "Shevchenko");
        properties.put("gender", "male");
        properties.put("birthday", new SimpleDateFormat("yyyy-MM-dd").parse( "1987-11-08" ).getTime());
        properties.put("birthday_month", 11);
        properties.put("birthday_day", 8);
        properties.put("creationDate", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse( "2010-04-22T18:05:40.912+0000" ).getTime());
        properties.put("locationIP", "46.151.255.205");
        properties.put( "browserUsed", "Firefox" );
        properties.put( "email", new String[0] );
        properties.put( "languages", new String[0] );
        long personNodeId = inserter.createNode(properties);

        assertEquals( "Shevchenko", inserter.getNodeProperties( personNodeId ).get( "lastName" ) );
        assertThat( (String[]) inserter.getNodeProperties( personNodeId ).get( "email" ), is( emptyArray() ) );

        inserter.setNodeProperty( personNodeId, "email", new String[]{"Edward1099511659993@gmail.com"} );
        assertThat( (String[]) inserter.getNodeProperties( personNodeId ).get( "email" ),
                arrayContaining( "Edward1099511659993@gmail.com" ) );

        inserter.setNodeProperty( personNodeId, "email",
                new String[]{"Edward1099511659993@gmail.com", "backup@gmail.com"} );

        assertThat( (String[]) inserter.getNodeProperties( personNodeId ).get( "email" ),
                arrayContaining( "Edward1099511659993@gmail.com", "backup@gmail.com" ) );
    }

    @Test
    public void propertiesCanBeReSetUsingBatchInserter2() throws Exception
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
    public void replaceWithBiggerPropertySpillsOverIntoNewPropertyRecord() throws Exception
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

    @Test
    public void mustSplitUpRelationshipChainsWhenCreatingDenseNodes() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        inserter.createNode( 1, null );
        inserter.createNode( 2, null );

        for ( int i = 0; i < 1000; i++ )
        {
            for ( MyRelTypes relType : MyRelTypes.values() )
            {
                inserter.createRelationship( 1, 2, relType, null );
            }
        }

        GraphDatabaseAPI db = (GraphDatabaseAPI) switchToEmbeddedGraphDatabaseService( inserter );
        try
        {
            DependencyResolver dependencyResolver = db.getDependencyResolver();
            NeoStoresSupplier neoStoresSupplier = dependencyResolver.resolveDependency( NeoStoresSupplier.class );
            NeoStores neoStores = neoStoresSupplier.get();
            NodeStore nodeStore = neoStores.getNodeStore();
            NodeRecord record = nodeStore.getRecord( 1 );
            assertTrue( "Node " + record + " should have been dense", record.isDense() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldGetRelationships() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        long node = inserter.createNode( null );
        createRelationships( inserter, node, RelTypes.REL_TYPE1, 3, 2, 1 );
        createRelationships( inserter, node, RelTypes.REL_TYPE2, 4, 5, 6 );

        // WHEN
        Set<Long> gottenRelationships = asSet( inserter.getRelationshipIds( node ) );

        // THEN
        assertEquals( 21, gottenRelationships.size() );
        inserter.shutdown();
    }

    @Test
    public void shouldNotCreateSameLabelTwiceOnSameNode() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        long nodeId = inserter.createNode( map( "itemId", 1000l ), DynamicLabel.label( "Item" ),
                DynamicLabel.label( "Item" ) );

        // THEN
        NeoStores neoStores = switchToNeoStores( inserter );
        try
        {
            NodeRecord node = neoStores.getNodeStore().getRecord( nodeId );
            NodeLabels labels = NodeLabelsField.parseLabelsField( node );
            long[] labelIds = labels.get( neoStores.getNodeStore() );
            assertEquals( 1, labelIds.length );
        }
        finally
        {
            neoStores.close();
        }
    }

    @Test
    public void shouldSortLabelIdsWhenGetOrCreate() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();

        // WHEN
        long nodeId = inserter.createNode( map( "Item", 123456789123l ), DynamicLabel.label( "AA" ),
                DynamicLabel.label( "BB" ), DynamicLabel.label( "CC" ), DynamicLabel.label( "DD" ) );
        inserter.setNodeLabels( nodeId, DynamicLabel.label( "CC" ), DynamicLabel.label( "AA" ),
                DynamicLabel.label( "DD" ), DynamicLabel.label( "EE" ), DynamicLabel.label( "FF" ) );

        // THEN
        try ( NeoStores neoStores = switchToNeoStores( inserter ) )
        {
            NodeRecord node = neoStores.getNodeStore().getRecord( nodeId );
            NodeLabels labels = NodeLabelsField.parseLabelsField( node );

            long[] labelIds = labels.get( neoStores.getNodeStore() );
            long[] sortedLabelIds = labelIds.clone();
            Arrays.sort( sortedLabelIds );
            assertArrayEquals( sortedLabelIds, labelIds );
        }
    }

    @Test
    public void shouldCreateUniquenessConstraint() throws Exception
    {
        // Given
        Label label = label( "Person" );
        String propertyKey = "name";
        String duplicatedValue = "Tom";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( propertyKey ).create();

        // Then
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                List<ConstraintDefinition> constraints = asList( db.schema().getConstraints() );
                assertEquals( 1, constraints.size() );
                ConstraintDefinition constraint = constraints.get( 0 );
                assertEquals( label.name(), constraint.getLabel().name() );
                assertEquals( propertyKey, single( constraint.getPropertyKeys() ) );

                db.createNode( label ).setProperty( propertyKey, duplicatedValue );

                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label ).setProperty( propertyKey, duplicatedValue );
                tx.success();
            }
            fail( "Uniqueness property constraint was violated, exception expected" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( e.getMessage(),
                    "Node 0 already exists with label " + label.name() + " and property \"" + propertyKey + "\"=[" +
                    duplicatedValue + "]"
            );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotAllowCreationOfUniquenessConstraintAndIndexOnSameLabelAndProperty() throws Exception
    {
        // Given
        Label label = label( "Person" );
        String property = "name";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();
        try
        {
            inserter.createDeferredSchemaIndex( label ).on( property ).create();
            fail( "Exception expected" );
        }
        catch ( ConstraintViolationException e )
        {
            // Then
            assertEquals( "Index for given {label;property} already exists", e.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowDuplicatedUniquenessConstraints() throws Exception
    {
        // Given
        Label label = label( "Person" );
        String property = "name";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();
        try
        {
            inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();
            fail( "Exception expected" );
        }
        catch ( ConstraintViolationException e )
        {
            // Then
            assertEquals(
                    "It is not allowed to create uniqueness constraints and indexes on the same {label;property}",
                    e.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowDuplicatedIndexes() throws Exception
    {
        // Given
        Label label = label( "Person" );
        String property = "name";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredSchemaIndex( label ).on( property ).create();
        try
        {
            inserter.createDeferredSchemaIndex( label ).on( property ).create();
            fail( "Exception expected" );
        }
        catch ( ConstraintViolationException e )
        {
            // Then
            assertEquals( "Index for given {label;property} already exists", e.getMessage() );
        }
    }

    @Test
    public void uniquenessConstraintShouldBeCheckedOnBatchInserterShutdownAndFailIfViolated() throws Exception
    {
        // Given
        Label label = DynamicLabel.label( "Foo" );
        String property = "Bar";
        String value = "Baz";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();

        inserter.createNode( Collections.<String,Object>singletonMap( property, value ), label );
        inserter.createNode( Collections.<String,Object>singletonMap( property, value ), label );

        // Then
        try
        {
            inserter.shutdown();
            fail( "Node that violates uniqueness constraint was created by batch inserter" );
        }
        catch ( RuntimeException ex )
        {
            // good
            assertEquals( new PreexistingIndexEntryConflictException( value, 0, 1 ), ex.getCause() );
        }
    }

    @Test
    public void shouldChangePropertiesInCurrentBatch() throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter();
        Map<String,Object> properties = map( "key1", "value1" );
        long node = inserter.createNode( properties );

        // WHEN
        properties.put( "additionalKey", "Additional value" );
        inserter.setNodeProperties( node, properties );

        // THEN
        assertEquals( properties, inserter.getNodeProperties( node ) );
    }

    private void createRelationships( BatchInserter inserter, long node, RelationshipType relType,
            int out, int in, int loop )
    {
        for ( int i = 0; i < out; i++ )
        {
            inserter.createRelationship( node, inserter.createNode( null ), relType, null );
        }
        for ( int i = 0; i < out; i++ )
        {
            inserter.createRelationship( inserter.createNode( null ), node, relType, null );
        }
        for ( int i = 0; i < out; i++ )
        {
            inserter.createRelationship( node, node, relType, null );
        }
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
