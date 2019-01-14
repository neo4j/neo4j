/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.unsafe.batchinsert.internal;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
import java.util.stream.Collectors;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;
import org.neo4j.values.storable.Values;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.neo4j.graphdb.GraphDatabaseInternalLogIT.INTERNAL_LOG_FILE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.kernel.impl.store.RecordStore.getRecord;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.test.mockito.matcher.CollectionMatcher.matchesCollection;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

@RunWith( Parameterized.class )
public class BatchInsertTest
{
    private final int denseNodeThreshold;
    // This is the assumed internal index descriptor based on knowledge of what ids get assigned
    private static final SchemaIndexDescriptor internalIndex = SchemaIndexDescriptorFactory.forLabel( 0, 0 );
    private static final SchemaIndexDescriptor internalUniqueIndex = SchemaIndexDescriptorFactory.uniqueForLabel( 0, 0 );

    @Parameterized.Parameters
    public static Collection<Integer> data()
    {
        return Arrays.asList( 5, parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() ) );
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

    @ClassRule
    public static TestDirectory globalStoreDir = TestDirectory.testDirectory( BatchInsertTest.class );
    @ClassRule
    public static DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public TestDirectory storeDir = TestDirectory.testDirectory( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private static BatchInserter globalInserter;

    @BeforeClass
    public static void startGlobalInserter() throws IOException
    {
        // Global inserter can be used in tests which simply want to verify "local" behaviour,
        // e.g. create a node with some properties and read them back.
        globalInserter = BatchInserters.inserter(
                globalStoreDir.directory( "global" ), fileSystemRule.get(), stringMap() );
    }

    @After
    public void flushGlobalInserter()
    {
        forceFlush( globalInserter );
    }

    @AfterClass
    public static void shutDownGlobalInserter()
    {
        globalInserter.shutdown();
    }

    private Map<String, String> configuration()
    {
        return stringMap( GraphDatabaseSettings.dense_node_threshold.name(), String.valueOf( denseNodeThreshold ) );
    }

    private BatchInserter newBatchInserter() throws Exception
    {
        return BatchInserters.inserter( storeDir.absolutePath(), fileSystemRule.get(), configuration() );
    }

    private BatchInserter newBatchInserterWithIndexProvider( KernelExtensionFactory<?> provider ) throws Exception
    {
        return BatchInserters.inserter( storeDir.absolutePath(), fileSystemRule.get(), configuration(), singletonList( provider ) );
    }

    private GraphDatabaseService switchToEmbeddedGraphDatabaseService( BatchInserter inserter )
    {
        inserter.shutdown();
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fileSystemRule.get() );
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

    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1()
    {
        // Given
        BatchInserter inserter = globalInserter;

        String[] array1 = { "1" };
        String[] array2 = { "a" };

        long id1 = inserter.createNode(map("array", array1));
        long id2 = inserter.createNode( map() );

        // When
        inserter.getNodeProperties( id1 ).get( "array" );
        inserter.setNodeProperty( id1, "array", array1 );
        inserter.setNodeProperty( id2, "array", array2 );

        inserter.getNodeProperties( id1 ).get( "array" );
        inserter.setNodeProperty( id1, "array", array1 );
        inserter.setNodeProperty( id2, "array", array2 );

        // Then
        assertThat( inserter.getNodeProperties( id1 ).get( "array" ), equalTo( array1 ) );
    }

    @Test
    public void testSimple()
    {
        BatchInserter graphDb = globalInserter;
        long node1 = graphDb.createNode( null );
        long node2 = graphDb.createNode( null );
        long rel1 = graphDb.createRelationship( node1, node2, RelTypes.BATCH_TEST,
                null );
        BatchRelationship rel = graphDb.getRelationshipById( rel1 );
        assertEquals( rel.getStartNode(), node1 );
        assertEquals( rel.getEndNode(), node2 );
        assertEquals( RelTypes.BATCH_TEST.name(), rel.getType().name() );
    }

    @Test
    public void testSetAndAddNodeProperties()
    {
        BatchInserter inserter = globalInserter;

        long tehNode = inserter.createNode( MapUtil.map( "one", "one" ,"two","two","three","three") );
        inserter.setNodeProperty( tehNode, "four", "four" );
        inserter.setNodeProperty( tehNode, "five", "five" );
        Map<String, Object> props = getNodeProperties( inserter, tehNode );
        assertEquals( 5, props.size() );
        assertEquals( "one", props.get( "one" ) );
        assertEquals( "five", props.get( "five" ) );
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

    @Test
    public void testSetAndKeepNodeProperty() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        long tehNode = inserter.createNode( MapUtil.map( "foo", "bar" ) );
        inserter.setNodeProperty( tehNode, "foo2", "bar2" );
        Map<String, Object> props = getNodeProperties( inserter, tehNode );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter();

        props = getNodeProperties( inserter, tehNode );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.setNodeProperty( tehNode, "foo", "bar3" );

        props = getNodeProperties( inserter, tehNode );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
        inserter = newBatchInserter();

        props = getNodeProperties( inserter, tehNode );
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

        long from = inserter.createNode( Collections.emptyMap() );
        long to = inserter.createNode( Collections.emptyMap() );
        long theRel = inserter.createRelationship( from, to,
                RelationshipType.withName( "TestingPropsHere" ),
                MapUtil.map( "foo", "bar" ) );
        inserter.setRelationshipProperty( theRel, "foo2", "bar2" );
        Map<String, Object> props = getRelationshipProperties( inserter, theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter();

        props = getRelationshipProperties( inserter, theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.setRelationshipProperty( theRel, "foo", "bar3" );

        props = getRelationshipProperties( inserter, theRel );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
        inserter = newBatchInserter();

        props = getRelationshipProperties( inserter, theRel );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
    }

    @Test
    public void testNodeHasProperty()
    {
        BatchInserter inserter = globalInserter;

        long theNode = inserter.createNode( properties );
        long anotherNode = inserter.createNode( Collections.emptyMap() );
        long relationship = inserter.createRelationship( theNode, anotherNode,
                RelationshipType.withName( "foo" ), properties );
        for ( String key : properties.keySet() )
        {
            assertTrue( inserter.nodeHasProperty( theNode, key ) );
            assertFalse( inserter.nodeHasProperty( theNode, key + "-" ) );
            assertTrue( inserter.relationshipHasProperty( relationship, key ) );
            assertFalse( inserter.relationshipHasProperty( relationship, key + "-" ) );
        }
    }

    @Test
    public void testRemoveProperties() throws Exception
    {
        BatchInserter inserter = newBatchInserter();

        long theNode = inserter.createNode( properties );
        long anotherNode = inserter.createNode( Collections.emptyMap() );
        long relationship = inserter.createRelationship( theNode, anotherNode,
                RelationshipType.withName( "foo" ), properties );

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
        BatchInserter batchInserter = globalInserter;
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
        BatchInserter batchInserter = globalInserter;
        String key = "tags";
        long nodeId = batchInserter.createNode( MapUtil.map( key, new String[] { "one", "two", "three" } ) );

        // WHEN
        String[] secondValue = new String[] { "four", "five", "six" };
        batchInserter.setNodeProperty( nodeId, key, secondValue );

        // THEN
        assertTrue( Arrays.equals( secondValue, (String[]) getNodeProperties( batchInserter, nodeId ).get( key ) ) );
    }

    @Test
    public void testMore()
    {
        BatchInserter graphDb = globalInserter;
        long startNode = graphDb.createNode( properties );
        long[] endNodes = new long[25];
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
            assertEquals( realSelfRelationship,
                    realStartNode.getSingleRelationship( RelTypes.REL_TYPE1, Direction.INCOMING ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships( Direction.OUTGOING ) ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships() ) );
        }
        finally
        {
            db.shutdown();
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
        BatchInserter inserter = BatchInserters.inserter( storeDir, fileSystemRule.get(), stringMap() );
        inserter.shutdown();
        assertTrue( new File( storeDir, INTERNAL_LOG_FILE ).delete() );
    }

    @Test
    public void createEntitiesWithEmptyPropertiesMap()
    {
        BatchInserter inserter = globalInserter;

        // Assert for node
        long nodeId = inserter.createNode( map() );
        getNodeProperties( inserter, nodeId );
        //cp=N U http://www.w3.org/1999/02/22-rdf-syntax-ns#type, c=N

        // Assert for relationship
        long anotherNodeId = inserter.createNode( null );
        long relId = inserter.createRelationship( nodeId, anotherNodeId, RelTypes.BATCH_TEST, map() );
        inserter.getRelationshipProperties( relId );
    }

    @Test
    public void createEntitiesWithDynamicPropertiesMap()
    {
        BatchInserter inserter = globalInserter;

        setAndGet( inserter, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" );
        setAndGet( inserter, intArray() );
    }

    @Test
    public void shouldAddInitialLabelsToCreatedNode()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;

        // WHEN
        long node = inserter.createNode( map(), Labels.FIRST, Labels.SECOND );

        // THEN
        assertTrue( inserter.nodeHasLabel( node, Labels.FIRST ) );
        assertTrue( inserter.nodeHasLabel( node, Labels.SECOND ) );
        assertFalse( inserter.nodeHasLabel( node, Labels.THIRD ) );
    }

    @Test
    public void shouldGetNodeLabels()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        long node = inserter.createNode( map(), Labels.FIRST, Labels.THIRD );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( asSet( Labels.FIRST.name(), Labels.THIRD.name() ), Iterables.asSet( labelNames ) );
    }

    @Test
    public void shouldAddManyInitialLabelsAsDynamicRecords()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        Pair<Label[], Set<String>> labels = manyLabels( 200 );
        long node = inserter.createNode( map(), labels.first() );
        forceFlush( inserter );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( labels.other(), Iterables.asSet( labelNames ) );
    }

    @Test
    public void shouldReplaceExistingInlinedLabelsWithDynamic()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        long node = inserter.createNode( map(), Labels.FIRST );

        // WHEN
        Pair<Label[], Set<String>> labels = manyLabels( 100 );
        inserter.setNodeLabels( node, labels.first() );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( labels.other(), Iterables.asSet( labelNames ) );
    }

    @Test
    public void shouldReplaceExistingDynamicLabelsWithInlined()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        long node = inserter.createNode( map(), manyLabels( 150 ).first() );

        // WHEN
        inserter.setNodeLabels( node, Labels.FIRST );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( asSet( Labels.FIRST.name() ), Iterables.asSet( labelNames ) );
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
        assertEquals( asCollection( iterator( "handle" ) ), Iterables.asCollection( definition.getPropertyKeys() ) );
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
        assertEquals( asSet( "handle" ), Iterables.asSet( definition.getPropertyKeys() ) );
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
            NeoStores neoStores = graphdb.getDependencyResolver()
                    .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
            SchemaStore store = neoStores.getSchemaStore();
            SchemaStorage storage = new SchemaStorage( store );
            List<Long> inUse = new ArrayList<>();
            DynamicRecord record = store.nextRecord();
            for ( long i = 1, high = store.getHighestPossibleIdInUse(); i <= high; i++ )
            {
                store.getRecord( i, record, RecordLoad.FORCE );
                if ( record.inUse() && record.isStartRecord() )
                {
                    inUse.add( i );
                }
            }
            assertEquals( "records in use", 2, inUse.size() );
            SchemaRule rule0 = storage.loadSingleSchemaRule( inUse.get( 0 ) );
            SchemaRule rule1 = storage.loadSingleSchemaRule( inUse.get( 1 ) );
            IndexRule indexRule;
            ConstraintRule constraintRule;
            if ( rule0 instanceof IndexRule )
            {
                indexRule = (IndexRule) rule0;
                constraintRule = (ConstraintRule) rule1;
            }
            else
            {
                constraintRule = (ConstraintRule) rule0;
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
    public void shouldNotAllowCreationOfDuplicateIndex()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        String labelName = "Hacker1-" + denseNodeThreshold;

        // WHEN
        inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();

        try
        {
            inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldNotAllowCreationOfDuplicateConstraint()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        String labelName = "Hacker2-" + denseNodeThreshold;

        // WHEN
        inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();

        try
        {
            inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldNotAllowCreationOfDeferredSchemaConstraintAfterIndexOnSameKeys()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        String labelName = "Hacker3-" + denseNodeThreshold;

        // WHEN
        inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();

        try
        {
            inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldNotAllowCreationOfDeferredSchemaIndexAfterConstraintOnSameKeys()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        String labelName = "Hacker4-" + denseNodeThreshold;

        // WHEN
        inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();

        try
        {
            inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldRunIndexPopulationJobAtShutdown() throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection( add( nodeId, internalIndex.schema(),
                Values.of( "Jakewins" ) ) ) ) );
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
        IndexProvider provider = mock( IndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection( add( nodeId, internalUniqueIndex.schema(), Values.of( "Jakewins" ) ) ) ) );
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
        IndexProvider provider = mock( IndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        long boggle = inserter.createNode( map( "handle", "b0ggl3" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection(
                add( jakewins, internalIndex.schema(), Values.of( "Jakewins" ) ),
                add( boggle, internalIndex.schema(), Values.of( "b0ggl3" ) ) ) ) );
        verify( populator ).verifyDeferredConstraints( any( PropertyAccessor.class ) );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldPopulateLabelScanStoreOnShutdown() throws Exception
    {
        // GIVEN
        // -- a database and a mocked label scan store
        BatchInserter inserter = newBatchInserter();

        // -- and some data that we insert
        long node1 = inserter.createNode( null, Labels.FIRST );
        long node2 = inserter.createNode( null, Labels.SECOND );
        long node3 = inserter.createNode( null, Labels.THIRD );
        long node4 = inserter.createNode( null, Labels.FIRST, Labels.SECOND );
        long node5 = inserter.createNode( null, Labels.FIRST, Labels.THIRD );

        // WHEN we shut down the batch inserter
        LabelScanStore labelScanStore = getLabelScanStore();
        inserter.shutdown();

        labelScanStore.init();
        labelScanStore.start();

        // THEN the label scan store should receive all the updates.
        // of course, we don't know the label ids at this point, but we're assuming 0..2 (bad boy)
        assertLabelScanStoreContains( labelScanStore, 0, node1, node4, node5 );
        assertLabelScanStoreContains( labelScanStore, 1, node2, node4 );
        assertLabelScanStoreContains( labelScanStore, 2, node3, node5 );

        labelScanStore.shutdown();
    }

    @Test
    public void propertiesCanBeReSetUsingBatchInserter()
    {
        // GIVEN
        BatchInserter batchInserter = globalInserter;
        Map<String, Object> props = new HashMap<>();
        props.put( "name", "One" );
        props.put( "count", 1 );
        props.put( "tags", new String[] { "one", "two" } );
        props.put( "something", "something" );
        long nodeId = batchInserter.createNode( props );
        batchInserter.setNodeProperty( nodeId, "name", "NewOne" );
        batchInserter.removeNodeProperty( nodeId, "count" );
        batchInserter.removeNodeProperty( nodeId, "something" );

        // WHEN setting new properties
        batchInserter.setNodeProperty( nodeId, "name", "YetAnotherOne" );
        batchInserter.setNodeProperty( nodeId, "additional", "something" );

        // THEN there should be no problems doing so
        assertEquals( "YetAnotherOne", batchInserter.getNodeProperties( nodeId ).get( "name" ) );
        assertEquals("something", batchInserter.getNodeProperties( nodeId ).get( "additional" ) );
    }

    /**
     * Test checks that during node property set we will cleanup not used property records
     * During initial node creation properties will occupy 5 property records.
     * Last property record will have only empty array for email.
     * During first update email property will be migrated to dynamic property and last property record will become
     * empty. That record should be deleted form property chain or otherwise on next node load user will get an
     * property record not in use exception.
     */
    @Test
    public void testCleanupEmptyPropertyRecords() throws Exception
    {
        BatchInserter inserter = globalInserter;

        Map<String, Object> properties = new HashMap<>();
        properties.put("id", 1099511659993L);
        properties.put("firstName", "Edward");
        properties.put("lastName", "Shevchenko");
        properties.put("gender", "male");
        properties.put( "birthday", new SimpleDateFormat( "yyyy-MM-dd" ).parse( "1987-11-08" ).getTime() );
        properties.put("birthday_month", 11);
        properties.put("birthday_day", 8);
        long time = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSZ" )
                        .parse( "2010-04-22T18:05:40.912+0000" )
                        .getTime();
        properties.put("creationDate", time );
        properties.put("locationIP", "46.151.255.205");
        properties.put( "browserUsed", "Firefox" );
        properties.put( "email", new String[0] );
        properties.put( "languages", new String[0] );
        long personNodeId = inserter.createNode(properties);

        assertEquals( "Shevchenko", getNodeProperties( inserter, personNodeId ).get( "lastName" ) );
        assertThat( (String[]) getNodeProperties( inserter, personNodeId ).get( "email" ), is( emptyArray() ) );

        inserter.setNodeProperty( personNodeId, "email", new String[]{"Edward1099511659993@gmail.com"} );
        assertThat( (String[]) getNodeProperties( inserter, personNodeId ).get( "email" ),
                arrayContaining( "Edward1099511659993@gmail.com" ) );

        inserter.setNodeProperty( personNodeId, "email",
                new String[]{"Edward1099511659993@gmail.com", "backup@gmail.com"} );

        assertThat( (String[]) getNodeProperties( inserter, personNodeId ).get( "email" ),
                arrayContaining( "Edward1099511659993@gmail.com", "backup@gmail.com" ) );
    }

    @Test
    public void propertiesCanBeReSetUsingBatchInserter2()
    {
        // GIVEN
        BatchInserter batchInserter = globalInserter;
        long id = batchInserter.createNode( new HashMap<>() );

        // WHEN
        batchInserter.setNodeProperty( id, "test", "looooooooooong test" );
        batchInserter.setNodeProperty( id, "test", "small test" );

        // THEN
        assertEquals( "small test", batchInserter.getNodeProperties( id ).get( "test" ) );
    }

    @Test
    public void replaceWithBiggerPropertySpillsOverIntoNewPropertyRecord()
    {
        // GIVEN
        BatchInserter batchInserter = globalInserter;
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
    }

    @Test
    public void mustSplitUpRelationshipChainsWhenCreatingDenseNodes()
    {
        BatchInserter inserter = globalInserter;

        long node1 = inserter.createNode( null );
        long node2 = inserter.createNode( null );

        for ( int i = 0; i < 1000; i++ )
        {
            for ( MyRelTypes relType : MyRelTypes.values() )
            {
                inserter.createRelationship( node1, node2, relType, null );
            }
        }

        NeoStores neoStores = getFlushedNeoStores( inserter );
        NodeRecord record = getRecord( neoStores.getNodeStore(), node1 );
        assertTrue( "Node " + record + " should have been dense", record.isDense() );
    }

    @Test
    public void shouldGetRelationships()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        long node = inserter.createNode( null );
        createRelationships( inserter, node, RelTypes.REL_TYPE1, 3 );
        createRelationships( inserter, node, RelTypes.REL_TYPE2, 4 );

        // WHEN
        Set<Long> gottenRelationships = Iterables.asSet( inserter.getRelationshipIds( node ) );

        // THEN
        assertEquals( 21, gottenRelationships.size() );
    }

    @Test
    public void shouldNotCreateSameLabelTwiceOnSameNode()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;

        // WHEN
        long nodeId = inserter.createNode( map( "itemId", 1000L ), label( "Item" ),
                label( "Item" ) );

        // THEN
        NodeStore nodeStore = getFlushedNeoStores( inserter ).getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
        NodeLabels labels = NodeLabelsField.parseLabelsField( node );
        long[] labelIds = labels.get( nodeStore );
        assertEquals( 1, labelIds.length );
    }

    @Test
    public void shouldSortLabelIdsWhenGetOrCreate()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;

        // WHEN
        long nodeId = inserter.createNode( map( "Item", 123456789123L ), label( "AA" ),
                label( "BB" ), label( "CC" ), label( "DD" ) );
        inserter.setNodeLabels( nodeId, label( "CC" ), label( "AA" ),
                label( "DD" ), label( "EE" ), label( "FF" ) );

        // THEN
        NodeStore nodeStore = getFlushedNeoStores( inserter ).getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), RecordLoad.NORMAL );
        NodeLabels labels = NodeLabelsField.parseLabelsField( node );

        long[] labelIds = labels.get( nodeStore );
        long[] sortedLabelIds = labelIds.clone();
        Arrays.sort( sortedLabelIds );
        assertArrayEquals( sortedLabelIds, labelIds );
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
                List<ConstraintDefinition> constraints = Iterables.asList( db.schema().getConstraints() );
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
            assertEquals( format( "Node(0) already exists with label `%s` and property `%s` = '%s'",
                                    label.name(), propertyKey, duplicatedValue ), e.getMessage() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotAllowCreationOfUniquenessConstraintAndIndexOnSameLabelAndProperty()
    {
        // Given
        Label label = label( "Person1-" + denseNodeThreshold );
        String property = "name";

        BatchInserter inserter = globalInserter;

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
    public void shouldNotAllowDuplicatedUniquenessConstraints()
    {
        // Given
        Label label = label( "Person2-" + denseNodeThreshold );
        String property = "name";

        BatchInserter inserter = globalInserter;

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
                    "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}",
                    e.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowDuplicatedIndexes()
    {
        // Given
        Label label = label( "Person3-" + denseNodeThreshold );
        String property = "name";

        BatchInserter inserter = globalInserter;

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
        Label label = label( "Foo" );
        String property = "Bar";
        String value = "Baz";

        BatchInserter inserter = newBatchInserter();

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();

        inserter.createNode( Collections.singletonMap( property, value ), label );
        inserter.createNode( Collections.singletonMap( property, value ), label );

        // Then
        try
        {
            inserter.shutdown();
            fail( "Node that violates uniqueness constraint was created by batch inserter" );
        }
        catch ( RuntimeException ex )
        {
            // good
            assertEquals( new IndexEntryConflictException( 0, 1, Values.of( value ) ), ex.getCause() );
        }
    }

    @Test
    public void shouldChangePropertiesInCurrentBatch()
    {
        // GIVEN
        BatchInserter inserter = globalInserter;
        Map<String,Object> properties = map( "key1", "value1" );
        long node = inserter.createNode( properties );

        // WHEN
        properties.put( "additionalKey", "Additional value" );
        inserter.setNodeProperties( node, properties );

        // THEN
        assertEquals( properties, getNodeProperties( inserter, node ) );
    }

    @Test
    public void shouldIgnoreRemovingNonExistentNodeProperty()
    {
        // given
        BatchInserter inserter = globalInserter;
        long id = inserter.createNode( Collections.emptyMap() );

        // when
        inserter.removeNodeProperty( id, "non-existent" );

        // then no exception should be thrown, this mimics GraphDatabaseService behaviour
    }

    @Test
    public void shouldIgnoreRemovingNonExistentRelationshipProperty()
    {
        // given
        BatchInserter inserter = globalInserter;
        Map<String,Object> noProperties = Collections.emptyMap();
        long nodeId1 = inserter.createNode( noProperties );
        long nodeId2 = inserter.createNode( noProperties );
        long id = inserter.createRelationship( nodeId1, nodeId2, MyRelTypes.TEST, noProperties );

        // when
        inserter.removeRelationshipProperty( id, "non-existent" );

        // then no exception should be thrown, this mimics GraphDatabaseService behaviour
    }

    private LabelScanStore getLabelScanStore()
    {
        return new NativeLabelScanStore( pageCacheRule.getPageCache( fileSystemRule ), fileSystemRule, storeDir.absolutePath(),
                FullStoreChangeStream.EMPTY, true, new Monitors(), RecoveryCleanupWorkCollector.immediate() );
    }

    private void assertLabelScanStoreContains( LabelScanStore labelScanStore, int labelId, long... nodes )
    {
        try ( LabelScanReader labelScanReader = labelScanStore.newReader() )
        {
            List<Long> actualNodeIds = extractPrimitiveLongIteratorAsList( labelScanReader.nodesWithLabel( labelId ) );
            List<Long> expectedNodeIds = Arrays.stream( nodes ).boxed().collect( Collectors.toList() );
            assertEquals( expectedNodeIds, actualNodeIds );
        }
    }

    private List<Long> extractPrimitiveLongIteratorAsList( PrimitiveLongIterator primitiveLongIterator )
    {
        List<Long> actualNodeIds = new ArrayList<>();
        while ( primitiveLongIterator.hasNext() )
        {
            actualNodeIds.add( primitiveLongIterator.next() );
        }
        return actualNodeIds;
    }

    private void createRelationships( BatchInserter inserter, long node, RelationshipType relType, int out )
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

    private long dbWithIndexAndSingleIndexedNode() throws Exception
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
        when( provider.getPopulator( anyLong(), any( SchemaIndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( populator );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( InMemoryIndexProviderFactory.KEY, provider ) );

        inserter.createDeferredSchemaIndex( label("Hacker") ).on( "handle" ).create();
        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );
        inserter.shutdown();
        return nodeId;
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

    private int[] intArray()
    {
        int length = 20;
        int[] array = new int[length];
        for ( int i = 0, startValue = (int)Math.pow( 2, 30 ); i < length; i++ )
        {
            array[i] = startValue + i;
        }
        return array;
    }

    private Node getNodeInTx( long nodeId, GraphDatabaseService db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return db.getNodeById( nodeId );
        }
    }

    private void forceFlush( BatchInserter inserter )
    {
        ((BatchInserterImpl)inserter).forceFlushChanges();
    }

    private NeoStores getFlushedNeoStores( BatchInserter inserter )
    {
        forceFlush( inserter );
        return ((BatchInserterImpl) inserter).getNeoStores();
    }

    private enum Labels implements Label
    {
        FIRST,
        SECOND,
        THIRD
    }

    private Iterable<String> asNames( Iterable<Label> nodeLabels )
    {
        return map( Label::name, nodeLabels );
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

    private Map<String,Object> getNodeProperties( BatchInserter inserter, long nodeId )
    {
        return inserter.getNodeProperties( nodeId );
    }

    private Map<String,Object> getRelationshipProperties( BatchInserter inserter, long relId )
    {
        return inserter.getRelationshipProperties( relId );
    }
}
