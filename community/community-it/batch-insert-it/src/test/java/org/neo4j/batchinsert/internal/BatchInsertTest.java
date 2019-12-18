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
package org.neo4j.batchinsert.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.index.label.FullStoreChangeStream;
import org.neo4j.internal.index.label.LabelScanReader;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.NativeLabelScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.test.mockito.matcher.CollectionMatcher.matchesCollection;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

@PageCacheExtension
@Neo4jLayoutExtension
class BatchInsertTest
{
    private static final IndexProviderDescriptor DESCRIPTOR = TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
    private static final String KEY = DESCRIPTOR.getKey();
    private static final String INTERNAL_LOG_FILE = "debug.log";
    // This is the assumed internal index descriptor based on knowledge of what ids get assigned
    private static final IndexDescriptor internalIndex = TestIndexDescriptorFactory.forLabel( 0, 0 );
    private static final IndexDescriptor internalUniqueIndex = TestIndexDescriptorFactory.uniqueForLabel( 0, 0 );
    private static final Map<String, Object> properties = new HashMap<>();
    private static final RelationshipType[] relTypeArray = {
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
        properties.put( "key10", new String[]{
            "SDSDASSDLKSDSAKLSLDAKSLKDLSDAKLDSLA", "dsasda", "dssadsad"
        } );
        properties.put( "key11", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key12", new short[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key13", new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key14", new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key15", new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key16", new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
        properties.put( "key17", new boolean[]{true, false, true, false} );
        properties.put( "key18", new char[]{1, 2, 3, 4, 5, 6, 7, 8, 9} );
    }
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private DatabaseManagementService managementService;

    private static Stream<Arguments> params()
    {
        return Stream.of(
            arguments( 5 ),
            arguments( GraphDatabaseSettings.dense_node_threshold.defaultValue() )
        );
    }
    private enum RelTypes implements RelationshipType
    {
        BATCH_TEST,
        REL_TYPE1,
        REL_TYPE2,
        REL_TYPE3,
        REL_TYPE4,
        REL_TYPE5
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1( int denseNodeThreshold ) throws Exception
    {
        // Given
        var inserter = newBatchInserter( denseNodeThreshold );

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
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSimple( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long node1 = inserter.createNode( null );
        long node2 = inserter.createNode( null );
        long rel1 = inserter.createRelationship( node1, node2, RelTypes.BATCH_TEST,
                null );
        BatchRelationship rel = inserter.getRelationshipById( rel1 );
        assertEquals( rel.getStartNode(), node1 );
        assertEquals( rel.getEndNode(), node2 );
        assertEquals( RelTypes.BATCH_TEST.name(), rel.getType().name() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSetAndAddNodeProperties( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long tehNode = inserter.createNode( map( "one", "one", "two", "two", "three", "three" ) );
        inserter.setNodeProperty( tehNode, "four", "four" );
        inserter.setNodeProperty( tehNode, "five", "five" );
        Map<String, Object> props = getNodeProperties( inserter, tehNode );
        assertEquals( 5, props.size() );
        assertEquals( "one", props.get( "one" ) );
        assertEquals( "five", props.get( "five" ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void setSingleProperty( int denseNodeThreshold ) throws Exception
    {
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );
        long node = inserter.createNode( null );

        String value = "Something";
        String key = "name";
        inserter.setNodeProperty( node, key, value );

        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        assertThat( getNodeInTx( node, db ), inTx( db, hasProperty( key ).withValue( value ) ) );
        managementService.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSetAndKeepNodeProperty( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long tehNode = inserter.createNode( map( "foo", "bar" ) );
        inserter.setNodeProperty( tehNode, "foo2", "bar2" );
        Map<String, Object> props = getNodeProperties( inserter, tehNode );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter( denseNodeThreshold );

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
        inserter = newBatchInserter( denseNodeThreshold );

        props = getNodeProperties( inserter, tehNode );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSetAndKeepRelationshipProperty( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long from = inserter.createNode( Collections.emptyMap() );
        long to = inserter.createNode( Collections.emptyMap() );
        long theRel = inserter.createRelationship( from, to,
                RelationshipType.withName( "TestingPropsHere" ),
            map( "foo", "bar" ) );
        inserter.setRelationshipProperty( theRel, "foo2", "bar2" );
        Map<String, Object> props = getRelationshipProperties( inserter, theRel );
        assertEquals( 2, props.size() );
        assertEquals( "bar", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();

        inserter = newBatchInserter( denseNodeThreshold );

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
        inserter = newBatchInserter( denseNodeThreshold );

        props = getRelationshipProperties( inserter, theRel );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( 2, props.size() );
        assertEquals( "bar3", props.get( "foo" ) );
        assertEquals( "bar2", props.get( "foo2" ) );

        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testNodeHasProperty( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
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
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testRemoveProperties( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
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
        inserter = newBatchInserter( denseNodeThreshold );

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

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldBeAbleToRemoveDynamicProperty( int denseNodeThreshold ) throws Exception
    {
        // Only triggered if assertions are enabled

        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        String key = "tags";
        long nodeId = inserter.createNode( map( key, new String[]{"one", "two", "three"} ) );

        // WHEN
        inserter.removeNodeProperty( nodeId, key );

        // THEN
        assertFalse( inserter.getNodeProperties( nodeId ).containsKey( key ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldBeAbleToOverwriteDynamicProperty( int denseNodeThreshold ) throws Exception
    {
        // Only triggered if assertions are enabled

        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        String key = "tags";
        long nodeId = inserter.createNode( map( key, new String[]{"one", "two", "three"} ) );

        // WHEN
        String[] secondValue = {"four", "five", "six"};
        inserter.setNodeProperty( nodeId, key, secondValue );

        // THEN
        assertArrayEquals( secondValue, (String[]) getNodeProperties( inserter, nodeId ).get( key ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testMore( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long startNode = inserter.createNode( properties );
        long[] endNodes = new long[25];
        Set<Long> rels = new HashSet<>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = inserter.createNode( properties );
            rels.add( inserter.createRelationship( startNode, endNodes[i],
                relTypeArray[i % 5], properties ) );
        }
        for ( BatchRelationship rel : inserter.getRelationships( startNode ) )
        {
            assertTrue( rels.contains( rel.getId() ) );
            assertEquals( rel.getStartNode(), startNode );
        }
        inserter.setNodeProperties( startNode, properties );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void makeSureLoopsCanBeCreated( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long startNode = inserter.createNode( properties );
        long otherNode = inserter.createNode( properties );
        long selfRelationship = inserter.createRelationship( startNode, startNode,
                relTypeArray[0], properties );
        long relationship = inserter.createRelationship( startNode, otherNode,
                relTypeArray[0], properties );
        for ( BatchRelationship rel : inserter.getRelationships( startNode ) )
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

        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( Transaction transaction = db.beginTx() )
        {
            Node realStartNode = transaction.getNodeById( startNode );
            Relationship realSelfRelationship = transaction.getRelationshipById( selfRelationship );
            Relationship realRelationship = transaction.getRelationshipById( relationship );
            assertEquals( realSelfRelationship,
                    realStartNode.getSingleRelationship( RelTypes.REL_TYPE1, Direction.INCOMING ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships( Direction.OUTGOING ) ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void createBatchNodeAndRelationshipsDeleteAllInEmbedded( int denseNodeThreshold ) throws Exception
    {
        /*
         *    ()--[REL_TYPE1]-->(node)--[BATCH_TEST]->()
         */

        var inserter = newBatchInserter( denseNodeThreshold );
        long nodeId = inserter.createNode( null );
        inserter.createRelationship( nodeId, inserter.createNode( null ),
                RelTypes.BATCH_TEST, null );
        inserter.createRelationship( inserter.createNode( null ), nodeId,
                RelTypes.REL_TYPE1, null );

        // Delete node and all its relationships
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.commit();
        }

        managementService.shutdown();
    }

    @Test
    void messagesLogGetsClosed() throws IOException
    {

        BatchInserter inserter = BatchInserters.inserter( databaseLayout, fs,
                Config.defaults( neo4j_home, testDirectory.homeDir().toPath() ) );
        inserter.shutdown();
        assertTrue( new File( databaseLayout.getNeo4jLayout().homeDirectory(), INTERNAL_LOG_FILE ).delete() );
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void createEntitiesWithEmptyPropertiesMap( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );

        // Assert for node
        long nodeId = inserter.createNode( map() );
        getNodeProperties( inserter, nodeId );
        //cp=N U http://www.w3.org/1999/02/22-rdf-syntax-ns#type, c=N

        // Assert for relationship
        long anotherNodeId = inserter.createNode( null );
        long relId = inserter.createRelationship( nodeId, anotherNodeId, RelTypes.BATCH_TEST, map() );
        inserter.getRelationshipProperties( relId );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void createEntitiesWithDynamicPropertiesMap( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );

        setAndGet( inserter, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" );
        setAndGet( inserter, intArray() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldAddInitialLabelsToCreatedNode( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        long node = inserter.createNode( map(), Labels.FIRST, Labels.SECOND );

        // THEN
        assertTrue( inserter.nodeHasLabel( node, Labels.FIRST ) );
        assertTrue( inserter.nodeHasLabel( node, Labels.SECOND ) );
        assertFalse( inserter.nodeHasLabel( node, Labels.THIRD ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldGetNodeLabels( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long node = inserter.createNode( map(), Labels.FIRST, Labels.THIRD );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( asSet( Labels.FIRST.name(), Labels.THIRD.name() ), Iterables.asSet( labelNames ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldAddManyInitialLabelsAsDynamicRecords( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        Pair<Label[], Set<String>> labels = manyLabels( 200 );
        long node = inserter.createNode( map(), labels.first() );
        forceFlush( inserter );

        // WHEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );

        // THEN
        assertEquals( labels.other(), Iterables.asSet( labelNames ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldReplaceExistingInlinedLabelsWithDynamic( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long node = inserter.createNode( map(), Labels.FIRST );

        // WHEN
        Pair<Label[], Set<String>> labels = manyLabels( 100 );
        inserter.setNodeLabels( node, labels.first() );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( labels.other(), Iterables.asSet( labelNames ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldReplaceExistingDynamicLabelsWithInlined( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long node = inserter.createNode( map(), manyLabels( 150 ).first() );

        // WHEN
        inserter.setNodeLabels( node, Labels.FIRST );

        // THEN
        Iterable<String> labelNames = asNames( inserter.getNodeLabels( node ) );
        assertEquals( asSet( Labels.FIRST.name() ), Iterables.asSet( labelNames ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldCreateDeferredSchemaIndexesInEmptyDatabase( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        IndexDefinition definition = inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        // THEN
        assertEquals( "Hacker", single( definition.getLabels() ).name() );
        assertEquals( asCollection( iterator( "handle" ) ), Iterables.asCollection( definition.getPropertyKeys() ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldCreateDeferredUniquenessConstraintInEmptyDatabase( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        ConstraintDefinition definition =
                inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        // THEN
        assertEquals( "Hacker", definition.getLabel().name() );
        assertEquals( ConstraintType.UNIQUENESS, definition.getConstraintType() );
        assertEquals( asSet( "handle" ), Iterables.asSet( definition.getPropertyKeys() ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldCreateConsistentUniquenessConstraint( int denseNodeThreshold ) throws Exception
    {
        // given
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

        // when
        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        // then
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try
        {
            NeoStores neoStores = graphdb.getDependencyResolver()
                    .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
            SchemaStore store = neoStores.getSchemaStore();
            TokenHolders tokenHolders = graphdb.getDependencyResolver().resolveDependency( TokenHolders.class );
            SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( store, tokenHolders );
            List<Long> inUse = new ArrayList<>();
            SchemaRecord record = store.newRecord();
            for ( long i = 1, high = store.getHighestPossibleIdInUse(); i <= high; i++ )
            {
                store.getRecord( i, record, RecordLoad.FORCE );
                if ( record.inUse() )
                {
                    inUse.add( i );
                }
            }
            assertEquals( 2, inUse.size(), "records in use" );
            SchemaRule rule0 = schemaRuleAccess.loadSingleSchemaRule( inUse.get( 0 ) );
            SchemaRule rule1 = schemaRuleAccess.loadSingleSchemaRule( inUse.get( 1 ) );
            IndexDescriptor indexRule;
            ConstraintDescriptor constraint;
            if ( rule0 instanceof IndexDescriptor )
            {
                indexRule = (IndexDescriptor) rule0;
                constraint = (ConstraintDescriptor) rule1;
            }
            else
            {
                constraint = (ConstraintDescriptor) rule0;
                indexRule = (IndexDescriptor) rule1;
            }
            OptionalLong owningConstraintId = indexRule.getOwningConstraintId();
            assertTrue( owningConstraintId.isPresent(), "index should have owning constraint" );
            assertEquals(
                constraint.getId(), owningConstraintId.getAsLong(), "index should reference constraint" );
            assertEquals(
                indexRule.getId(), constraint.asIndexBackedConstraint().ownedIndexId(), "constraint should reference index" );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowCreationOfDuplicateIndex( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        String labelName = "Hacker1";

        // WHEN
        inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();

        assertThrows( ConstraintViolationException.class, () -> inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowCreationOfDuplicateConstraint( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        String labelName = "Hacker2";

        // WHEN
        inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();

        assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowCreationOfDeferredSchemaConstraintAfterIndexOnSameKeys( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        String labelName = "Hacker3";
        var inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create();

        assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowCreationOfDeferredSchemaIndexAfterConstraintOnSameKeys( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        String labelName = "Hacker4";
        var inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        inserter.createDeferredConstraint( label( labelName ) ).assertPropertyIsUnique( "handle" ).create();

        assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredSchemaIndex( label( labelName ) ).on( "handle" ).create() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldRunIndexPopulationJobAtShutdown( int denseNodeThreshold ) throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        when( provider.getProviderDescriptor() ).thenReturn( DESCRIPTOR );
        when( provider.getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() ) ).thenReturn( populator );
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        when( provider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) ).thenReturn( accessor );
        when( provider.completeConfiguration( any( IndexDescriptor.class ) ) ).then( inv -> inv.getArgument( 0 ) );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( KEY, provider ), provider.getProviderDescriptor(), denseNodeThreshold );

        inserter.createDeferredSchemaIndex( label( "Hacker" ) ).on( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection( add( nodeId, internalIndex.schema(), Values.of( "Jakewins" ) ) ) ) );
        verify( populator ).verifyDeferredConstraints( any( NodePropertyAccessor.class ) );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldRunConstraintPopulationJobAtShutdown( int denseNodeThreshold ) throws Throwable
    {
        // GIVEN
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        when( provider.getProviderDescriptor() ).thenReturn( DESCRIPTOR );
        when( provider.getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() ) ).thenReturn( populator );
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        when( provider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) ).thenReturn( accessor );
        when( provider.completeConfiguration( any( IndexDescriptor.class ) ) ).then( inv -> inv.getArgument( 0 ) );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( KEY, provider ), provider.getProviderDescriptor(), denseNodeThreshold );

        inserter.createDeferredConstraint( label( "Hacker" ) ).assertPropertyIsUnique( "handle" ).create();

        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection( add( nodeId, internalUniqueIndex.schema(), Values.of( "Jakewins" ) ) ) ) );
        verify( populator ).verifyDeferredConstraints( any( NodePropertyAccessor.class ) );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldRepopulatePreexistingIndexed( int denseNodeThreshold ) throws Throwable
    {
        // GIVEN
        long jakewins = dbWithIndexAndSingleIndexedNode( denseNodeThreshold );

        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );
        IndexAccessor accessor = mock( IndexAccessor.class );

        when( provider.getProviderDescriptor() ).thenReturn( DESCRIPTOR );
        when( provider.completeConfiguration( any( IndexDescriptor.class ) ) ).then( inv -> inv.getArgument( 0 ) );
        when( provider.getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() ) ).thenReturn( populator );
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        when( provider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) ).thenReturn( accessor );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( KEY, provider ), provider.getProviderDescriptor(), denseNodeThreshold );

        long boggle = inserter.createNode( map( "handle", "b0ggl3" ), label( "Hacker" ) );

        // WHEN
        inserter.shutdown();

        // THEN
        verify( provider ).init();
        verify( provider ).start();
        verify( provider ).getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() );
        verify( populator ).create();
        verify( populator ).add( argThat( matchesCollection(
                add( jakewins, internalIndex.schema(), Values.of( "Jakewins" ) ),
                add( boggle, internalIndex.schema(), Values.of( "b0ggl3" ) ) ) ) );
        verify( populator ).verifyDeferredConstraints( any( NodePropertyAccessor.class ) );
        verify( populator ).close( true );
        verify( provider ).stop();
        verify( provider ).shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldPopulateLabelScanStoreOnShutdown( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        // -- a database and a mocked label scan store
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

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

    @ParameterizedTest
    @MethodSource( "params" )
    void propertiesCanBeReSetUsingBatchInserter( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        Map<String, Object> props = new HashMap<>();
        props.put( "name", "One" );
        props.put( "count", 1 );
        props.put( "tags", new String[] { "one", "two" } );
        props.put( "something", "something" );
        long nodeId = inserter.createNode( props );
        inserter.setNodeProperty( nodeId, "name", "NewOne" );
        inserter.removeNodeProperty( nodeId, "count" );
        inserter.removeNodeProperty( nodeId, "something" );

        // WHEN setting new properties
        inserter.setNodeProperty( nodeId, "name", "YetAnotherOne" );
        inserter.setNodeProperty( nodeId, "additional", "something" );

        // THEN there should be no problems doing so
        assertEquals( "YetAnotherOne", inserter.getNodeProperties( nodeId ).get( "name" ) );
        assertEquals("something", inserter.getNodeProperties( nodeId ).get( "additional" ) );
        inserter.shutdown();
    }

    /**
     * Test checks that during node property set we will cleanup not used property records
     * During initial node creation properties will occupy 5 property records.
     * Last property record will have only empty array for email.
     * During first update email property will be migrated to dynamic property and last property record will become
     * empty. That record should be deleted form property chain or otherwise on next node load user will get an
     * property record not in use exception.
     * @param denseNodeThreshold relationship group threshold from "params".
     */
    @ParameterizedTest
    @MethodSource( "params" )
    void testCleanupEmptyPropertyRecords( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );

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
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void propertiesCanBeReSetUsingBatchInserter2( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long id = inserter.createNode( new HashMap<>() );

        // WHEN
        inserter.setNodeProperty( id, "test", "looooooooooong test" );
        inserter.setNodeProperty( id, "test", "small test" );

        // THEN
        assertEquals( "small test", inserter.getNodeProperties( id ).get( "test" ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void replaceWithBiggerPropertySpillsOverIntoNewPropertyRecord( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        Map<String, Object> props = new HashMap<>();
        props.put( "name", "One" );
        props.put( "count", 1 );
        props.put( "tags", new String[] { "one", "two" } );
        long id = inserter.createNode( props );
        inserter.setNodeProperty( id, "name", "NewOne" );

        // WHEN
        inserter.setNodeProperty( id, "count", "something" );

        // THEN
        assertEquals( "something", inserter.getNodeProperties( id ).get( "count" ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void mustSplitUpRelationshipChainsWhenCreatingDenseNodes( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );

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
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord record = nodeStore.getRecord( node1, nodeStore.newRecord(), NORMAL );
        assertTrue( record.isDense(), "Node " + record + " should have been dense" );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldGetRelationships( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long node = inserter.createNode( null );
        createRelationships( inserter, node, RelTypes.REL_TYPE1, 3 );
        createRelationships( inserter, node, RelTypes.REL_TYPE2, 4 );

        // WHEN
        Set<Long> gottenRelationships = Iterables.asSet( inserter.getRelationshipIds( node ) );

        // THEN
        assertEquals( 21, gottenRelationships.size() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotCreateSameLabelTwiceOnSameNode( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        // WHEN
        long nodeId = inserter.createNode( map( "itemId", 1000L ), label( "Item" ),
                label( "Item" ) );

        // THEN
        NodeStore nodeStore = getFlushedNeoStores( inserter ).getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
        NodeLabels labels = NodeLabelsField.parseLabelsField( node );
        long[] labelIds = labels.get( nodeStore );
        assertEquals( 1, labelIds.length );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSortLabelIdsWhenGetOrCreate( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );

        // WHEN
        long nodeId = inserter.createNode( map( "Item", 123456789123L ), label( "AA" ),
                label( "BB" ), label( "CC" ), label( "DD" ) );
        inserter.setNodeLabels( nodeId, label( "CC" ), label( "AA" ),
                label( "DD" ), label( "EE" ), label( "FF" ) );

        // THEN
        NodeStore nodeStore = getFlushedNeoStores( inserter ).getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
        NodeLabels labels = NodeLabelsField.parseLabelsField( node );

        long[] labelIds = labels.get( nodeStore );
        long[] sortedLabelIds = Arrays.copyOf( labelIds, labelIds.length );
        Arrays.sort( sortedLabelIds );
        assertArrayEquals( sortedLabelIds, labelIds );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldCreateUniquenessConstraint( int denseNodeThreshold ) throws Exception
    {
        // Given
        Label label = label( "Person" );
        String propertyKey = "name";
        String duplicatedValue = "Tom";
        var inserter = newBatchInserter( denseNodeThreshold );

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( propertyKey ).create();

        // Then
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                List<ConstraintDefinition> constraints = Iterables.asList( tx.schema().getConstraints() );
                assertEquals( 1, constraints.size() );
                ConstraintDefinition constraint = constraints.get( 0 );
                assertEquals( label.name(), constraint.getLabel().name() );
                assertEquals( propertyKey, single( constraint.getPropertyKeys() ) );

                tx.createNode( label ).setProperty( propertyKey, duplicatedValue );

                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                tx.createNode( label ).setProperty( propertyKey, duplicatedValue );
                tx.commit();
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
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowCreationOfUniquenessConstraintAndIndexOnSameLabelAndProperty( int denseNodeThreshold ) throws Exception
    {
        // Given
        Label label = label( "Person1" );
        String property = "name";
        var inserter = newBatchInserter( denseNodeThreshold );

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();
        var e = assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredSchemaIndex( label ).on( property ).create() );
        // Then
        assertEquals( "Index for given {label;property} already exists", e.getMessage() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowDuplicatedUniquenessConstraints( int denseNodeThreshold ) throws Exception
    {
        // Given
        Label label = label( "Person2" );
        String property = "name";
        var inserter = newBatchInserter( denseNodeThreshold );

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();
        var e = assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create() );
        assertEquals( "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}",
                e.getMessage() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotAllowDuplicatedIndexes( int denseNodeThreshold ) throws Exception
    {
        // Given
        Label label = label( "Person3" );
        String property = "name";
        var inserter = newBatchInserter( denseNodeThreshold );

        // When
        inserter.createDeferredSchemaIndex( label ).on( property ).create();
        var e = assertThrows( ConstraintViolationException.class,
            () -> inserter.createDeferredSchemaIndex( label ).on( property ).create() );
        // Then
        assertEquals( "Index for given {label;property} already exists", e.getMessage() );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void uniquenessConstraintShouldBeCheckedOnBatchInserterShutdownAndFailIfViolated( int denseNodeThreshold ) throws Exception
    {
        // Given
        Label label = label( "Foo" );
        String property = "Bar";
        String value = "Baz";

        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

        // When
        inserter.createDeferredConstraint( label ).assertPropertyIsUnique( property ).create();

        inserter.createNode( Collections.singletonMap( property, value ), label );
        inserter.createNode( Collections.singletonMap( property, value ), label );

        // Then
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try ( Transaction tx = db.beginTx() )
        {
            var schema = tx.schema();
            IndexDefinition index = schema.getIndexes( label ).iterator().next();
            String indexFailure = schema.getIndexFailure( index );
            assertThat( indexFailure, containsString( "IndexEntryConflictException" ) );
            assertThat( indexFailure, containsString( value ) );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldChangePropertiesInCurrentBatch( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        Map<String,Object> properties = map( "key1", "value1" );
        long node = inserter.createNode( properties );

        // WHEN
        properties.put( "additionalKey", "Additional value" );
        inserter.setNodeProperties( node, properties );

        // THEN
        assertEquals( properties, getNodeProperties( inserter, node ) );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldIgnoreRemovingNonExistentNodeProperty( int denseNodeThreshold ) throws Exception
    {
        // given
        var inserter = newBatchInserter( denseNodeThreshold );
        long id = inserter.createNode( Collections.emptyMap() );

        // when
        inserter.removeNodeProperty( id, "non-existent" );

        // then no exception should be thrown, this mimics GraphDatabaseService behaviour
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldIgnoreRemovingNonExistentRelationshipProperty( int denseNodeThreshold ) throws Exception
    {
        // given
        var inserter = newBatchInserter( denseNodeThreshold );
        Map<String,Object> noProperties = Collections.emptyMap();
        long nodeId1 = inserter.createNode( noProperties );
        long nodeId2 = inserter.createNode( noProperties );
        long id = inserter.createRelationship( nodeId1, nodeId2, MyRelTypes.TEST, noProperties );

        // when
        inserter.removeRelationshipProperty( id, "non-existent" );

        // then no exception should be thrown, this mimics GraphDatabaseService behaviour
        inserter.shutdown();
    }

    private Config configuration( int denseNodeThreshold )
    {

        return Config.newBuilder()
                .set( neo4j_home, testDirectory.absolutePath().toPath() )
                .set( GraphDatabaseSettings.dense_node_threshold, denseNodeThreshold )
                .build();
    }

    private BatchInserter newBatchInserter( int denseNodeThreshold ) throws Exception
    {
        return BatchInserters.inserter( databaseLayout, fs, configuration( denseNodeThreshold ) );
    }

    private BatchInserter newBatchInserterWithIndexProvider( ExtensionFactory<?> provider, IndexProviderDescriptor providerDescriptor, int denseNodeThreshold )
        throws Exception
    {
        Config configuration = configuration( denseNodeThreshold );
        configuration.set( GraphDatabaseSettings.default_schema_provider, providerDescriptor.name() );
        return BatchInserters.inserter( databaseLayout, fs, configuration, singletonList( provider ) );
    }

    private GraphDatabaseService switchToEmbeddedGraphDatabaseService( BatchInserter inserter, int denseNodeThreshold )
    {
        inserter.shutdown();
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder( databaseLayout );
        factory.setFileSystem( fs );
        managementService = factory.impermanent()
            // Shouldn't be necessary to set dense node threshold since it's a stick config
            .setConfig( configuration( denseNodeThreshold ) ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private LabelScanStore getLabelScanStore()
    {
        return new NativeLabelScanStore( pageCache, databaseLayout, fs,
                FullStoreChangeStream.EMPTY, true, new Monitors(), RecoveryCleanupWorkCollector.immediate() );
    }

    private static void assertLabelScanStoreContains( LabelScanStore labelScanStore, int labelId, long... nodes )
    {
        LabelScanReader labelScanReader = labelScanStore.newReader();
        List<Long> expectedNodeIds = Arrays.stream( nodes ).boxed().collect( Collectors.toList() );
        List<Long> actualNodeIds;
        try ( PrimitiveLongResourceIterator itr = labelScanReader.nodesWithLabel( labelId ) )
        {
            actualNodeIds = extractPrimitiveLongIteratorAsList( itr );
        }
        assertEquals( expectedNodeIds, actualNodeIds );
    }

    private static List<Long> extractPrimitiveLongIteratorAsList( PrimitiveLongResourceIterator longIterator )
    {
        List<Long> actualNodeIds = new ArrayList<>();
        while ( longIterator.hasNext() )
        {
            actualNodeIds.add( longIterator.next() );
        }
        return actualNodeIds;
    }

    private static void createRelationships( BatchInserter inserter, long node, RelationshipType relType, int out )
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

    private long dbWithIndexAndSingleIndexedNode( int denseNodeThreshold ) throws Exception
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexProvider provider = mock( IndexProvider.class );

        when( provider.getProviderDescriptor() ).thenReturn( DESCRIPTOR );
        when( provider.getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any() ) )
                .thenReturn( populator );
        when( provider.completeConfiguration( any( IndexDescriptor.class ) ) ).then( inv -> inv.getArgument( 0 ) );

        BatchInserter inserter = newBatchInserterWithIndexProvider(
                singleInstanceIndexProviderFactory( KEY, provider ), provider.getProviderDescriptor(), denseNodeThreshold );

        inserter.createDeferredSchemaIndex( label("Hacker") ).on( "handle" ).create();
        long nodeId = inserter.createNode( map( "handle", "Jakewins" ), label( "Hacker" ) );
        inserter.shutdown();
        return nodeId;
    }

    private static void setAndGet( BatchInserter inserter, Object value )
    {
        long nodeId = inserter.createNode( map( "key", value ) );
        Object readValue = inserter.getNodeProperties( nodeId ).get( "key" );
        if ( readValue.getClass().isArray() )
        {
            assertArrayEquals( (int[]) value, (int[]) readValue );
        }
        else
        {
            assertEquals( value, readValue );
        }
    }

    private static int[] intArray()
    {
        int length = 20;
        int[] array = new int[length];
        for ( int i = 0, startValue = 1 << 30; i < length; i++ )
        {
            array[i] = startValue + i;
        }
        return array;
    }

    private static Node getNodeInTx( long nodeId, GraphDatabaseService db )
    {
        try ( var tx = db.beginTx() )
        {
            return tx.getNodeById( nodeId );
        }
    }

    private static void forceFlush( BatchInserter inserter )
    {
        ((BatchInserterImpl)inserter).forceFlushChanges();
    }

    private static NeoStores getFlushedNeoStores( BatchInserter inserter )
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

    private static Iterable<String> asNames( Iterable<Label> nodeLabels )
    {
        return map( Label::name, nodeLabels );
    }

    private static Pair<Label[], Set<String>> manyLabels( int count )
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

    private static Map<String, Object> getNodeProperties( BatchInserter inserter, long nodeId )
    {
        return inserter.getNodeProperties( nodeId );
    }

    private static Map<String, Object> getRelationshipProperties( BatchInserter inserter, long relId )
    {
        return inserter.getRelationshipProperties( relId );
    }
}
