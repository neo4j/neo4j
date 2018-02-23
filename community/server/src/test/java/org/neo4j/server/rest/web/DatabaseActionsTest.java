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
package org.neo4j.server.rest.web;

import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.repr.IndexedEntityRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentationTest;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.rules.ExpectedException.none;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.helpers.ServerHelper.cleanTheDatabase;
import static org.neo4j.server.rest.domain.TraverserReturnType.node;
import static org.neo4j.server.rest.repr.RelationshipRepresentationTest.verifySerialisation;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.nodeUriToId;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;
import static org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection.all;
import static org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection.in;
import static org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection.out;
import static org.neo4j.time.Clocks.fakeClock;

@EnableRuleMigrationSupport
public class DatabaseActionsTest
{
    private static final Label LABEL = label( "Label" );
    private static GraphDbHelper graphdbHelper;
    private static Database database;
    private static GraphDatabaseFacade graph;
    private static DatabaseActions actions;

    @Rule
    public ExpectedException expectedException = none();

    @BeforeAll
    public static void createDb()
    {
        graph = (GraphDatabaseFacade) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( record_id_batch_size, "1" ).newGraphDatabase();
        database = new WrappedDatabase( graph );
        graphdbHelper = new GraphDbHelper( database );
        actions = new TransactionWrappedDatabaseActions( new LeaseManager( fakeClock() ), database.getGraph() );
    }

    @AfterAll
    public static void shutdownDatabase()
    {
        graph.shutdown();
    }

    @AfterEach
    public void clearDb()
    {
        cleanTheDatabase( graph );
    }

    private long createNode( Map<String,Object> properties )
    {

        long nodeId;
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().createNode( LABEL );
            for ( Map.Entry<String,Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            nodeId = node.getId();
            tx.success();
        }
        return nodeId;
    }

    @Test
    public void createdNodeShouldBeInDatabase() throws Exception
    {
        NodeRepresentation noderep = actions.createNode( emptyMap() );

        try ( Transaction tx = database.getGraph().beginTx() )
        {
            assertNotNull( database.getGraph().getNodeById( noderep.getId() ) );
        }
    }

    @Test
    public void nodeInDatabaseShouldBeRetrievable() throws NodeNotFoundException
    {
        long nodeId = new GraphDbHelper( database ).createNode();
        assertNotNull( actions.getNode( nodeId ) );
    }

    @Test
    public void shouldBeAbleToStorePropertiesInAnExistingNode() throws PropertyValueException, NodeNotFoundException
    {
        long nodeId = graphdbHelper.createNode();
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "baz", 17 );
        actions.setAllNodeProperties( nodeId, properties );

        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            assertHasProperties( node, properties );
        }
    }

    @Test
    public void shouldFailOnTryingToStoreMixedArraysAsAProperty()
    {
        assertThrows( PropertyValueException.class, () -> {
            long nodeId = graphdbHelper.createNode();
            Map<String,Object> properties = new HashMap<>();
            Object[] dodgyArray = new Object[3];
            dodgyArray[0] = 0;
            dodgyArray[1] = 1;
            dodgyArray[2] = "two";
            properties.put( "foo", dodgyArray );

            actions.setAllNodeProperties( nodeId, properties );
        } );
    }

    @Test
    public void shouldOverwriteExistingProperties() throws PropertyValueException, NodeNotFoundException
    {

        long nodeId;
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().createNode();
            node.setProperty( "remove me", "trash" );
            nodeId = node.getId();
            tx.success();
        }

        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "baz", 17 );
        actions.setAllNodeProperties( nodeId, properties );
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            assertHasProperties( node, properties );
            assertNull( node.getProperty( "remove me", null ) );
        }
    }

    @Test
    public void shouldBeAbleToGetPropertiesOnNode() throws NodeNotFoundException
    {

        long nodeId;
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "neo", "Thomas A. Anderson" );
        properties.put( "number", 15L );
        Node node;
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            node = database.getGraph().createNode();
            for ( Map.Entry<String,Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            nodeId = node.getId();
            tx.success();
        }

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( properties, serialize( actions.getAllNodeProperties( nodeId ) ) );
        }
    }

    @Test
    public void shouldRemoveNodeWithNoRelationsFromDBOnDelete()
            throws NodeNotFoundException, ConstraintViolationException
    {
        long nodeId;
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().createNode();
            nodeId = node.getId();
            tx.success();
        }

        int nodeCount = graphdbHelper.getNumberOfNodes();
        actions.deleteNode( nodeId );
        assertEquals( nodeCount - 1, graphdbHelper.getNumberOfNodes() );
    }

    @Test
    public void shouldBeAbleToSetPropertyOnNode() throws Exception
    {
        long nodeId = createNode( emptyMap() );
        String key = "foo";
        Object value = "bar";
        actions.setNodeProperty( nodeId, key, value );
        assertEquals( singletonMap( key, value ), graphdbHelper.getNodeProperties( nodeId ) );
    }

    @Test
    public void settingAnEmptyArrayShouldWorkIfOriginalEntityHasAnEmptyArrayAsWell() throws Exception
    {
        // Given
        long nodeId = createNode( map( "emptyArray", new int[]{} ) );

        // When
        actions.setNodeProperty( nodeId, "emptyArray", new ArrayList<>() );

        // Then
        try ( Transaction transaction = graph.beginTx() )
        {
            assertThat( ((List<Object>) serialize( actions.getNodeProperty( nodeId, "emptyArray" ) )).size(), is( 0 ) );
        }
    }

    @Test
    public void shouldBeAbleToGetPropertyOnNode() throws Exception
    {
        String key = "foo";
        Object value = "bar";
        long nodeId = createNode( singletonMap( key, value ) );
        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( value, serialize( actions.getNodeProperty( nodeId, key ) ) );
        }
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperties() throws Exception
    {
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        long nodeId = createNode( properties );
        actions.removeAllNodeProperties( nodeId );

        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            assertEquals( false, node.getPropertyKeys().iterator().hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldStoreRelationshipsBetweenTwoExistingNodes() throws Exception
    {
        int relationshipCount = graphdbHelper.getNumberOfRelationships();
        actions.createRelationship( graphdbHelper.createNode(), graphdbHelper.createNode(), "LOVES", emptyMap() );
        assertEquals( relationshipCount + 1, graphdbHelper.getNumberOfRelationships() );
    }

    @Test
    public void shouldStoreSuppliedPropertiesWhenCreatingRelationship() throws Exception
    {
        Map<String,Object> properties = new HashMap<>();
        properties.put( "string", "value" );
        properties.put( "integer", 17 );
        long relId = actions.createRelationship( graphdbHelper.createNode(), graphdbHelper.createNode(), "LOVES",
                properties ).getId();

        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Relationship rel = database.getGraph().getRelationshipById( relId );
            for ( String key : rel.getPropertyKeys() )
            {
                assertTrue( properties.containsKey( key ), "extra property stored" );
            }
            for ( Map.Entry<String,Object> entry : properties.entrySet() )
            {
                assertEquals( entry.getValue(), rel.getProperty( entry.getKey() ) );
            }
        }
    }

    @Test
    public void shouldNotCreateRelationshipBetweenNonExistentNodes() throws Exception
    {
        long nodeId = graphdbHelper.createNode();
        Map<String,Object> properties = emptyMap();
        try
        {
            actions.createRelationship( nodeId, nodeId * 1000, "Loves", properties );
            fail( "Failure was expected" );
        }
        catch ( EndNodeNotFoundException e )
        {
            // ok
        }
        try
        {
            actions.createRelationship( nodeId * 1000, nodeId, "Loves", properties );
            fail( "Failure was expected" );
        }
        catch ( StartNodeNotFoundException e )
        {
            // ok
        }
    }

    @Test
    public void shouldAllowCreateRelationshipWithSameStartAsEndNode() throws Exception
    {
        long nodeId = graphdbHelper.createNode();
        Map<String,Object> properties = emptyMap();
        RelationshipRepresentation rel = actions.createRelationship( nodeId, nodeId, "Loves", properties );
        assertNotNull( rel );

    }

    @Test
    public void shouldBeAbleToRemoveNodeProperty() throws Exception
    {
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        long nodeId = createNode( properties );
        actions.removeNodeProperty( nodeId, "foo" );

        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            assertEquals( 15, node.getProperty( "number" ) );
            assertEquals( false, node.hasProperty( "foo" ) );
            tx.success();
        }
    }

    @Test
    public void shouldReturnTrueIfNodePropertyRemoved() throws Exception
    {
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        long nodeId = createNode( properties );
        actions.removeNodeProperty( nodeId, "foo" );
    }

    @Test
    public void shouldReturnFalseIfNodePropertyNotRemoved()
    {
        assertThrows( NoSuchPropertyException.class, () -> {
            Map<String,Object> properties = new HashMap<>();
            properties.put( "foo", "bar" );
            properties.put( "number", 15 );
            long nodeId = createNode( properties );
            actions.removeNodeProperty( nodeId, "baz" );
        } );
    }

    @Test
    public void shouldBeAbleToRetrieveARelationship() throws Exception
    {
        long relationship = graphdbHelper.createRelationship( "ENJOYED" );
        assertNotNull( actions.getRelationship( relationship ) );
    }

    @Test
    public void shouldBeAbleToGetPropertiesOnRelationship() throws Exception
    {

        long relationshipId;
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "neo", "Thomas A. Anderson" );
        properties.put( "number", 15L );
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node startNode = database.getGraph().createNode();
            Node endNode = database.getGraph().createNode();
            Relationship relationship = startNode.createRelationshipTo( endNode, withName( "knows" ) );
            for ( Map.Entry<String,Object> entry : properties.entrySet() )
            {
                relationship.setProperty( entry.getKey(), entry.getValue() );
            }
            relationshipId = relationship.getId();
            tx.success();
        }

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( properties, serialize( actions.getAllRelationshipProperties( relationshipId ) ) );
        }
    }

    @Test
    public void shouldBeAbleToRetrieveASinglePropertyFromARelationship() throws Exception
    {
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "neo", "Thomas A. Anderson" );
        properties.put( "number", 15L );

        long relationshipId = graphdbHelper.createRelationship( "LOVES" );
        graphdbHelper.setRelationshipProperties( relationshipId, properties );

        Object relationshipProperty;
        try ( Transaction transaction = graph.beginTx() )
        {
            relationshipProperty = serialize( actions.getRelationshipProperty( relationshipId, "foo" ) );
        }
        assertEquals( "bar", relationshipProperty );
    }

    @Test
    public void shouldBeAbleToDeleteARelationship() throws Exception
    {
        long relationshipId = graphdbHelper.createRelationship( "LOVES" );

        actions.deleteRelationship( relationshipId );
        try
        {
            graphdbHelper.getRelationship( relationshipId );
            fail( "Failure was expected" );
        }
        catch ( NotFoundException ignored )
        {
        }
    }

    @Test
    public void shouldBeAbleToRetrieveRelationshipsFromNode() throws Exception
    {
        long nodeId = graphdbHelper.createNode();
        graphdbHelper.createRelationship( "LIKES", nodeId, graphdbHelper.createNode() );
        graphdbHelper.createRelationship( "LIKES", graphdbHelper.createNode(), nodeId );
        graphdbHelper.createRelationship( "HATES", nodeId, graphdbHelper.createNode() );

        try ( Transaction transaction = graph.beginTx() )
        {
            verifyRelReps( 3, actions.getNodeRelationships( nodeId, all, emptyList() ) );
            verifyRelReps( 1, actions.getNodeRelationships( nodeId, in, emptyList() ) );
            verifyRelReps( 2, actions.getNodeRelationships( nodeId, out, emptyList() ) );

            verifyRelReps( 3, actions.getNodeRelationships( nodeId, all, asList( "LIKES", "HATES" ) ) );
            verifyRelReps( 1, actions.getNodeRelationships( nodeId, in, asList( "LIKES", "HATES" ) ) );
            verifyRelReps( 2, actions.getNodeRelationships( nodeId, out, asList( "LIKES", "HATES" ) ) );

            verifyRelReps( 2, actions.getNodeRelationships( nodeId, all, asList( "LIKES" ) ) );
            verifyRelReps( 1, actions.getNodeRelationships( nodeId, in, asList( "LIKES" ) ) );
            verifyRelReps( 1, actions.getNodeRelationships( nodeId, out, asList( "LIKES" ) ) );

            verifyRelReps( 1, actions.getNodeRelationships( nodeId, all, asList( "HATES" ) ) );
            verifyRelReps( 0, actions.getNodeRelationships( nodeId, in, asList( "HATES" ) ) );
            verifyRelReps( 1, actions.getNodeRelationships( nodeId, out, asList( "HATES" ) ) );
        }
    }

    @Test
    public void shouldNotGetAnyRelationshipsWhenRetrievingFromNodeWithoutRelationships() throws Exception
    {
        long nodeId = graphdbHelper.createNode();

        try ( Transaction transaction = graph.beginTx() )
        {
            verifyRelReps( 0, actions.getNodeRelationships( nodeId, all, emptyList() ) );
            verifyRelReps( 0, actions.getNodeRelationships( nodeId, in, emptyList() ) );
            verifyRelReps( 0, actions.getNodeRelationships( nodeId, out, emptyList() ) );
        }
    }

    @Test
    public void shouldBeAbleToSetRelationshipProperties() throws Exception
    {
        long relationshipId = graphdbHelper.createRelationship( "KNOWS" );
        Map<String,Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 10 );
        actions.setAllRelationshipProperties( relationshipId, properties );
        assertEquals( properties, graphdbHelper.getRelationshipProperties( relationshipId ) );
    }

    @Test
    public void shouldBeAbleToSetRelationshipProperty() throws Exception
    {
        long relationshipId = graphdbHelper.createRelationship( "KNOWS" );
        String key = "foo";
        Object value = "bar";
        actions.setRelationshipProperty( relationshipId, key, value );
        assertEquals( singletonMap( key, value ), graphdbHelper.getRelationshipProperties( relationshipId ) );
    }

    @Test
    public void shouldRemoveRelationProperties() throws Exception
    {
        long relId = graphdbHelper.createRelationship( "PAIR-PROGRAMS_WITH" );
        Map<String,Object> map = new HashMap<>();
        map.put( "foo", "bar" );
        map.put( "baz", 22 );
        graphdbHelper.setRelationshipProperties( relId, map );

        actions.removeAllRelationshipProperties( relId );

        assertTrue( graphdbHelper.getRelationshipProperties( relId ).isEmpty() );
    }

    @Test
    public void shouldRemoveRelationshipProperty() throws Exception
    {
        long relId = graphdbHelper.createRelationship( "PAIR-PROGRAMS_WITH" );
        Map<String,Object> map = new HashMap<>();
        map.put( "foo", "bar" );
        map.put( "baz", 22 );
        graphdbHelper.setRelationshipProperties( relId, map );

        actions.removeRelationshipProperty( relId, "foo" );
        assertEquals( 1, graphdbHelper.getRelationshipProperties( relId ).size() );
    }

    @SuppressWarnings( "unchecked" )
    private void verifyRelReps( int expectedSize, ListRepresentation repr )
    {
        List<Object> relreps = serialize( repr );
        assertEquals( expectedSize, relreps.size() );
        for ( Object relrep : relreps )
        {
            verifySerialisation( (Map<String,Object>) relrep );
        }
    }

    private void assertHasProperties( PropertyContainer container, Map<String,Object> properties )
    {
        for ( Map.Entry<String,Object> entry : properties.entrySet() )
        {
            assertEquals( entry.getValue(), container.getProperty( entry.getKey() ) );
        }
    }

    @Test
    public void shouldBeAbleToIndexNode()
    {
        String key = "mykey";
        String value = "myvalue";
        long nodeId = graphdbHelper.createNode();
        String indexName = "node";

        actions.createNodeIndex( map( "name", indexName ) );

        List<Object> listOfIndexedNodes;
        try ( Transaction transaction = graph.beginTx() )
        {
            listOfIndexedNodes = serialize( actions.getIndexedNodes( indexName, key, value ) );
        }
        assertFalse( listOfIndexedNodes.iterator().hasNext() );
        actions.addToNodeIndex( indexName, key, value, nodeId );
        assertEquals( asList( nodeId ), graphdbHelper.getIndexedNodes( indexName, key, value ) );
    }

    @Test
    public void shouldBeAbleToFulltextIndex()
    {
        String key = "key";
        String value = "the value with spaces";
        long nodeId = graphdbHelper.createNode();
        String indexName = "fulltext-node";
        graphdbHelper.createNodeFullTextIndex( indexName );
        try ( Transaction transaction = graph.beginTx() )
        {
            assertFalse( serialize( actions.getIndexedNodes( indexName, key, value ) ).iterator().hasNext() );
        }
        actions.addToNodeIndex( indexName, key, value, nodeId );
        assertEquals( asList( nodeId ), graphdbHelper.getIndexedNodes( indexName, key, value ) );
        assertEquals( asList( nodeId ), graphdbHelper.getIndexedNodes( indexName, key, "the value with spaces" ) );
        assertEquals( asList( nodeId ), graphdbHelper.queryIndexedNodes( indexName, key, "the" ) );
        assertEquals( asList( nodeId ), graphdbHelper.queryIndexedNodes( indexName, key, "value" ) );
        assertEquals( asList( nodeId ), graphdbHelper.queryIndexedNodes( indexName, key, "with" ) );
        assertEquals( asList( nodeId ), graphdbHelper.queryIndexedNodes( indexName, key, "spaces" ) );
        assertEquals( asList( nodeId ), graphdbHelper.queryIndexedNodes( indexName, key, "*spaces*" ) );
        assertTrue( graphdbHelper.getIndexedNodes( indexName, key, "nohit" ).isEmpty() );
    }

    @Test
    public void shouldGetExtendedNodeRepresentationsWhenGettingFromIndex()
    {
        String key = "mykey3";
        String value = "value";

        long nodeId = graphdbHelper.createNode( LABEL );
        String indexName = "node";
        graphdbHelper.addNodeToIndex( indexName, key, value, nodeId );
        int counter = 0;

        List<Object> indexedNodes;
        try ( Transaction transaction = graph.beginTx() )
        {
            indexedNodes = serialize( actions.getIndexedNodes( indexName, key, value ) );
        }

        for ( Object indexedNode : indexedNodes )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,Object> serialized = (Map<String,Object>) indexedNode;
            NodeRepresentationTest.verifySerialisation( serialized );
            assertNotNull( serialized.get( "indexed" ) );
            counter++;
        }
        assertEquals( 1, counter );
    }

    @Test
    public void shouldBeAbleToRemoveNodeFromIndex()
    {
        String key = "mykey2";
        String value = "myvalue";
        String value2 = "myvalue2";
        String indexName = "node";
        long nodeId = graphdbHelper.createNode();
        actions.addToNodeIndex( indexName, key, value, nodeId );
        actions.addToNodeIndex( indexName, key, value2, nodeId );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key, value ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key, value2 ).size() );
        actions.removeFromNodeIndex( indexName, key, value, nodeId );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key, value ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key, value2 ).size() );
        actions.removeFromNodeIndex( indexName, key, value2, nodeId );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key, value ).size() );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key, value2 ).size() );
    }

    @Test
    public void shouldBeAbleToRemoveNodeFromIndexWithoutKeyValue()
    {
        String key1 = "kvkey1";
        String key2 = "kvkey2";
        String value = "myvalue";
        String value2 = "myvalue2";
        String indexName = "node";
        long nodeId = graphdbHelper.createNode();
        actions.addToNodeIndex( indexName, key1, value, nodeId );
        actions.addToNodeIndex( indexName, key1, value2, nodeId );
        actions.addToNodeIndex( indexName, key2, value, nodeId );
        actions.addToNodeIndex( indexName, key2, value2, nodeId );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key1, value ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key2, value ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key2, value2 ).size() );
        actions.removeFromNodeIndexNoValue( indexName, key1, nodeId );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key1, value ).size() );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key2, value ).size() );
        assertEquals( 1, graphdbHelper.getIndexedNodes( indexName, key2, value2 ).size() );
        actions.removeFromNodeIndexNoKeyValue( indexName, nodeId );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key1, value ).size() );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key2, value ).size() );
        assertEquals( 0, graphdbHelper.getIndexedNodes( indexName, key2, value2 ).size() );
    }

    private long createBasicTraversableGraph()
    {
        // (Root)
        // / \
        // (Mattias) (Johan)
        // / / \
        // (Emil) (Peter) (Tobias)

        long startNode = graphdbHelper.createNode( map( "name", "Root" ), LABEL );
        long child1_l1 = graphdbHelper.createNode( map( "name", "Mattias" ), LABEL );
        graphdbHelper.createRelationship( "knows", startNode, child1_l1 );
        long child2_l1 = graphdbHelper.createNode( map( "name", "Johan" ), LABEL );
        graphdbHelper.createRelationship( "knows", startNode, child2_l1 );
        long child1_l2 = graphdbHelper.createNode( map( "name", "Emil" ), LABEL );
        graphdbHelper.createRelationship( "knows", child2_l1, child1_l2 );
        long child1_l3 = graphdbHelper.createNode( map( "name", "Peter" ), LABEL );
        graphdbHelper.createRelationship( "knows", child1_l2, child1_l3 );
        long child2_l3 = graphdbHelper.createNode( map( "name", "Tobias" ), LABEL );
        graphdbHelper.createRelationship( "loves", child1_l2, child2_l3 );
        return startNode;
    }

    private long[] createMoreComplexGraph()
    {
        // (a)
        // / \
        // v v
        // (b)<---(c) (d)-->(e)
        // \ / \ / /
        // v v v v /
        // (f)--->(g)<----

        long a = graphdbHelper.createNode();
        long b = graphdbHelper.createNode();
        long c = graphdbHelper.createNode();
        long d = graphdbHelper.createNode();
        long e = graphdbHelper.createNode();
        long f = graphdbHelper.createNode();
        long g = graphdbHelper.createNode();
        graphdbHelper.createRelationship( "to", a, c );
        graphdbHelper.createRelationship( "to", a, d );
        graphdbHelper.createRelationship( "to", c, b );
        graphdbHelper.createRelationship( "to", d, e );
        graphdbHelper.createRelationship( "to", b, f );
        graphdbHelper.createRelationship( "to", c, f );
        graphdbHelper.createRelationship( "to", f, g );
        graphdbHelper.createRelationship( "to", d, g );
        graphdbHelper.createRelationship( "to", e, g );
        graphdbHelper.createRelationship( "to", c, g );
        return new long[]{a, g};
    }

    private void createRelationshipWithProperties( long start, long end, Map<String,Object> properties )
    {
        long rel = graphdbHelper.createRelationship( "to", start, end );
        graphdbHelper.setRelationshipProperties( rel, properties );
    }

    private long[] createDijkstraGraph( boolean includeOnes )
    {
        /* Layout:
         *                       (y)
         *                        ^
         *                        [2]  _____[1]___
         *                          \ v           |
         * (start)--[1]->(a)--[9]-->(x)<-        (e)--[2]->(f)
         *                |         ^ ^^  \       ^
         *               [1]  ---[7][5][4] -[3]  [1]
         *                v  /       | /      \  /
         *               (b)--[1]-->(c)--[1]->(d)
         */

        Map<String,Object> costOneProperties = includeOnes ? map( "cost", (double) 1 ) : map();
        long start = graphdbHelper.createNode();
        long a = graphdbHelper.createNode();
        long b = graphdbHelper.createNode();
        long c = graphdbHelper.createNode();
        long d = graphdbHelper.createNode();
        long e = graphdbHelper.createNode();
        long f = graphdbHelper.createNode();
        long x = graphdbHelper.createNode();
        long y = graphdbHelper.createNode();

        createRelationshipWithProperties( start, a, costOneProperties );
        createRelationshipWithProperties( a, x, map( "cost", (double) 9 ) );
        createRelationshipWithProperties( a, b, costOneProperties );
        createRelationshipWithProperties( b, x, map( "cost", (double) 7 ) );
        createRelationshipWithProperties( b, c, costOneProperties );
        createRelationshipWithProperties( c, x, map( "cost", (double) 5 ) );
        createRelationshipWithProperties( c, x, map( "cost", (double) 4 ) );
        createRelationshipWithProperties( c, d, costOneProperties );
        createRelationshipWithProperties( d, x, map( "cost", (double) 3 ) );
        createRelationshipWithProperties( d, e, costOneProperties );
        createRelationshipWithProperties( e, x, costOneProperties );
        createRelationshipWithProperties( e, f, map( "cost", (double) 2 ) );
        createRelationshipWithProperties( x, y, map( "cost", (double) 2 ) );
        return new long[]{start, x};
    }

    @Test
    public void shouldBeAbleToTraverseWithDefaultParameters()
    {
        long startNode = createBasicTraversableGraph();

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( 2, serialize( actions.traverse( startNode, new HashMap<>(),
                    TraverserReturnType.node ) ).size() );
        }
    }

    @Test
    public void shouldBeAbleToTraverseDepthTwo()
    {
        long startNode = createBasicTraversableGraph();

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( 3, serialize( actions.traverse( startNode, map( "max_depth", 2 ), node ) ).size() );
        }
    }

    @Test
    public void shouldBeAbleToTraverseEverything()
    {
        long startNode = createBasicTraversableGraph();

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( 6, serialize( actions.traverse( startNode,
                    map( "return_filter", map( "language", "javascript", "body", "true;" ), "max_depth", 10 ), node ) ).size() );
            assertEquals( 6, serialize( actions.traverse( startNode,
                    map( "return_filter", map( "language", "builtin", "name", "all" ), "max_depth", 10 ), node ) ).size() );
        }
    }

    @Test
    public void shouldBeAbleToUseCustomReturnFilter()
    {
        long startNode = createBasicTraversableGraph();

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( 3, serialize( actions.traverse( startNode,
                    map( "prune_evaluator", map( "language", "builtin", "name", "none" ), "return_filter",
                            map( "language", "javascript", "body",
                                    "position.endNode().getProperty( 'name' ).contains( 'o' )" ) ), node ) ).size() );
        }
    }

    @Test
    public void shouldBeAbleToTraverseWithMaxDepthAndPruneEvaluatorCombined()
    {
        long startNode = createBasicTraversableGraph();

        try ( Transaction transaction = graph.beginTx() )
        {
            assertEquals( 3, serialize( actions.traverse( startNode, map( "max_depth", 2, "prune_evaluator",
                    map( "language", "javascript", "body", "position.endNode().getProperty('name').equals('Emil')" ) ),
                    node ) ).size() );
            assertEquals( 2, serialize( actions.traverse( startNode, map( "max_depth", 1, "prune_evaluator",
                    map( "language", "javascript", "body", "position.endNode().getProperty('name').equals('Emil')" ) ),
                    node ) ).size() );
        }
    }

    @Test
    public void shouldBeAbleToGetRelationshipsIfSpecified()
    {
        long startNode = createBasicTraversableGraph();

        List<Object> hits;
        try ( Transaction transaction = graph.beginTx() )
        {
            hits = serialize( actions.traverse( startNode, new HashMap<>(),
                    TraverserReturnType.relationship ) );
        }

        for ( Object hit : hits )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,Object> map = (Map<String,Object>) hit;
            verifySerialisation( map );
        }
    }

    @Test
    public void shouldBeAbleToGetPathsIfSpecified()
    {
        long startNode = createBasicTraversableGraph();

        List<Object> hits;
        try ( Transaction transaction = graph.beginTx() )
        {
            hits = serialize( actions.traverse( startNode, new HashMap<>(),
                    TraverserReturnType.path ) );
        }

        for ( Object hit : hits )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,Object> map = (Map<String,Object>) hit;
            assertThat( map, hasKey( "start" ) );
            assertThat( map, hasKey( "end" ) );
            assertThat( map, hasKey( "length" ) );
        }
    }

    @Test
    public void shouldBeAbleToGetFullPathsIfSpecified()
    {
        long startNode = createBasicTraversableGraph();

        List<Object> hits;
        try ( Transaction transaction = graph.beginTx() )
        {
            hits = serialize( actions.traverse( startNode, new HashMap<>(),
                    TraverserReturnType.fullpath ) );
        }

        for ( Object hit : hits )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,Object> map = (Map<String,Object>) hit;
            @SuppressWarnings( "unchecked" )
            Collection<Object> relationships = (Collection<Object>) map.get( "relationships" );
            for ( Object rel : relationships )
            {
                @SuppressWarnings( "unchecked" )
                Map<String,Object> relationship = (Map<String,Object>) rel;
                verifySerialisation( relationship );
            }
            @SuppressWarnings( "unchecked" )
            Collection<Object> nodes = (Collection<Object>) map.get( "nodes" );
            for ( Object n : nodes )
            {
                @SuppressWarnings( "unchecked" )
                Map<String,Object> node = (Map<String,Object>) n;
                NodeRepresentationTest.verifySerialisation( node );
            }
            assertThat( map, hasKey( "start" ) );
            assertThat( map, hasKey( "end" ) );
            assertThat( map, hasKey( "length" ) );
        }
    }

    @Test
    public void shouldBeAbleToGetShortestPaths()
    {
        long[] nodes = createMoreComplexGraph();

        // /paths
        try ( Transaction transaction = graph.beginTx() )
        {
            List<Object> result = serialize( actions.findPaths( nodes[0], nodes[1],
                    map( "max_depth", 2, "algorithm", "shortestPath", "relationships",
                            map( "type", "to", "direction", "out" ) ) ) );
            assertPaths( 2, nodes, 2, result );
            // /path
            Map<String,Object> path = serialize( actions.findSinglePath( nodes[0], nodes[1],
                    map( "max_depth", 2, "algorithm", "shortestPath", "relationships",
                            map( "type", "to", "direction", "out" ) ) ) );
            assertPaths( 1, nodes, 2, asList( path ) );

            // /path {single: false} (has no effect)
            path = serialize( actions.findSinglePath( nodes[0], nodes[1],
                    map( "max_depth", 2, "algorithm", "shortestPath", "relationships",
                            map( "type", "to", "direction", "out" ), "single", false ) ) );
            assertPaths( 1, nodes, 2, asList( path ) );
        }
    }

    @Test
    public void shouldBeAbleToGetPathsUsingDijkstra()
    {
        long[] nodes = createDijkstraGraph( true );

        try ( Transaction transaction = graph.beginTx() )
        {
            // /paths
            assertPaths( 1, nodes, 6, serialize( actions.findPaths( nodes[0], nodes[1],
                    map( "algorithm", "dijkstra", "cost_property", "cost", "relationships",
                            map( "type", "to", "direction", "out" ) ) ) ) );

            // /path
            Map<String,Object> path = serialize( actions.findSinglePath( nodes[0], nodes[1],
                    map( "algorithm", "dijkstra", "cost_property", "cost", "relationships",
                            map( "type", "to", "direction", "out" ) ) ) );
            assertPaths( 1, nodes, 6, asList( path ) );
            assertEquals( 6.0d, path.get( "weight" ) );
        }
    }

    @Test
    public void shouldBeAbleToGetPathsUsingDijkstraWithDefaults()
    {
        long[] nodes = createDijkstraGraph( false );

        // /paths
        try ( Transaction transaction = graph.beginTx() )
        {
            List<Object> result = serialize( actions.findPaths( nodes[0], nodes[1],
                    map( "algorithm", "dijkstra", "cost_property", "cost", "default_cost", 1, "relationships",
                            map( "type", "to", "direction", "out" ) ) ) );
            assertPaths( 1, nodes, 6, result );

            // /path
            Map<String,Object> path = serialize( actions.findSinglePath( nodes[0], nodes[1],
                    map( "algorithm", "dijkstra", "cost_property", "cost", "default_cost", 1, "relationships",
                            map( "type", "to", "direction", "out" ) ) ) );
            assertPaths( 1, nodes, 6, asList( path ) );
            assertEquals( 6.0d, path.get( "weight" ) );
        }
    }

    @Test
    public void shouldHandleNoFoundPathsCorrectly()
    {
        assertThrows( NotFoundException.class, () -> {
            long[] nodes = createMoreComplexGraph();
            actions.findSinglePath( nodes[0], nodes[1],
                    map( "max_depth", 2, "algorithm", "shortestPath", "relationships",
                            map( "type", "to", "direction", "in" ), "single", false ) );
        } );
    }

    @Test
    public void shouldAddLabelToNode() throws Exception
    {
        // GIVEN
        long node = actions.createNode( null ).getId();
        Collection<String> labels = new ArrayList<>();
        String labelName = "Wonk";
        labels.add( labelName );

        // WHEN
        actions.addLabelToNode( node, labels );

        // THEN
        try ( Transaction transaction = graph.beginTx() )
        {
            Iterable<String> result = graphdbHelper.getNodeLabels( node );
            assertEquals( labelName, single( result ) );
        }
    }

    @Test
    public void shouldRemoveLabelFromNode() throws Exception
    {
        // GIVEN
        String labelName = "mylabel";
        long node = actions.createNode( null, label( labelName ) ).getId();

        // WHEN
        actions.removeLabelFromNode( node, labelName );

        // THEN
        assertEquals( 0, graphdbHelper.getLabelCount( node ) );
    }

    @Test
    public void shouldListExistingLabelsOnNode() throws Exception
    {
        // GIVEN
        long node = graphdbHelper.createNode();
        String labelName1 = "LabelOne";
        String labelName2 = "labelTwo";
        graphdbHelper.addLabelToNode( node, labelName1 );
        graphdbHelper.addLabelToNode( node, labelName2 );

        // WHEN

        List<String> labels;
        try ( Transaction transaction = graph.beginTx() )
        {
            labels = (List) serialize( actions.getNodeLabels( node ) );
        }

        // THEN
        assertEquals( asSet( labelName1, labelName2 ), Iterables.asSet( labels ) );
    }

    @Test
    public void getNodesWithLabel()
    {
        // GIVEN
        String label1 = "first";
        String label2 = "second";
        long node1 = graphdbHelper.createNode( label( label1 ) );
        long node2 = graphdbHelper.createNode( label( label1 ), label( label2 ) );
        graphdbHelper.createNode( label( label2 ) );

        // WHEN
        List<Object> representation;
        try ( Transaction transaction = graph.beginTx() )
        {
            representation = serialize( actions.getNodesWithLabel( label1, map() ) );
        }

        // THEN
        assertEquals( asSet( node1, node2 ), Iterables.asSet( Iterables.map( from -> {
            Map<?,?> nodeMap = (Map<?,?>) from;
            return nodeUriToId( (String) nodeMap.get( "self" ) );
        }, representation ) ) );
    }

    @Test
    public void getNodesWithLabelAndSeveralPropertiesShouldFail()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // WHEN
            actions.getNodesWithLabel( "Person", map( "name", "bob", "age", 12 ) );
        } );
    }

    private void assertPaths( int numPaths, long[] nodes, int length, List<Object> result )
    {
        assertEquals( numPaths, result.size() );
        for ( Object path : result )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,Object> serialized = (Map<String,Object>) path;
            assertTrue( serialized.get( "start" ).toString().endsWith( "/" + nodes[0] ) );
            assertTrue( serialized.get( "end" ).toString().endsWith( "/" + nodes[1] ) );
            assertEquals( length, serialized.get( "length" ) );
        }
    }

    @Test
    public void shouldCreateSchemaIndex()
    {
        // GIVEN
        String labelName = "person";
        String propertyKey = "name";

        // WHEN
        actions.createSchemaIndex( labelName, asList( propertyKey ) );

        // THEN
        try ( Transaction transaction = graph.beginTx() )
        {
            Iterable<IndexDefinition> defs = graphdbHelper.getSchemaIndexes( labelName );
            assertEquals( 1, count( defs ) );
            assertEquals( propertyKey, firstOrNull( firstOrNull( defs ).getPropertyKeys() ) );
        }
    }

    @Test
    public void shouldDropSchemaIndex()
    {
        // GIVEN
        String labelName = "user";
        String propertyKey = "login";
        IndexDefinition index = graphdbHelper.createSchemaIndex( labelName, propertyKey );

        // WHEN
        actions.dropSchemaIndex( labelName, propertyKey );

        // THEN
        try ( Transaction transaction = graph.beginTx() )
        {
            assertFalse( Iterables.asSet( graphdbHelper.getSchemaIndexes( labelName ) ).contains( index ),
                    "Index should have been dropped" );
        }
    }

    @Test
    public void shouldGetSchemaIndexes()
    {
        // GIVEN
        String labelName = "mylabel";
        String propertyKey = "name";
        graphdbHelper.createSchemaIndex( labelName, propertyKey );

        // WHEN
        List<Object> serialized;
        try ( Transaction transaction = graph.beginTx() )
        {
            serialized = serialize( actions.getSchemaIndexes( labelName ) );
        }

        // THEN
        assertEquals( 1, serialized.size() );
        Map<?,?> definition = (Map<?,?>) serialized.get( 0 );
        assertEquals( labelName, definition.get( "label" ) );
        assertEquals( asList( propertyKey ), definition.get( "property_keys" ) );
    }

    @Test
    public void shouldCreatePropertyUniquenessConstraint()
    {
        // GIVEN
        String labelName = "person";
        String propertyKey = "name";

        // WHEN
        actions.createPropertyUniquenessConstraint( labelName, asList( propertyKey ) );

        // THEN
        try ( Transaction tx = graph.beginTx() )
        {
            Iterable<ConstraintDefinition> defs = graphdbHelper.getPropertyUniquenessConstraints( labelName, propertyKey );
            assertEquals( asSet( propertyKey ), Iterables.asSet( single( defs ).getPropertyKeys() ) );
            tx.success();
        }
    }

    @Test
    public void shouldDropPropertyUniquenessConstraint()
    {
        // GIVEN
        String labelName = "user";
        String propertyKey = "login";
        ConstraintDefinition index = graphdbHelper.createPropertyUniquenessConstraint( labelName, asList( propertyKey ) );

        // WHEN
        actions.dropPropertyUniquenessConstraint( labelName, asList( propertyKey ) );

        // THEN
        assertFalse( Iterables.asSet( graphdbHelper.getPropertyUniquenessConstraints( labelName, propertyKey ) )
                .contains( index ), "Constraint should have been dropped" );
    }

    @Test
    public void dropNonExistentConstraint()
    {
        // GIVEN
        String labelName = "user";
        String propertyKey = "login";
        ConstraintDefinition constraint = graphdbHelper.createPropertyUniquenessConstraint( labelName, asList( propertyKey ) );

        // EXPECT
        expectedException.expect( ConstraintViolationException.class );

        // WHEN
        try ( Transaction tx = graph.beginTx() )
        {
            constraint.drop();
            constraint.drop();
        }
    }

    @Test
    public void shouldGetPropertyUniquenessConstraint()
    {
        // GIVEN
        String labelName = "mylabel";
        String propertyKey = "name";
        graphdbHelper.createPropertyUniquenessConstraint( labelName, asList( propertyKey ) );

        // WHEN
        List<Object> serialized;
        try ( Transaction transaction = graph.beginTx() )
        {
            serialized = serialize( actions.getPropertyUniquenessConstraint( labelName, asList( propertyKey ) ) );
        }

        // THEN
        assertEquals( 1, serialized.size() );
        Map<?,?> definition = (Map<?,?>) serialized.get( 0 );
        assertEquals( labelName, definition.get( "label" ) );
        assertEquals( asList( propertyKey ), definition.get( "property_keys" ) );
        assertEquals( UNIQUENESS.name(), definition.get( "type" ) );
    }

    @Test
    public void shouldIndexNodeOnlyOnce() throws Exception
    {
        long nodeId = graphdbHelper.createNode();
        graphdbHelper.createRelationshipIndex( "myIndex" );

        try ( Transaction tx = graph.beginTx() )
        {
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedNode( "myIndex", "foo", "bar", nodeId, null );

            assertThat( result.other(), is( true ) );
            assertThat( serialize( actions.getIndexedNodes( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.nodeIsIndexed( "myIndex", "foo", "bar", nodeId ), is( true ) );

            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedNode( "myIndex", "foo", "bar", nodeId, null );

            assertThat( result.other(), is( false ) );
            assertThat( serialize( actions.getIndexedNodes( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.nodeIsIndexed( "myIndex", "foo", "bar", nodeId ), is( true ) );

            tx.success();
        }
    }

    @Test
    public void shouldIndexRelationshipOnlyOnce() throws Exception
    {
        long relationshipId = graphdbHelper.createRelationship( "FOO" );
        graphdbHelper.createRelationshipIndex( "myIndex" );

        try ( Transaction tx = graph.beginTx() )
        {
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedRelationship( "myIndex", "foo", "bar", relationshipId, null, null, null,
                            null );

            assertThat( result.other(), is( true ) );
            assertThat( serialize( actions.getIndexedRelationships( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.relationshipIsIndexed( "myIndex", "foo", "bar", relationshipId ), is( true ) );

            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedRelationship( "myIndex", "foo", "bar", relationshipId, null, null, null,
                            null );

            assertThat( result.other(), is( false ) );
            assertThat( serialize( actions.getIndexedRelationships( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.relationshipIsIndexed( "myIndex", "foo", "bar", relationshipId ), is( true ) );

            tx.success();
        }
    }

    @Test
    public void shouldNotIndexNodeWhenAnotherNodeAlreadyIndexed() throws Exception
    {
        graphdbHelper.createRelationshipIndex( "myIndex" );

        try ( Transaction tx = graph.beginTx() )
        {
            long nodeId = graphdbHelper.createNode();
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedNode( "myIndex", "foo", "bar", nodeId, null );

            assertThat( result.other(), is( true ) );
            assertThat( serialize( actions.getIndexedNodes( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.nodeIsIndexed( "myIndex", "foo", "bar", nodeId ), is( true ) );

            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            long nodeId = graphdbHelper.createNode();
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedNode( "myIndex", "foo", "bar", nodeId, null );

            assertThat( result.other(), is( false ) );
            assertThat( serialize( actions.getIndexedNodes( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.nodeIsIndexed( "myIndex", "foo", "bar", nodeId ), is( false ) );

            tx.success();
        }
    }

    @Test
    public void shouldNotIndexRelationshipWhenAnotherRelationshipAlreadyIndexed() throws Exception
    {

        graphdbHelper.createRelationshipIndex( "myIndex" );

        try ( Transaction tx = graph.beginTx() )
        {
            long relationshipId = graphdbHelper.createRelationship( "FOO" );
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedRelationship( "myIndex", "foo", "bar", relationshipId, null, null, null,
                            null );

            assertThat( result.other(), is( true ) );
            assertThat( serialize( actions.getIndexedRelationships( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.relationshipIsIndexed( "myIndex", "foo", "bar", relationshipId ), is( true ) );

            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            long relationshipId = graphdbHelper.createRelationship( "FOO" );
            Pair<IndexedEntityRepresentation,Boolean> result =
                    actions.getOrCreateIndexedRelationship( "myIndex", "foo", "bar", relationshipId, null, null, null,
                            null );

            assertThat( result.other(), is( false ) );
            assertThat( serialize( actions.getIndexedRelationships( "myIndex", "foo", "bar" ) ).size(), is( 1 ) );
            assertThat( actions.relationshipIsIndexed( "myIndex", "foo", "bar", relationshipId ), is( false ) );

            tx.success();
        }
    }
}
