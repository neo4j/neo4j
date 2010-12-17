/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.server.rest.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.StorageActions.TraverserReturnType;
import org.neo4j.server.rest.web.*;
import org.neo4j.server.rest.web.PropertyValueException;

public class StorageActionsTest {

    private static final URI BASE_URI;
    static {
        try {
            BASE_URI = new URI("http://neo4j.org/");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private StorageActions actions;
    private GraphDbHelper graphdbHelper;
    private Database database;

    @Before
    public void clearDb() throws IOException {
        database = new Database(ServerTestUtils.createTempDir().getAbsolutePath());

        graphdbHelper = new GraphDbHelper(database);
        this.actions = new StorageActions(BASE_URI, database);
    }

    @After
    public void shutdownDatabase()
    {
        this.database.shutdown();
    }

    private long createNode(Map<String, Object> properties) throws DatabaseBlockedException {

        long nodeId;
        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.createNode();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            nodeId = node.getId();
            tx.success();
        } finally {
            tx.finish();
        }
        return nodeId;
    }


    @Test
    public void createdNodeShouldBeInDatabase() throws Exception {
        NodeRepresentation noderep = actions.createNode(new PropertiesMap(Collections.<String, Object> emptyMap()));

        Transaction tx = database.graph.beginTx();
        try {
            assertNotNull(database.graph.getNodeById(noderep.getId()));
        } finally {
            tx.finish();
        }
    }

    @Test
    public void nodeInDatabaseShouldBeRetreivable() throws DatabaseBlockedException {
        long nodeId = new GraphDbHelper(database).createNode();
        assertNotNull(actions.retrieveNode(nodeId));
    }

    @Test
    public void shouldBeAbleToStorePropertiesInAnExistingNode() throws DatabaseBlockedException, org.neo4j.server.rest.web.PropertyValueException
    {
        long nodeId = graphdbHelper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("baz", 17);
        actions.setNodeProperties(nodeId, new PropertiesMap(properties));

        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.getNodeById(nodeId);
            assertHasProperties(node, properties);
        } finally {
            tx.finish();
        }
    }

    @Test
    public void shouldOverwriteExistingProperties() throws DatabaseBlockedException, PropertyValueException
    {

        long nodeId;
        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.createNode();
            node.setProperty("remove me", "trash");
            nodeId = node.getId();
            tx.success();
        } finally {
            tx.finish();
        }
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("baz", 17);
        actions.setNodeProperties(nodeId, new PropertiesMap(properties));
        tx = database.graph.beginTx();
        try {
            Node node = database.graph.getNodeById(nodeId);
            assertHasProperties(node, properties);
            assertNull(node.getProperty("remove me", null));
        } finally {
            tx.finish();
        }
    }

    @Test
    public void shouldBeAbleToGetPropertiesOnNode() throws DatabaseBlockedException {

        long nodeId;
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("neo", "Thomas A. Anderson");
        properties.put("number", 15);
        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.createNode();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            nodeId = node.getId();
            tx.success();
        } finally {
            tx.finish();
        }

        Map<String, Object> readProperties = actions.getNodeProperties(nodeId).serialize();
        assertEquals(properties, readProperties);
    }

    @Test
    public void shouldRemoveNodeWithNoRelationsFromDBOnDelete() throws DatabaseBlockedException {
        long nodeId;
        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.createNode();
            nodeId = node.getId();
            tx.success();
        } finally {
            tx.finish();
        }

        int nodeCount = graphdbHelper.getNumberOfNodes();
        actions.deleteNode(nodeId);
        assertEquals(nodeCount - 1, graphdbHelper.getNumberOfNodes());
    }

    @Test
    public void shouldBeAbleToSetPropertyOnNode() throws DatabaseBlockedException {
        long nodeId = createNode(Collections.<String, Object> emptyMap());
        String key = "foo";
        Object value = "bar";
        actions.setNodeProperty(nodeId, key, value);
        assertEquals(Collections.singletonMap(key, value), graphdbHelper.getNodeProperties(nodeId));
    }

    @Test
    public void shouldBeAbleToGetPropertyOnNode() throws DatabaseBlockedException {
        String key = "foo";
        Object value = "bar";
        long nodeId = createNode(Collections.singletonMap(key, (Object) value));
        assertEquals(value, actions.getNodeProperty(nodeId, key));
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperties() throws DatabaseBlockedException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        long nodeId = createNode(properties);
        actions.removeNodeProperties(nodeId);

        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.getNodeById(nodeId);
            assertEquals(false, node.getPropertyKeys().iterator().hasNext());
            tx.success();
        } finally {
            tx.finish();
        }
    }

    @Test
    public void shouldStoreRelationshipsBetweenTwoExistingNodes() throws Exception {
        int relationshipCount = graphdbHelper.getNumberOfRelationships();
        actions.createRelationship("LOVES", graphdbHelper.createNode(), graphdbHelper.createNode(), new PropertiesMap(Collections.<String, Object> emptyMap()));
        assertEquals(relationshipCount + 1, graphdbHelper.getNumberOfRelationships());
    }

    @Test
    public void shouldStoreSuppliedPropertiesWhenCreatingRelationship() throws Exception {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("string", "value");
        properties.put("integer", 17);
        long relId = actions.createRelationship("LOVES", graphdbHelper.createNode(), graphdbHelper.createNode(), new PropertiesMap(properties)).getId();

        Transaction tx = database.graph.beginTx();
        try {
            Relationship rel = database.graph.getRelationshipById(relId);
            for (String key : rel.getPropertyKeys()) {
                assertTrue("extra property stored", properties.containsKey(key));
            }
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                assertEquals(entry.getValue(), rel.getProperty(entry.getKey()));
            }
        } finally {
            tx.finish();
        }
    }

    @Test
    public void shouldNotCreateRelationshipBetweenNonExistentNodes() throws Exception {
        long nodeId = graphdbHelper.createNode();
        PropertiesMap properties = new PropertiesMap(Collections.<String, Object> emptyMap());
        try {
            actions.createRelationship("Loves", nodeId, nodeId * 1000, properties);
            fail();
        } catch (EndNodeNotFoundException e) {
            // ok
        }
        try {
            actions.createRelationship("Loves", nodeId * 1000, nodeId, properties);
            fail();
        } catch (StartNodeNotFoundException e) {
            // ok
        }
    }

    @Test
    public void shouldNotCreateRelationshipWithSameStartAsEndNode() throws Exception {
        long nodeId = graphdbHelper.createNode();
        PropertiesMap properties = new PropertiesMap(Collections.<String, Object> emptyMap());
        try {
            actions.createRelationship("Loves", nodeId, nodeId, properties);
            fail();
        } catch (StartNodeSameAsEndNodeException e) {
            // ok
        }
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperty() throws DatabaseBlockedException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        long nodeId = createNode(properties);
        actions.removeNodeProperty(nodeId, "foo");

        Transaction tx = database.graph.beginTx();
        try {
            Node node = database.graph.getNodeById(nodeId);
            assertEquals(15, node.getProperty("number"));
            assertEquals(false, node.hasProperty("foo"));
            tx.success();
        } finally {
            tx.finish();
        }
    }

    @Test
    public void shouldReturnTrueIfNodePropertyRemoved() throws DatabaseBlockedException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        long nodeId = createNode(properties);
        assertEquals(true, actions.removeNodeProperty(nodeId, "foo"));
    }

    @Test
    public void shouldReturnFalseIfNodePropertyNotRemoved() throws DatabaseBlockedException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        long nodeId = createNode(properties);
        assertEquals(false, actions.removeNodeProperty(nodeId, "baz"));
    }

    @Test
    public void shouldBeAbleToRetrieveARelationship() throws DatabaseBlockedException {
        long relationship = graphdbHelper.createRelationship("ENJOYED");
        assertNotNull(actions.retrieveRelationship(relationship));
    }

    @Test
    public void shouldBeAbleToGetPropertiesOnRelationship() throws DatabaseBlockedException {

        long relationshipId;
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("neo", "Thomas A. Anderson");
        properties.put("number", 15);
        Transaction tx = database.graph.beginTx();
        try {
            Node startNode = database.graph.createNode();
            Node endNode = database.graph.createNode();
            Relationship relationship = startNode.createRelationshipTo(endNode, DynamicRelationshipType.withName("knows"));
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                relationship.setProperty(entry.getKey(), entry.getValue());
            }
            relationshipId = relationship.getId();
            tx.success();
        } finally {
            tx.finish();
        }

        Map<String, Object> readProperties = actions.getRelationshipProperties(relationshipId).serialize();
        assertEquals(properties, readProperties);
    }

    @Test
    public void shouldBeAbleToRetrieveASinglePropertyFromARelationship() throws DatabaseBlockedException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("neo", "Thomas A. Anderson");
        properties.put("number", 15);

        long relationshipId = graphdbHelper.createRelationship("LOVES");
        graphdbHelper.setRelationshipProperties(relationshipId, properties);

        Object relationshipProperty = actions.getRelationshipProperty(relationshipId, "foo");
        assertEquals("bar", relationshipProperty);
    }

    @Test
    public void shouldBeAbleToDeleteARelationship() throws DatabaseBlockedException {
        long relationshipId = graphdbHelper.createRelationship("LOVES");

        actions.removeRelationship(relationshipId);
        try {
            graphdbHelper.getRelationship(relationshipId);
            fail();
        } catch (NotFoundException e) {
        }
    }

    @Test
    public void shouldBeAbleToRetrieveRelationshipsFromNode() throws DatabaseBlockedException {
        long nodeId = graphdbHelper.createNode();
        graphdbHelper.createRelationship("LIKES", nodeId, graphdbHelper.createNode());
        graphdbHelper.createRelationship("LIKES", graphdbHelper.createNode(), nodeId);
        graphdbHelper.createRelationship("HATES", nodeId, graphdbHelper.createNode());

        verifyRelReps(3, actions.retrieveRelationships(nodeId, RelationshipDirection.all));
        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.in));
        verifyRelReps(2, actions.retrieveRelationships(nodeId, RelationshipDirection.out));

        verifyRelReps(3, actions.retrieveRelationships(nodeId, RelationshipDirection.all, "LIKES", "HATES"));
        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.in, "LIKES", "HATES"));
        verifyRelReps(2, actions.retrieveRelationships(nodeId, RelationshipDirection.out, "LIKES", "HATES"));

        verifyRelReps(2, actions.retrieveRelationships(nodeId, RelationshipDirection.all, "LIKES"));
        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.in, "LIKES"));
        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.out, "LIKES"));

        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.all, "HATES"));
        verifyRelReps(0, actions.retrieveRelationships(nodeId, RelationshipDirection.in, "HATES"));
        verifyRelReps(1, actions.retrieveRelationships(nodeId, RelationshipDirection.out, "HATES"));
    }

    @Test
    public void shouldNotGetAnyRelationshipsWhenRetrievingFromNodeWithoutRelationships() throws DatabaseBlockedException {
        long nodeId = graphdbHelper.createNode();

        verifyRelReps(0, actions.retrieveRelationships(nodeId, RelationshipDirection.all));
        verifyRelReps(0, actions.retrieveRelationships(nodeId, RelationshipDirection.in));
        verifyRelReps(0, actions.retrieveRelationships(nodeId, RelationshipDirection.out));
    }

    @Test
    public void shouldBeAbleToSetRelationshipProperties() throws DatabaseBlockedException, PropertyValueException
    {
        long relationshipId = graphdbHelper.createRelationship("KNOWS");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 10);
        actions.setRelationshipProperties(relationshipId, new PropertiesMap(properties));
        assertEquals(properties, graphdbHelper.getRelationshipProperties(relationshipId));
    }

    @Test
    public void shouldBeAbleToSetRelationshipProperty() throws DatabaseBlockedException {
        long relationshipId = graphdbHelper.createRelationship("KNOWS");
        String key = "foo";
        Object value = "bar";
        actions.setRelationshipProperty(relationshipId, key, value);
        assertEquals(Collections.singletonMap(key, value), graphdbHelper.getRelationshipProperties(relationshipId));
    }

    @Test
    public void shouldRemoveRelationProperties() throws DatabaseBlockedException {
        long relId = graphdbHelper.createRelationship("PAIR-PROGRAMS_WITH");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("foo", "bar");
        map.put("baz", 22);
        graphdbHelper.setRelationshipProperties(relId, map);

        actions.removeRelationshipProperties(relId);

        assertTrue(graphdbHelper.getRelationshipProperties(relId).isEmpty());
    }

    @Test
    public void shouldRemoveRelationshipProperty() throws DatabaseBlockedException {
        long relId = graphdbHelper.createRelationship("PAIR-PROGRAMS_WITH");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("foo", "bar");
        map.put("baz", 22);
        graphdbHelper.setRelationshipProperties(relId, map);

        assertTrue(actions.removeRelationshipProperty(relId, "foo"));
        assertEquals(1, graphdbHelper.getRelationshipProperties(relId).size());
    }

    private void verifyRelReps(int expectedSize, List<RelationshipRepresentation> relreps) {
        assertEquals(expectedSize, relreps.size());
        for (RelationshipRepresentation relrep : relreps) {
            RelationshipRepresentationTest.verifySerialisation(relrep.serialize());
        }
    }

    private void assertHasProperties(PropertyContainer container, Map<String, Object> properties) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            assertEquals(entry.getValue(), container.getProperty(entry.getKey()));
        }
    }

    @Test
    public void shouldBeAbleToIndexNode() throws DatabaseBlockedException {
        String key = "mykey";
        String value = "myvalue";
        long nodeId = graphdbHelper.createNode();
        assertFalse(actions.getIndexedNodes("node", key, value).iterator().hasNext());
        actions.addNodeToIndex("node", key, value, nodeId);
        assertEquals(Arrays.asList(nodeId), graphdbHelper.getIndexedNodes("node", key, value));
    }

    @Test
    public void shouldBeAbleToFulltextIndex() throws DatabaseBlockedException {
        String key = "key";
        String value = "the value with spaces";
        long nodeId = graphdbHelper.createNode();
        String indexName = "fulltext-node";
        graphdbHelper.createNodeFullTextIndex( indexName );
        assertFalse( actions.getIndexedNodes(indexName, key, value ).iterator().hasNext() );
        actions.addNodeToIndex(indexName, key, value, nodeId);
        assertEquals(Arrays.asList(nodeId), graphdbHelper.getIndexedNodes(indexName, key, value));
        assertEquals(Arrays.asList(nodeId), graphdbHelper.getIndexedNodes(indexName, key, "the value with spaces"));
        assertEquals(Arrays.asList(nodeId), graphdbHelper.queryIndexedNodes(indexName, key, "the"));
        assertEquals(Arrays.asList(nodeId), graphdbHelper.queryIndexedNodes(indexName, key, "value"));
        assertEquals(Arrays.asList(nodeId), graphdbHelper.queryIndexedNodes(indexName, key, "with"));
        assertEquals(Arrays.asList(nodeId), graphdbHelper.queryIndexedNodes(indexName, key, "spaces"));
        assertTrue(graphdbHelper.getIndexedNodes(indexName, key, "nohit").isEmpty());
    }

    @Test
    public void shouldBeAbleToGetReferenceNode() throws DatabaseBlockedException {
        NodeRepresentation rep = actions.getReferenceNode();
        actions.retrieveNode(rep.getId());
    }

    @Test
    public void shouldGetExtendedNodeRepresentationsWhenGettingFromIndex() throws DatabaseBlockedException {
        String key = "mykey3";
        String value = "value";

        long nodeId = graphdbHelper.createNode();
        graphdbHelper.addNodeToIndex("node", key, value, nodeId);
        int counter = 0;
        for (IndexedNodeRepresentation rep : actions.getIndexedNodes("node", key, value)) {
            Map<String, Object> serialized = rep.serialize();
            NodeRepresentationTest.verifySerialisation(serialized);
            assertNotNull(serialized.get("indexed"));
            counter++;
        }
        assertEquals(1, counter);
    }

    @Test
    public void shouldBeAbleToRemoveNodeFromIndex() throws DatabaseBlockedException {
        String key = "mykey2";
        String value = "myvalue";
        String value2 = "myvalue2";
        String indexName = "node";
        long nodeId = graphdbHelper.createNode();
        actions.addNodeToIndex(indexName, key, value, nodeId);
        actions.addNodeToIndex(indexName, key, value2, nodeId);
        assertEquals(1, graphdbHelper.getIndexedNodes(indexName, key, value).size());
        assertEquals(1, graphdbHelper.getIndexedNodes(indexName, key, value2).size());
        actions.removeNodeFromIndex(indexName, key, value, nodeId);
        assertEquals(0, graphdbHelper.getIndexedNodes(indexName, key, value).size());
        assertEquals(1, graphdbHelper.getIndexedNodes(indexName, key, value2).size());
        actions.removeNodeFromIndex(indexName, key, value2, nodeId);
        assertEquals(0, graphdbHelper.getIndexedNodes(indexName, key, value).size());
        assertEquals(0, graphdbHelper.getIndexedNodes(indexName, key, value2).size());
    }

    private long createBasicTraversableGraph() throws DatabaseBlockedException {
        // (Root)
        // / \
        // (Mattias) (Johan)
        // / / \
        // (Emil) (Peter) (Tobias)

        long startNode = graphdbHelper.createNode(MapUtil.map("name", "Root"));
        long child1_l1 = graphdbHelper.createNode(MapUtil.map("name", "Mattias"));
        graphdbHelper.createRelationship("knows", startNode, child1_l1);
        long child2_l1 = graphdbHelper.createNode(MapUtil.map("name", "Johan"));
        graphdbHelper.createRelationship("knows", startNode, child2_l1);
        long child1_l2 = graphdbHelper.createNode(MapUtil.map("name", "Emil"));
        graphdbHelper.createRelationship("knows", child2_l1, child1_l2);
        long child1_l3 = graphdbHelper.createNode(MapUtil.map("name", "Peter"));
        graphdbHelper.createRelationship("knows", child1_l2, child1_l3);
        long child2_l3 = graphdbHelper.createNode(MapUtil.map("name", "Tobias"));
        graphdbHelper.createRelationship("loves", child1_l2, child2_l3);
        return startNode;
    }

    private long[] createMoreComplexGraph() throws DatabaseBlockedException {
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
        graphdbHelper.createRelationship("to", a, c);
        graphdbHelper.createRelationship("to", a, d);
        graphdbHelper.createRelationship("to", c, b);
        graphdbHelper.createRelationship("to", d, e);
        graphdbHelper.createRelationship("to", b, f);
        graphdbHelper.createRelationship("to", c, f);
        graphdbHelper.createRelationship("to", f, g);
        graphdbHelper.createRelationship("to", d, g);
        graphdbHelper.createRelationship("to", e, g);
        graphdbHelper.createRelationship("to", c, g);
        return new long[] { a, g };
    }

    @Test
    public void shouldBeAbleToTraverseWithDefaultParameters() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, new HashMap<String, Object>(), TraverserReturnType.node);
        assertEquals(2, hits.size());
    }

    @Test
    public void shouldBeAbleToTraverseDepthTwo() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, MapUtil.map("max depth", 2), TraverserReturnType.node);
        assertEquals(3, hits.size());
    }

    @Test
    public void shouldBeAbleToTraverseEverything() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, MapUtil.map("return filter", MapUtil.map("language", "javascript", "body", "true;"),
                "max depth", 10), TraverserReturnType.node);
        assertEquals(6, hits.size());
        hits = actions.traverseAndCollect(startNode, MapUtil.map("return filter", MapUtil.map("language", "builtin", "name", "all"), "max depth", 10),
                TraverserReturnType.node);
        assertEquals(6, hits.size());
    }

    @Test
    public void shouldBeAbleToUseCustomReturnFilter() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, MapUtil.map("prune evaluator", MapUtil.map("language", "builtin", "name", "none"),
                "return filter", MapUtil.map("language", "javascript", "body", "position.endNode().getProperty( 'name' ).contains( 'o' )")),
                TraverserReturnType.node);
        assertEquals(3, hits.size());
    }

    @Test
    public void shouldBeAbleToTraverseWithMaxDepthAndPruneEvaluatorCombined() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, MapUtil.map("max depth", 2, "prune evaluator", MapUtil.map("language", "javascript",
                "body", "position.endNode().getProperty('name').equals('Emil')")), TraverserReturnType.node);
        assertEquals(3, hits.size());
        hits = actions.traverseAndCollect(startNode, MapUtil.map("max depth", 1, "prune evaluator", MapUtil.map("language", "javascript", "body",
                "position.endNode().getProperty('name').equals('Emil')")), TraverserReturnType.node);
        assertEquals(2, hits.size());
    }

    @Test
    public void shouldBeAbleToGetRelationshipsIfSpecified() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, new HashMap<String, Object>(), TraverserReturnType.relationship);
        for (Representation hit : hits) {
            assertEquals(RelationshipRepresentation.class, hit.getClass());
        }
    }

    @Test
    public void shouldBeAbleToGetPathsIfSpecified() throws DatabaseBlockedException {
        long startNode = createBasicTraversableGraph();
        List<Representation> hits = actions.traverseAndCollect(startNode, new HashMap<String, Object>(), TraverserReturnType.path);
        for (Representation hit : hits) {
            assertEquals(PathRepresentation.class, hit.getClass());
        }
    }

    @Test
    public void shouldBeAbleToGetShortestPaths() throws Exception {
        long[] nodes = createMoreComplexGraph();

        // /paths
        List<PathRepresentation> result = actions.findPaths(nodes[0], nodes[1], false, MapUtil.map("max depth", 2, "algorithm", "shortestPath",
                "relationships", MapUtil.map("type", "to", "direction", "out")));
        assertPaths(2, nodes, 2, result);

        // /paths {single: true}
        result = actions.findPaths(nodes[0], nodes[1], false, MapUtil.map("max depth", 2, "algorithm", "shortestPath", "relationships", MapUtil.map("type",
                "to", "direction", "out"), "single", true));
        assertPaths(1, nodes, 2, result);

        // /path
        result = actions.findPaths(nodes[0], nodes[1], true, MapUtil.map("max depth", 2, "algorithm", "shortestPath", "relationships", MapUtil.map("type",
                "to", "direction", "out")));
        assertPaths(1, nodes, 2, result);

        // /path {single: false} (has no effect)
        result = actions.findPaths(nodes[0], nodes[1], true, MapUtil.map("max depth", 2, "algorithm", "shortestPath", "relationships", MapUtil.map("type",
                "to", "direction", "out"), "single", false));
        assertPaths(1, nodes, 2, result);

        // /path {max depth: 1} (should get no hits)
        result = actions.findPaths(nodes[0], nodes[1], true, MapUtil.map("max depth", 2, "algorithm", "shortestPath", "relationships", MapUtil.map("type",
                "to", "direction", "in"), "single", false));
        assertTrue(result.isEmpty());
    }

    private void assertPaths(int numPaths, long[] nodes, int length, List<PathRepresentation> result) {
        assertEquals(numPaths, result.size());
        for (PathRepresentation path : result) {
            Map<String, Object> serialized = path.serialize();
            assertTrue(serialized.get("start").toString().endsWith("/" + nodes[0]));
            assertTrue(serialized.get("end").toString().endsWith("/" + nodes[1]));
            assertEquals(length, serialized.get("length"));
        }
    }
}
