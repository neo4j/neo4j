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

package org.neo4j.server.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraverserFunctionalTest {
    private static long startNode;
    private static long child1_l1;
    private static long child2_l1;
    private static long child1_l2;
    private static long child1_l3;
    private static long child2_l3;
    private static GraphDbHelper helper;
    public static NeoServer server;

    @BeforeClass
    public static void startServer() throws Exception {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        helper = new GraphDbHelper(server.database());
        createSmallGraph();
    }

    private static void createSmallGraph() throws Exception {
        Transaction tx = server.database().graph.beginTx();
        startNode = helper.createNode(MapUtil.map("name", "Root"));
        child1_l1 = helper.createNode(MapUtil.map("name", "Mattias"));
        helper.createRelationship("knows", startNode, child1_l1);
        child2_l1 = helper.createNode(MapUtil.map("name", "Johan"));
        helper.createRelationship("knows", startNode, child2_l1);
        child1_l2 = helper.createNode(MapUtil.map("name", "Emil"));
        helper.createRelationship("knows", child2_l1, child1_l2);
        child1_l3 = helper.createNode(MapUtil.map("name", "Peter"));
        helper.createRelationship("knows", child1_l2, child1_l3);
        child2_l3 = helper.createNode(MapUtil.map("name", "Tobias"));
        helper.createRelationship("loves", child1_l2, child2_l3);
        tx.success();
        tx.finish();
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    private ClientResponse traverse(long node, String description) {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.nodeUri(node) + "/traverse/node").accept(MediaType.APPLICATION_JSON_TYPE).entity(
                description, MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class);
        return response;
    }

    @Test
    public void shouldGet404WhenTraversingFromNonExistentNode() {
        ClientResponse response = traverse(99999, "{}");
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void shouldGet200WhenNoHitsFromTraversing() throws DatabaseBlockedException {
        long node = helper.createNode();
        ClientResponse response = traverse(node, "{}");
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void shouldGetSomeHitsWhenTraversingWithDefaultDescription() {
        ClientResponse response = traverse(startNode, "");
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        expectNodes(entity, child1_l1, child2_l1);
    }

    private void expectNodes(String entity, long... nodes) {
        Set<String> expected = new HashSet<String>();
        for (long node : nodes) {
            expected.add(FunctionalTestUtil.nodeUri(node));
        }
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue(entity);
        for (Object item : items) {
            Map<?, ?> map = (Map<?, ?>) item;
            String uri = (String) map.get("self");
            assertTrue(uri + " not found", expected.remove(uri));
        }
        assertTrue("Expected not empty:" + expected, expected.isEmpty());
    }

    @Test
    public void shouldGetExpectedHitsWhenTraversingWithDescription() {
        String description = JsonHelper.createJsonFrom(MapUtil.map("prune evaluator", MapUtil.map("language", "builtin", "name", "none"), "return filter",
                MapUtil.map("language", "javascript", "body", "position.endNode().getProperty('name').toLowerCase().contains('t')")));
        ClientResponse response = traverse(startNode, description);
        String entity = response.getEntity(String.class);
        expectNodes(entity, startNode, child1_l1, child1_l3, child2_l3);
    }

    @Test
    public void shouldGet400WhenSupplyingInvalidTraverserDescriptionFormat() throws DatabaseBlockedException {
        long node = helper.createNode();
        ClientResponse response = traverse(node, "::not JSON{[ at all");
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }
}
