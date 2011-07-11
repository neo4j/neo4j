/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest;

import org.junit.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestData;

import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraverserFunctionalTest
{
    private long startNode;
    private long child1_l1;
    private long child2_l1;
    private long child1_l2;
    private long child1_l3;
    private long child2_l3;

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        ServerHelper.cleanTheDatabase( server );
        createSmallGraph();
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );

    private void createSmallGraph() throws Exception
    {
        Transaction tx = server.getDatabase().graph.beginTx();
        startNode = helper.createNode( MapUtil.map( "name", "Root" ) );
        child1_l1 = helper.createNode( MapUtil.map( "name", "Mattias" ) );
        helper.createRelationship( "knows", startNode, child1_l1 );
        child2_l1 = helper.createNode( MapUtil.map( "name", "Johan" ) );
        helper.createRelationship( "knows", startNode, child2_l1 );
        child1_l2 = helper.createNode( MapUtil.map( "name", "Emil" ) );
        helper.createRelationship( "knows", child2_l1, child1_l2 );
        child1_l3 = helper.createNode( MapUtil.map( "name", "Peter" ) );
        helper.createRelationship( "knows", child1_l2, child1_l3 );
        child2_l3 = helper.createNode( MapUtil.map( "name", "Tobias" ) );
        helper.createRelationship( "loves", child1_l2, child2_l3 );
        tx.success();
        tx.finish();
    }

    private JaxRsResponse traverse(long node, String description)
    {
        return RestRequest.req().post(functionalTestHelper.nodeUri(node) + "/traverse/node", description);
    }

    @Test
    public void shouldGet404WhenTraversingFromNonExistentNode()
    {
        JaxRsResponse response = traverse(99999, "{}");
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet200WhenNoHitsFromTraversing() throws DatabaseBlockedException
    {
        long node = helper.createNode();
        JaxRsResponse response = traverse(node, "{}");
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetSomeHitsWhenTraversingWithDefaultDescription() throws PropertyValueException
    {
        JaxRsResponse response = traverse(startNode, "");
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        String entity = response.getEntity( String.class );
        expectNodes( entity, child1_l1, child2_l1 );
        response.close();
    }

    private void expectNodes( String entity, long... nodes ) throws PropertyValueException
    {
        Set<String> expected = new HashSet<String>();
        for ( long node : nodes )
        {
            expected.add( functionalTestHelper.nodeUri( node ) );
        }
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            String uri = (String) map.get( "self" );
            assertTrue( uri + " not found", expected.remove( uri ) );
        }
        assertTrue( "Expected not empty:" + expected, expected.isEmpty() );
    }

    /**
     * Traversal using a return filter.
     * 
     * In this example, the +none+ prune evaluator is used and a return filter
     * is supplied. The result is to be returned as nodes and the max depth is
     * set to 3.
     */
    @Documented
    @Test
    public void shouldGetExpectedHitsWhenTraversingWithDescription() throws PropertyValueException
    {
        List<Map<String, Object>> rels = new ArrayList<Map<String, Object>>();
        rels.add( MapUtil.map( "type", "knows", "direction", "all" ) );
        rels.add( MapUtil.map( "type", "loves", "direction", "all" ) );
        String description = JsonHelper.createJsonFrom( MapUtil.map( "order", "breadth_first", "uniqueness",
                "node_global", "prune_evaluator", MapUtil.map( "language", "builtin", "name", "none" ),
                "return_filter", MapUtil.map( "language", "javascript", "body",
                        "position.endNode().getProperty('name').toLowerCase().contains('t')" ), "relationships", rels,
                "max depth", 3 ) );
        String entity = gen.get()
                .expectedStatus( 200 )
                .payload( description )
                .post( functionalTestHelper.nodeUri( startNode ) + "/traverse/node" )
                .entity();
        expectNodes( entity, startNode, child1_l1, child1_l3, child2_l3 );
    }

    @Test
    public void shouldGet400WhenSupplyingInvalidTraverserDescriptionFormat() throws DatabaseBlockedException
    {
        long node = helper.createNode();
        JaxRsResponse response = traverse(node, "::not JSON{[ at all");
        assertEquals( Status.BAD_REQUEST.getStatusCode(), response.getStatus() );
        response.close();
    }
}
