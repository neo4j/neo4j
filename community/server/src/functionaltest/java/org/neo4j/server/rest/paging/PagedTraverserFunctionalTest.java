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
package org.neo4j.server.rest.paging;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.Database;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class PagedTraverserFunctionalTest extends ExclusiveServerTestBase
{

    private static CommunityNeoServer server;
    private static FunctionalTestHelper functionalTestHelper;

    private Node theStartNode;
    private static final String PAGED_TRAVERSE_LINK_REL = "paged_traverse";
    private static final int SHORT_LIST_LENGTH = 33;
    private static final int LONG_LIST_LENGTH = 444;

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }
    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerBuilder.server()
        .withFakeClock()
        .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void nodeRepresentationShouldHaveLinkToPagedTraverser() throws Exception {
        theStartNode = createLinkedList(SHORT_LIST_LENGTH, server.getDatabase());

        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.nodeUri(theStartNode.getId()));

        Map<String, Object> jsonMap = JsonHelper.jsonToMap(response.getEntity(String.class));

        assertNotNull(jsonMap.containsKey(PAGED_TRAVERSE_LINK_REL));
        assertThat(String.valueOf(jsonMap.get(PAGED_TRAVERSE_LINK_REL)),
                containsString("/db/data/node/" + String.valueOf(theStartNode.getId())
                        + "/paged/traverse/{returnType}{?pageSize,leaseTime}"));
    }

    /**
     * Creating a paged traverser. Paged traversers are created by +POST+-ing a
     * traversal description to the link identified by the +paged_traverser+ key
     * in a node representation. When creating a paged traverser, the same
     * options apply as for a regular traverser, meaning that +node+, +path+,
     * or +fullpath+, can be targeted.
     */
    @Documented
    @Test
    public void shouldPostATraverserWithDefaultOptionsAndReceiveTheFirstPageOfResults() throws Exception
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ResponseEntity entity = gen.get()
        .expectedType( MediaType.APPLICATION_JSON_TYPE )
        .expectedHeader( "Location" )
        .expectedStatus( 201 )
        .payload( traverserDescription() )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node" );
        assertEquals( 201, entity.response()
                .getStatus() );
        assertThat( entity.response()
                .getLocation()
                .toString(), containsString( "/db/data/node/" + theStartNode.getId() + "/paged/traverse/node/" ) );
        assertEquals( "application/json", entity.response()
                .getType()
                .toString()
                .toLowerCase() );
    }

    /**
     * Paging through the results of a paged traverser. Paged traversers hold
     * state on the server, and allow clients to page through the results of a
     * traversal. To progress to the next page of traversal results, the client
     * issues a HTTP GET request on the paged traversal URI which causes the
     * traversal to fill the next page (or partially fill it if insufficient
     * results are available).
     * 
     * Note that if a traverser expires through inactivity it will cause a 404
     * response on the next +GET+ request. Traversers' leases are renewed on
     * every successful access for the same amount of time as originally
     * specified.
     * 
     * When the paged traverser reaches the end of its results, the client can
     * expect a 404 response as the traverser is disposed by the server.
     */
    @Documented
    @Test
    public void shouldBeAbleToTraverseAllThePagesWithDefaultPageSize()
    {
        theStartNode = createLinkedList( LONG_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverser().getLocation();

        int enoughPagesToExpireTheTraverser = 3;
        for ( int i = 0; i < enoughPagesToExpireTheTraverser; i++ )
        {

            gen.get()
            .expectedType( MediaType.APPLICATION_JSON_TYPE )
            .expectedStatus( 200 )
            .payload( traverserDescription() )
            .get( traverserLocation.toString() );
        }

        JaxRsResponse response = new RestRequest(traverserLocation).get();
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldExpireTheTraverserAfterDefaultTimeoutAndGetA404Response() {
        theStartNode = createLinkedList(SHORT_LIST_LENGTH, server.getDatabase());

        JaxRsResponse postResponse = createPagedTraverser();
        assertEquals(201, postResponse.getStatus());

        FakeClock clock = (FakeClock) LeaseManagerProvider.getClock();
        final int TEN_MINUTES = 10;
        clock.forwardMinutes(TEN_MINUTES);

        JaxRsResponse getResponse = new RestRequest(postResponse.getLocation()).get();

        assertEquals(404, getResponse.getStatus() );
    }

    /**
     * Paged traverser page size. The default page size is 50 items, but
     * depending on the application larger or smaller pages sizes might be
     * appropriate. This can be set by adding a +pageSize+ query parameter.
     */
    @Documented
    @Test
    public void shouldBeAbleToTraverseAllThePagesWithNonDefaultPageSize()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverserWithPageSize( 1 ).getLocation();

        int enoughPagesToExpireTheTraverser = 12;
        for ( int i = 0; i < enoughPagesToExpireTheTraverser; i++ )
        {

            JaxRsResponse response = new RestRequest(traverserLocation).get();
            assertEquals( 200, response.getStatus() );
        }

        JaxRsResponse response = new RestRequest(traverserLocation).get();
        assertEquals( 404, response.getStatus() );
    }

    /**
     * Paged traverser timeout. The default timeout for a paged traverser is 60
     * seconds, but depending on the application larger or smaller timeouts
     * might be appropriate. This can be set by adding a +leaseTime+ query
     * parameter with the number of seconds the paged traverser should last.
     */
    @Documented
    @Test
    public void shouldExpireTraverserWithNonDefaultTimeout()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverserWithTimeoutInMinutes( 10 ).getLocation();

        ( (FakeClock) LeaseManagerProvider.getClock() ).forwardMinutes( 11 );

        JaxRsResponse response = new RestRequest(traverserLocation).get();
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldTraverseAllPagesWithANonDefaultTimeoutAndNonDefaultPageSize()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverserWithTimeoutInMinutesAndPageSize( 10, 2 ).getLocation();

        int enoughPagesToExpireTheTraverser = 6;
        for ( int i = 0; i < enoughPagesToExpireTheTraverser; i++ )
        {

            JaxRsResponse response = new RestRequest(traverserLocation).get();
            assertEquals( 200, response.getStatus() );
        }

        JaxRsResponse response = new RestRequest(traverserLocation).get();
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400OnNegativeLeaseTime()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        int negativeLeaseTime = -9;
        JaxRsResponse response = RestRequest.req().post(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                + String.valueOf( negativeLeaseTime ) , traverserDescription() );

        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400OnNegativePageSize()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        int negativePageSize = -99;
        JaxRsResponse response = RestRequest.req().post(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?pageSize="
                + String.valueOf( negativePageSize ) ,
                traverserDescription() );

        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldRespondWith200OnFirstDeletionOfTraversalAnd404Afterwards()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        JaxRsResponse response = createPagedTraverser();

        final RestRequest request = RestRequest.req();
        JaxRsResponse deleteResponse = request.delete(response.getLocation());
        assertEquals( 200, deleteResponse.getStatus() );

        deleteResponse = request.delete(response.getLocation());
        assertEquals( 404, deleteResponse.getStatus() );
    }

    private JaxRsResponse createPagedTraverserWithTimeoutInMinutesAndPageSize(final int leaseTime, final int pageSize)
    {
        String description = traverserDescription();

        return RestRequest.req().post(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                + String.valueOf( leaseTime ) + "&pageSize=" + pageSize , description );
    }

    private JaxRsResponse createPagedTraverserWithTimeoutInMinutes(final int leaseTime)
    {
        ResponseEntity responseEntity = gen.get()
        .expectedType( MediaType.APPLICATION_JSON_TYPE )
        .expectedStatus( 201 )
        .payload( traverserDescription() )
        .post( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                + String.valueOf( leaseTime ) );

        return responseEntity.response();
    }

    private JaxRsResponse createPagedTraverserWithPageSize(final int pageSize)
    {
        ResponseEntity responseEntity = gen.get()
        .expectedType( MediaType.APPLICATION_JSON_TYPE )
        .expectedStatus( 201 )
        .payload( traverserDescription() )
        .post( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?pageSize="
                + String.valueOf( pageSize ) );

        return responseEntity.response();
    }

    private JaxRsResponse createPagedTraverser()
    {

        final String uri = functionalTestHelper.nodeUri(theStartNode.getId()) + "/paged/traverse/node";
        return RestRequest.req().post(uri, traverserDescription());
    }

    private String traverserDescription()
    {
        String description = "{"
            + "\"prune_evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
            + "\"return_filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').contains('1');\"},"
            + "\"order\":\"depth_first\","
            + "\"relationships\":{\"type\":\"NEXT\",\"direction\":\"out\"}"
            + "}";

        return description;
    }

    private Node createLinkedList( final int listLength, final Database db )
    {
        Transaction tx = db.getGraph().beginTx();
        Node startNode = null;
        try
        {
            Node previous = null;
            for ( int i = 0; i < listLength; i++ )
            {
                Node current = db.getGraph().createNode();
                current.setProperty( "name", String.valueOf( i ) );

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, DynamicRelationshipType.withName( "NEXT" ) );
                }
                else
                {
                    startNode = current;
                }

                previous = current;
            }
            tx.success();
            return startNode;
        }
        finally
        {
            tx.finish();
        }
    }
}
