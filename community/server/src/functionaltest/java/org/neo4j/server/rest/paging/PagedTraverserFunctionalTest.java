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
package org.neo4j.server.rest.paging;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.rest.FunctionalTestHelper.CLIENT;

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
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.Database;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.DocsGenerator;
import org.neo4j.server.rest.DocsGenerator.ResponseEntity;
import org.neo4j.server.rest.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.ClientResponse;

public class PagedTraverserFunctionalTest
{
    public @Rule
    TestData<DocsGenerator> docGenerator = TestData.producedThrough( DocsGenerator.PRODUCER );

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;

    private Node theStartNode;
    private static final String PAGED_TRAVERSE_LINK_REL = "paged_traverse";
    private static final int SHORT_LIST_LENGTH = 33;
    private static final int LONG_LIST_LENGTH = 444;

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
    public void nodeRepresentationShouldHaveLinkToPagedTraverser() throws Exception
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ClientResponse response = CLIENT.resource( functionalTestHelper.nodeUri( theStartNode.getId() ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        assertNotNull( jsonMap.containsKey( PAGED_TRAVERSE_LINK_REL ) );
        assertThat( String.valueOf( jsonMap.get( PAGED_TRAVERSE_LINK_REL ) ),
                containsString( "/db/data/node/" + String.valueOf( theStartNode.getId() )
                                + "/paged/traverse/{returnType}{?pageSize,leaseTime}" ) );
    }

    /**
     * Creating a paged traverser. Paged traversers are created by +POST+-ing a
     * traversal description to the link identified by the +paged_traverser+ key
     * in a node representation. When creating a paged traverser, the same
     * options apply as for a regular traverser, meaning that +node+, 'path/',
     * or 'fullpath', can be targeted.
     */
    @Documented
    @Test
    public void shouldPostATraverserWithDefaultOptionsAndReceiveTheFirstPageOfResults() throws Exception
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ResponseEntity entity = docGenerator.get()
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

            docGenerator.get()
                    .expectedType( MediaType.APPLICATION_JSON_TYPE )
                    .expectedStatus( 200 )
                    .payload( traverserDescription() )
                    .get( traverserLocation.toString() );
        }

        ClientResponse response = CLIENT.resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldExpireTheTraverserAfterDefaultTimeoutAndGetA404Response()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ClientResponse postResponse = createPagedTraverser();
        assertEquals( 201, postResponse.getStatus() );

        FakeClock clock = (FakeClock) LeaseManagerProvider.getClock();
        final int TEN_MINUTES = 10;
        clock.forwardMinutes( TEN_MINUTES );

        ClientResponse getResponse = CLIENT.resource( postResponse.getLocation() )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        assertEquals( 404, getResponse.getStatus() );
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

            ClientResponse response = CLIENT.resource( traverserLocation )
                    .accept( MediaType.APPLICATION_JSON )
                    .get( ClientResponse.class );
            assertEquals( 200, response.getStatus() );
        }

        ClientResponse response = CLIENT.resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
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

        ClientResponse response = CLIENT.resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
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

            ClientResponse response = CLIENT.resource( traverserLocation )
                    .accept( MediaType.APPLICATION_JSON )
                    .get( ClientResponse.class );
            assertEquals( 200, response.getStatus() );
        }

        ClientResponse response = CLIENT.resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400OnNegativeLeaseTime()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        int negativeLeaseTime = -9;
        ClientResponse response = CLIENT.resource(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                        + String.valueOf( negativeLeaseTime ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( traverserDescription() )
                .post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400OnNegativePageSize()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        int negativePageSize = -99;
        ClientResponse response = CLIENT.resource(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?pageSize="
                        + String.valueOf( negativePageSize ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( traverserDescription() )
                .post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldRespondWith200OnFirstDeletionOfTraversalAnd404Afterwards()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ClientResponse response = createPagedTraverser();

        ClientResponse deleteResponse = CLIENT.resource( response.getLocation() )
                .delete( ClientResponse.class );
        assertEquals( 200, deleteResponse.getStatus() );

        deleteResponse = CLIENT.resource( response.getLocation() )
                .delete( ClientResponse.class );
        assertEquals( 404, deleteResponse.getStatus() );
    }

    private ClientResponse createPagedTraverserWithTimeoutInMinutesAndPageSize( int leaseTime, int pageSize )
    {
        String description = traverserDescription();

        ClientResponse response = CLIENT.resource(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                        + String.valueOf( leaseTime ) + "&pageSize=" + pageSize )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
    }

    private ClientResponse createPagedTraverserWithTimeoutInMinutes( int leaseTime )
    {
        ResponseEntity responseEntity = docGenerator.get()
                .expectedType( MediaType.APPLICATION_JSON_TYPE )
                .expectedStatus( 201 )
                .payload( traverserDescription() )
                .post( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                       + String.valueOf( leaseTime ) );

        return responseEntity.response();
    }

    private ClientResponse createPagedTraverserWithPageSize( int pageSize )
    {
        ResponseEntity responseEntity = docGenerator.get()
                .expectedType( MediaType.APPLICATION_JSON_TYPE )
                .expectedStatus( 201 )
                .payload( traverserDescription() )
                .post( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?pageSize="
                       + String.valueOf( pageSize ) );

        return responseEntity.response();
    }

    private ClientResponse createPagedTraverser()
    {

        ClientResponse response = CLIENT.resource(
                functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node" )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( traverserDescription() )
                .post( ClientResponse.class );
        return response;
    }

    private String traverserDescription()
    {
        String description = "{"
                             + "\"prune_evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                             + "\"return_filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').contains('1');\"},"
                             + "\"order\":\"depth_first\","
                             + "\"relationships\":{\"type\":\"NEXT\",\"direction\":\"out\"}" + "}";

        return description;
    }

    private Node createLinkedList( int listLength, Database db )
    {
        Transaction tx = db.graph.beginTx();
        Node startNode = null;
        try
        {
            Node previous = null;
            for ( int i = 0; i < listLength; i++ )
            {
                Node current = db.graph.createNode();
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
