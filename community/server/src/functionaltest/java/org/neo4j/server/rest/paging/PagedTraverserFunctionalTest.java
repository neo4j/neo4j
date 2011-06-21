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

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.Database;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class PagedTraverserFunctionalTest
{
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

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.nodeUri( theStartNode.getId() ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        assertNotNull( jsonMap.containsKey( PAGED_TRAVERSE_LINK_REL ) );
        assertThat( String.valueOf( jsonMap.get( PAGED_TRAVERSE_LINK_REL ) ),
                containsString( "/db/data/node/" + String.valueOf( theStartNode.getId() ) + "/paged/traverse/{returnType}{?pageSize,leaseTime}" ) );
    }

    @Test
    public void shouldPostATraverserWithDefaultOptionsAndReceiveTheFirstPageOfResults() throws Exception
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        ClientResponse response = createPagedTraverser();

        assertEquals( 201, response.getStatus() );
        assertThat( response.getLocation()
                .toString(), containsString( "/db/data/node/" + theStartNode.getId() + "/paged/traverse/node/" ) );
        assertEquals( "application/json", response.getType()
                .toString()
                .toLowerCase() );
    }

    @Test
    public void shouldBeAbleToTraverseAllThePagesWithDefaultPageSize()
    {
        theStartNode = createLinkedList( LONG_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverser().getLocation();

        int enoughPagesToExpireTheTraverser = 3;
        for ( int i = 0; i < enoughPagesToExpireTheTraverser; i++ )
        {

            ClientResponse response = Client.create()
                    .resource( traverserLocation )
                    .accept( MediaType.APPLICATION_JSON )
                    .get( ClientResponse.class );
            assertEquals( 200, response.getStatus() );
        }

        ClientResponse response = Client.create()
                .resource( traverserLocation )
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

        ClientResponse getResponse = Client.create()
                .resource( postResponse.getLocation() )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        assertEquals( 404, getResponse.getStatus() );
    }

    @Test
    public void shouldBeAbleToTraverseAllThePagesWithNonDefaultPageSize()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverserWithPageSize( 1 ).getLocation();

        int enoughPagesToExpireTheTraverser = 12;
        for ( int i = 0; i < enoughPagesToExpireTheTraverser; i++ )
        {

            ClientResponse response = Client.create()
                    .resource( traverserLocation )
                    .accept( MediaType.APPLICATION_JSON )
                    .get( ClientResponse.class );
            assertEquals( 200, response.getStatus() );
        }

        ClientResponse response = Client.create()
                .resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldExpireTraverserWithNonDefaultTimeout()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        URI traverserLocation = createPagedTraverserWithTimeoutInMinutes( 10 ).getLocation();

        ( (FakeClock) LeaseManagerProvider.getClock() ).forwardMinutes( 11 );

        ClientResponse response = Client.create()
                .resource( traverserLocation )
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

            ClientResponse response = Client.create()
                    .resource( traverserLocation )
                    .accept( MediaType.APPLICATION_JSON )
                    .get( ClientResponse.class );
            assertEquals( 200, response.getStatus() );
        }

        ClientResponse response = Client.create()
                .resource( traverserLocation )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400OnNegativeLeaseTime()
    {
        theStartNode = createLinkedList( SHORT_LIST_LENGTH, server.getDatabase() );

        int negativeLeaseTime = -9;
        ClientResponse response = Client.create()
                .resource(
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
        ClientResponse response = Client.create()
                .resource(
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

        ClientResponse deleteResponse = Client.create()
                .resource( response.getLocation() )
                .delete( ClientResponse.class );
        assertEquals( 200, deleteResponse.getStatus() );

        deleteResponse = Client.create()
                .resource( response.getLocation() )
                .delete( ClientResponse.class );
        assertEquals( 404, deleteResponse.getStatus() );
    }

    private ClientResponse createPagedTraverserWithTimeoutInMinutesAndPageSize( int leaseTime, int pageSize )
    {
        String description = traverserDescription();

        ClientResponse response = Client.create()
                .resource(
                        functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                                + String.valueOf( leaseTime ) + "&pageSize=" + pageSize )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
    }

    private ClientResponse createPagedTraverserWithTimeoutInMinutes( int leaseTime )
    {
        String description = traverserDescription();

        ClientResponse response = Client.create()
                .resource(
                        functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?leaseTime="
                                + String.valueOf( leaseTime ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
    }

    private ClientResponse createPagedTraverserWithPageSize( int pageSize )
    {
        String description = traverserDescription();

        ClientResponse response = Client.create()
                .resource(
                        functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node?pageSize="
                                + String.valueOf( pageSize ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
    }

    private ClientResponse createPagedTraverser()
    {

        String description = traverserDescription();

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node" )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
    }

    private String traverserDescription()
    {
        String description = "{"
                             + "\"prune evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                             + "\"return filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').contains('1');\"},"
                             + "\"order\":\"depth first\","
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
