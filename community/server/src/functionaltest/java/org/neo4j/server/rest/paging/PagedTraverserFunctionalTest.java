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
    private int lONG_LIST_LENGTH = 1234;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerBuilder.server()
                //.withFakeClock()
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        ServerHelper.cleanTheDatabase( server );
        theStartNode = createLinkedList( lONG_LIST_LENGTH, server.getDatabase() );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void nodeRepresentationShouldHaveLinkToPagedTraverser() throws Exception
    {
        ClientResponse response = Client.create()
                .resource( functionalTestHelper.nodeUri( theStartNode.getId() ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        assertNotNull( jsonMap.containsKey( PAGED_TRAVERSE_LINK_REL ) );
        assertThat( String.valueOf( jsonMap.get( PAGED_TRAVERSE_LINK_REL ) ),
                containsString( "/db/data/node/1/paged/traverse/{returnType}{?pageSize,leaseTime}" ) );
    }

    @Test
    public void shouldPostATraverserWithDefaultOptionsAndReceiveTheFirstPageOfResults() throws Exception
    {
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
        URI traverserLocation = createPagedTraverser().getLocation();
        
        System.out.println(traverserLocation);
        
        ClientResponse response = Client.create().resource( traverserLocation ).accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
        assertEquals(200, response.getStatus());
    }

    private ClientResponse createPagedTraverser()
    {

        String description = "{"
                                 + "\"prune evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                                 + "\"return filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').contains('1');\"},"
                                 + "\"order\":\"depth first\","
                                 + "\"relationships\":{\"type\":\"NEXT\",\"direction\":\"out\"}" + "}";

        
        ClientResponse response = Client.create()
                .resource( functionalTestHelper.nodeUri( theStartNode.getId() ) + "/paged/traverse/node/" )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( description )
                .post( ClientResponse.class );
        return response;
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
