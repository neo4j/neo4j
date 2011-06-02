package org.neo4j.server;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import javax.ws.rs.core.MediaType;

import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.UnitOfWork;
import org.neo4j.server.rest.FunctionalTestHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerJAXRSFunctionalTest
{
    private static final int ROOT_NODE = 1;
    private NeoServerWithEmbeddedWebServer server;

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception
    {
        server = ServerHelper.createServer();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.getWebadminUri() )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldLoadThirdPartyJaxRsClasses() throws Exception
    {
        server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT )
                .withPassingStartupHealthcheck()
                .withAllServerModules()
                .withRandomDatabaseDir()
                .build();
        server.start();

        URI thirdPartyServiceUri = new URI( server.baseUri()
                .toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT ).normalize();
        String response = Client.create()
                .resource( thirdPartyServiceUri.toString() )
                .get( String.class );
        assertEquals( "hello", response );
    }

    @Test
    public void shouldLoadExtensionInitializers() throws Exception
    {
        server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT )
                .withPassingStartupHealthcheck()
                .withAllServerModules()
                .withRandomDatabaseDir()
                .build();
        server.start();

        int nodesCreated = createSimpleDatabase( server.getDatabase().graph );

        URI thirdPartyServiceUri = new URI( server.baseUri()
                .toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT + "/inject-test" ).normalize();
        String response = Client.create()
                .resource( thirdPartyServiceUri.toString() )
                .get( String.class );
        assertEquals( String.valueOf( nodesCreated + ROOT_NODE ), response );
    }

    private int createSimpleDatabase( final AbstractGraphDatabase graph )
    {
        final int numberOfNodes = 10;
        new Transactor( graph, new UnitOfWork()
        {

            @Override
            public void doWork()
            {
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    graph.createNode();
                }

                for ( Node n1 : graph.getAllNodes() )
                {
                    for ( Node n2 : graph.getAllNodes() )
                    {
                        if ( n1.equals( n2 ) ) continue;

                        n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "REL" ) );
                    }
                }
            }
        } ).execute();

        return numberOfNodes;
    }
}
