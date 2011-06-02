package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.rest.FunctionalTestHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerPortConflictFunctionalTest
{
    
    private NeoServerWithEmbeddedWebServer server;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @After
    public void stopServer()
    {
        if(server != null) 
        {
            server.stop();
        }
    }
    
    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException
    {
        int contestedPort = 9999;
        ServerSocket socket = new ServerSocket( contestedPort );

        InMemoryAppender appender = new InMemoryAppender( NeoServerWithEmbeddedWebServer.log );
        server = ServerBuilder.server()
                .withPassingStartupHealthcheck()
                .onPort( contestedPort )
                .withRandomDatabaseDir()
                .build();
        server.start();

        // Don't include the SEVERE string since it's
        // OS-regional-settings-specific
        assertThat(
                appender.toString(),
                containsString( String.format( ": Failed to start Neo Server on port [%s]", server.getWebServerPort() ) ) );
        socket.close();
    }
    
    @Test
    public void shouldDefaultToSensiblePortIfNoneSpecifiedInConfig() throws Exception
    {

        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        ClientResponse response = Client.create().resource( functionalTestHelper.getWebadminUri() )
                .get( ClientResponse.class );

        assertThat( response.getStatus(), is( 200 ) );
        response.close();
    }
}
