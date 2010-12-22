package org.neo4j.server.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectorTests
{
    private NeoServer server;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
    }

    @After
    public void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldRedirectRootToWebadmin() throws Exception
    {
        ClientResponse response = Client.
                create().
                resource( server.baseUri() ).
                type( MediaType.APPLICATION_JSON ).
                accept( MediaType.APPLICATION_JSON ).
                get( ClientResponse.class );

        assertThat( response.getStatus(), is( not( 404 ) ) );
    }

    @Test
    public void shouldNotRedirectTheRestOfTheWorld() throws Exception
    {
        String url = server.baseUri() + "a/different/relative/webadmin/data/uri/";

        ClientResponse response = Client.
                create().
                resource( url ).
                type( MediaType.APPLICATION_JSON ).
                accept( MediaType.APPLICATION_JSON ).
                get( ClientResponse.class );

        assertThat( response.getStatus(), is( 404 ) );
    }
}
