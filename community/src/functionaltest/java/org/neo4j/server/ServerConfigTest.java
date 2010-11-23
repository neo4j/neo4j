package org.neo4j.server;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.ServerBuilder.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class ServerConfigTest {
    
    private static final int NON_DEFAULT_PORT = 54321;
    
    @Before
    @After
    public void noServerRunning() {
        ServerTestUtils.nukeServer();
    }
    
    @Test
    public void shouldPickUpPortFromConfig() throws Exception {
        
        NeoServer server = server().onPort(NON_DEFAULT_PORT).build();
        server.start();
        
        assertEquals(NON_DEFAULT_PORT, server.webServerPort);
        
        Client client = Client.create();
        ClientResponse response = client.resource(server.baseUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
    }
}
