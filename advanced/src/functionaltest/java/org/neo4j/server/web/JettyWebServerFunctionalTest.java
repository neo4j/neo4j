package org.neo4j.server.web;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.neo4j.server.WebTestUtils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;


public class JettyWebServerFunctionalTest {
    
    @Test
    public void shouldStartServerAtSpecificPort() throws Exception {
        
        WebServer ws = new JettyWebServer();
        int portNo = WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.start();
        
        ClientResponse response = WebTestUtils.sendGetRequestTo(new URI("http://localhost:" + portNo + "/"));
        
        ws.shutdown();
        
        assertThat(response.getStatus(), greaterThan(199));
    }
    
    @Test
    public void shouldShutdownServer() throws Exception {
        WebServer ws = new JettyWebServer();
        int portNo = WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.start();
        ws.shutdown();
        
        try {
            WebTestUtils.sendGetRequestTo(new URI("http://localhost:" + portNo + "/"));
        } catch(ClientHandlerException che) {
            assertThat(che.getMessage(), containsString("Connection refused"));
        }
    }
    
    @Test
    public void shouldMountASimpleJAXRSApp() throws Exception {
        WebServer ws = new JettyWebServer();
        int portNo = WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.start();
     
        ws.addPackages(getDummyWebResourcePackage());
        
        ClientResponse response = Client.create().resource("http://localhost:" + portNo + HelloWorldWebResource.ROOT_PATH).entity("Bertrand Russell").type("text/plain").accept("text/plain").post(ClientResponse.class);
        
        ws.shutdown();
        
        assertEquals(200, response.getStatus());
        assertThat(response.getEntity(String.class), containsString("hello, Bertrand Russell"));
    }

    private Set<String> getDummyWebResourcePackage() {
        HashSet<String> result = new HashSet<String>();
        result.add("org.neo4j.server.web");
        return result;
    }
}
