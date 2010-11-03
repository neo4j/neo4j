/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.web;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.net.URI;

import org.junit.Test;
import org.neo4j.server.WebTestUtils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.tools.javac.util.List;


public class JettyWebServerFunctionalTest {
    
    @Test
    public void shouldHostWelcomePageOnStartup() throws Exception {
        
        WebServer ws = new Jetty6WebServer();
        int portNo = WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.addStaticContent(defaultStaticContentLocation(), "/content");
        ws.start();
        ClientResponse response = WebTestUtils.sendGetRequestTo(new URI(String.format("http://localhost:%d/content/welcome.html", portNo)));
        
        ws.stop();
        
        assertThat(response.getStatus(), greaterThan(199));
        assertThat(response.getStatus(), lessThan(308));
        assertThat(response.getEntity(String.class), containsString("Welcome"));
        assertThat(response.getEntity(String.class), not(containsString("Directory:")));
    }
    
    @Test
    public void shouldShutdownServer() throws Exception {
        WebServer ws = new Jetty6WebServer();
        int portNo = WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.addStaticContent(defaultStaticContentLocation(), "/content");
        ws.start();
        ws.stop();
        
        try {
            WebTestUtils.sendGetRequestTo(new URI(String.format("http://localhost:%d/content/welcome.html", portNo)));
        } catch(ClientHandlerException che) {
            assertThat(che.getMessage(), containsString("Connection refused"));
        }
    }
    
    @Test
    public void shouldMountASimpleJAXRSApp() throws Exception {
        WebServer ws = new Jetty6WebServer();
        int portNo = 5555;//WebTestUtils.nextAvailablePortNumber();
        ws.setPort(portNo);
        ws.addJAXRSPackages(List.from(new String[] { getDummyWebResourcePackage() }), "/services");
        ws.start();
     
        
        ClientResponse response = Client.create().resource("http://localhost:" + portNo + "/services" + HelloWorldWebResource.ROOT_PATH).entity("Bertrand Russell").type("text/plain").accept("text/plain").post(ClientResponse.class);
        
        ws.stop();
        
        assertEquals(200, response.getStatus());
        assertThat(response.getEntity(String.class), containsString("hello, Bertrand Russell"));
    }

    private String defaultStaticContentLocation() {
        return "html";
    }

    private String getDummyWebResourcePackage() {
        return "org.neo4j.server.web";
    }
}
