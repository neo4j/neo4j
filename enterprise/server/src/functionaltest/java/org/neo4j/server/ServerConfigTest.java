/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.ServerBuilder.server;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class ServerConfigTest {

    private NeoServer server;
    
    @After
    public void stopServer() {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void shouldPickUpPortFromConfig() throws Exception {
        final int NON_DEFAULT_PORT = 4321;
        
        server = server().withRandomDatabaseDir().withPassingStartupHealthcheck().onPort(
                NON_DEFAULT_PORT ).build();
        server.start();

        assertEquals(NON_DEFAULT_PORT, server.getWebServerPort());

        Client client = Client.create();
        ClientResponse response = client.resource(server.baseUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
    }
    
    @Test
    public void shouldPickupRelativeUrisForWebAdminAndWebAdminRest() throws IOException {
        String webAdminDataUri = "/a/different/webadmin/data/uri/";
        String webAdminManagementUri = "/a/different/webadmin/management/uri/";
        
        server = server().withRandomDatabaseDir().withRelativeWebDataAdminUriPath(webAdminDataUri).withRelativeWebAdminUriPath(webAdminManagementUri).withPassingStartupHealthcheck().build();
        server.start();
        
        Client client = Client.create();
        ClientResponse response = client.resource("http://localhost:7474" + webAdminDataUri).accept(MediaType.TEXT_HTML).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
        
        response = client.resource("http://localhost:7474" + webAdminManagementUri).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void shouldPickupAbsoluteUrisForWebAdminAndWebAdminRest() {
        
    }
    
    @Test
    public void shouldDealWithNonNormalizedUrisForWebAdminAndWebAdminRest() {
        
    }
}
