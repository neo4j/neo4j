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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.After;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.rest.FunctionalTestHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerFunctionalTest {

    private NeoServerWithEmbeddedWebServer server;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void shouldDefaultToSensiblePortIfNoneSpecifiedInConfig() throws Exception {
        server = ServerBuilder.server().withPassingStartupHealthcheck().withoutWebServerPort().withRandomDatabaseDir().build();
        server.start();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper(server);

        Client client = Client.create();
        // ClientResponse response =
        // client.resource(server.webadminUri()).get(ClientResponse.class);
        ClientResponse response = client.resource(functionalTestHelper.getWebadminUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
    }

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception {
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        assertNotNull(server.getDatabase());
    }

    @Test
    public void shouldLogStartup() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServerWithEmbeddedWebServer.log);
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        assertThat(appender.toString(), containsString("Starting Neo Server on port [" + server.getWebServerPort() + "]"));
    }

    @Test
    public void shouldRedirectRootToWebadmin() throws Exception {
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();

        Client client = Client.create();
        assertFalse(server.baseUri().toString().contains("webadmin"));
        ClientResponse response = client.resource(server.baseUri()).get(ClientResponse.class);
        assertThat(response.getStatus(), is(200));
        assertThat(response.toString(), containsString("webadmin"));
    }

    @Test
    public void serverShouldProvideAWelcomePage() throws Exception {
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper(server);

        Client client = Client.create();
        ClientResponse response = client.resource(functionalTestHelper.getWebadminUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
    }

    @Test
    public void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception {
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper(server);

        ClientResponse response = Client.create().resource(functionalTestHelper.getWebadminUri()).accept(MediaType.APPLICATION_JSON_TYPE)
                .get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldLogShutdown() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServerWithEmbeddedWebServer.log);
        server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        server.stop();
        server = null; // Need this to prevent the server being stopped twice
        assertThat(appender.toString(), containsString("INFO - Successfully shutdown Neo Server on port [7474], database ["));
    }

    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException {
        int contestedPort = 9999;
        ServerSocket socket = new ServerSocket(contestedPort);

        InMemoryAppender appender = new InMemoryAppender(NeoServerWithEmbeddedWebServer.log);
        server = ServerBuilder.server().withPassingStartupHealthcheck().onPort(contestedPort).withRandomDatabaseDir().build();
        server.start();

        assertThat(appender.toString(), containsString(String.format("ERROR - Failed to start Neo Server on port [%s]", server.getWebServerPort())));
        socket.close();
    }

    @Test
    public void shouldLoadThirdPartyJaxRsClasses() throws Exception {
        server = ServerBuilder.server().withThirdPartyJaxRsPackage("org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();

        URI thirdPartyServiceUri = new URI(server.baseUri().toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT).normalize();
        String response = Client.create().resource(thirdPartyServiceUri.toString()).get(String.class);
        assertEquals("hello", response);
    }

}
