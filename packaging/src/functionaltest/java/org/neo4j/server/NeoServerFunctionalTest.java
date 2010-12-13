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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerFunctionalTest {

    @Test
    public void shouldDefaultToSensiblePortIfNoneSpecifiedInConfig() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withoutWebServerPort().withRandomDatabaseDir().build();
        server.start();

        Client client = Client.create();
        ClientResponse response = client.resource(server.webadminUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
        server.stop();
    }

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        assertNotNull(server.getDatabase());
        server.stop();
    }

    @Test
    public void shouldLogStartup() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        assertThat(appender.toString(), containsString("Starting Neo Server on port [" + server.restApiUri().getPort() + "]"));
        server.stop();
    }

    @Test
    public void shouldRedirectRootToWebadmin() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();

        Client client = Client.create();
        assertFalse(server.baseUri().toString().contains("webadmin"));
        ClientResponse response = client.resource(server.baseUri()).get(ClientResponse.class);
        assertThat(response.getStatus(), is(200));
        assertThat(response.toString(), containsString("webadmin"));

        server.stop();
    }

    @Test
    public void serverShouldProvideAWelcomePage() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        Client client = Client.create();
        ClientResponse response = client.resource(server.webadminUri()).get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
        server.stop();
    }

    @Test
    public void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        ClientResponse response = Client.create().resource(server.restApiUri()).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
        server.stop();
    }

    @Test
    public void shouldLogShutdown() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        server.stop();
        assertThat(appender.toString(), containsString("INFO - Successfully shutdown Neo Server on port [7474], database ["));
    }

    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException {
        int contestedPort = 9999;
        ServerSocket socket = new ServerSocket(contestedPort);

        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().onPort(contestedPort).withRandomDatabaseDir().build();
        server.start();

        assertThat(appender.toString(), containsString(String.format("ERROR - Failed to start Neo Server on port [%s]", server.restApiUri().getPort())));
        socket.close();
    }
    
    @Test
    public void shouldEmitManagementAndDataAndWebadminUrisToConsoleAtStartup() throws IOException {
        PrintStream oldOut = System.out;

        ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(consoleOutput);
        System.setOut(printStream);
        
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withDefaultDatabaseTuning().withRandomDatabaseDir().build();
        server.start();
        server.stop();
        
        printStream.flush();
        System.setOut(oldOut);
        
        String consoleString = consoleOutput.toString();
        assertThat(consoleString, containsString("Neo4j server management URI [http://localhost:7474/db/manage/]"));
        assertThat(consoleString, containsString("Neo4j server data URI [http://localhost:7474/db/data/]"));
        assertThat(consoleString, containsString("Neo4j server webadmin URI [http://localhost:7474/webadmin/]"));
    }
    
    @Test
    public void shouldLoadThirdPartyJaxRsClasses() throws Exception {
        NeoServer server = ServerBuilder.server().withThirdPartyJaxRsPackage("org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT).withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();

        URI thirdPartyServiceUri = new URI(server.baseUri().toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT).normalize();
        String response = Client.create().resource(thirdPartyServiceUri.toString()).get(String.class);
        assertEquals("hello", response);
        
        server.stop();
    }
}
