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
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerFunctionalTest {

    private NeoServer server;
    private InMemoryAppender appender;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        appender = new InMemoryAppender(NeoServer.log);
        server.start();
    }

    @After
    public void stopServer() {
        server.stop();
    }

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
        assertNotNull(server.getDatabase());
    }

    @Test
    public void shouldLogStartup() throws Exception {
        assertThat(appender.toString(), containsString("Starting Neo Server on port [" + server.restApiUri().getPort() + "]"));
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

    @Ignore
    @Test
    public void shouldSurviveDoubleMounts() throws Exception {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        Client client = Client.create();
        assertFalse(server.baseUri().toString().contains("testing"));
        ClientResponse response = client.resource(server.baseUri() + "db/manage/testing").get(ClientResponse.class);
        assertThat(response.getStatus(), is(200));
        assertThat(response.toString(), containsString("dupOne"));
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
}
