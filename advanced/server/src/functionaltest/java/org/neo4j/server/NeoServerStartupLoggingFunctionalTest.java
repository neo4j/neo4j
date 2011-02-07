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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.web.Jetty6WebServer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerStartupLoggingFunctionalTest {
    private File tempDir;

    private NeoServer server;

    private InMemoryAppender appender;

    @Before
    public void setupServer() throws IOException {
        tempDir = new File(ServerTestUtils.createTempDir().getAbsolutePath() + File.separator + "html");

        appender = new InMemoryAppender(Jetty6WebServer.log);
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();

    }

    @After
    public void stopServer() throws IOException {
        server.stop();
    }

    @Test
    public void shouldLogStartup() throws IOException, ServerStartupException {

        // Check the logs
        assertThat(appender.toString().length(), is(greaterThan(0)));

        // Check the server is alive
        Client client = Client.create();
        client.setFollowRedirects(false);
        ClientResponse response = client.resource("http://localhost:" + server.getWebServerPort() + "/").get(ClientResponse.class);
        assertThat(response.getStatus(), is(greaterThan(199)));

    }
}
