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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

public class NeoServerTest {
    private NeoServer theServer;

    private int portNo = WebTestUtils.nextAvailablePortNumber();

    @Before
    public void setup() throws IOException {       
        if (theServer != null) {
            theServer.shutdown();
            theServer = null;
        }

        Configurator configurator = configurator();

        Database db = new Database(configurator.configuration().getString("database.location"));

        WebServer webServer = webServer();

        theServer = new NeoServer(configurator, db, webServer);
    }

    private WebServer webServer() {
        WebServer webServer = new JettyWebServer();
        webServer.setPort(portNo);
        HashSet<String> packages = new HashSet<String>();
        packages.add("org.neo4j.server.web");
        webServer.addPackages(packages);
        return webServer;
    }

    private Configurator configurator() throws IOException {
        File propertyFile = ServerTestUtils.createTempPropertyFile();

        writePropertyFile(propertyFile);

        return new Configurator(propertyFile);
    }

    private void writePropertyFile(File propertyFile) throws IOException {
        FileWriter fstream = new FileWriter(propertyFile);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("database.location=");
        out.write(ServerTestUtils.createTempDir().getAbsolutePath());
        out.close();
    }

    @Test
    public void whenServerIsStartedItShouldBringUpAWebServerWithWelcomePage() throws Exception {

        theServer.start();

        ClientResponse response = Client.create().resource(theServer.webServer().getWelcomeUri()).get(ClientResponse.class);

        assertEquals(200, response.getStatus());
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("text/html"));

        theServer.shutdown();
    }

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() {
        theServer.start();

        assertNotNull(theServer.database());

        theServer.shutdown();
    }

    @Test
    public void shouldLogStartup() {
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        theServer.start();

        assertThat(appender.toString(), containsString("Starting Neo Server on port [" + portNo + "]"));

        theServer.shutdown();
    }

    @Test(expected = ClientHandlerException.class)
    public void whenServerIsShutDownTheWebServerShouldHalt() throws UniformInterfaceException, URISyntaxException {
        
        theServer.start();
        
        URI welcomeUri = theServer.webServer().getWelcomeUri();
        
        theServer.shutdown();

        Client.create().resource(welcomeUri).get(ClientResponse.class);
    }

    @Test(expected = NullPointerException.class)
    public void whenServerIsShutDownTheDatabaseShouldNotBeAvailable() {

        theServer.start();
        // Do some work
        theServer.database().beginTx().success();
        theServer.shutdown();

        theServer.database().beginTx();
    }

    @Test
    public void shouldLogShutdown() {

    }
}
