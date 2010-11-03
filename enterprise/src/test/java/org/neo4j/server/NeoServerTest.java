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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;

import com.sun.tools.javac.util.List;

public class NeoServerTest {

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception {
        NeoServer server = server();
        server.start(null);

        assertNotNull(server.database());

        server.stop();
    }

    @Test
    public void shouldLogStartup() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        NeoServer server = server();
        server.start(null);

        assertThat(appender.toString(), containsString("Started Neo Server on port [" + 7474 + "]"));

        server.stop();
    }

    @Test(expected = NullPointerException.class)
    public void whenServerIsShutDownTheDatabaseShouldNotBeAvailable() throws IOException {

        NeoServer server = server();
        server.start(null);
        // Do some work
        server.database().beginTx().success();
        server.stop();

        server.database().beginTx();
    }
    
    @Test(expected=StartupHealthCheckFailedException.class)
    public void shouldExitWhenFailedStartupHealthCheck() {
        System.clearProperty(NeoServer.NEO_CONFIG_FILE_PROPERTY);
        new NeoServer();
    }
    
    private NeoServer server() throws IOException {       
        Configurator configurator = configurator();
        Database db = new Database(configurator.configuration().getString("org.neo4j.database.location"));
        WebServer webServer = webServer();
        return new NeoServer(configurator, db, webServer);
    }

    private WebServer webServer() {
        WebServer webServer = new Jetty6WebServer();
        webServer.addJAXRSPackages(List.from(new String[] {NeoServer.REST_API_PACKAGE}), NeoServer.REST_API_PATH);
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
        out.write("org.neo4j.database.location=");
        out.write(ServerTestUtils.createTempDir().getAbsolutePath());
        out.close();
    }
}
