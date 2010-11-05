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

import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class NeoServerTest {

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception {
        NeoServer server = server();
        assertNotNull(server.database());
        server.stop();
    }

    @Test
    public void shouldLogStartup() throws Exception {
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        InMemoryAppender jettyServerAppender = new InMemoryAppender(Jetty6WebServer.log);
        NeoServer server = server();
        System.out.println(appender.toString());
        System.out.println(jettyServerAppender.toString());
        assertThat(appender.toString(), containsString("Started Neo Server on port [" + 7474 + "]"));

        server.stop();
    }

    @Test(expected = NullPointerException.class)
    public void whenServerIsShutDownTheDatabaseShouldNotBeAvailable() throws IOException {

        NeoServer server = server(); 
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
        Configurator configurator = ServerTestUtils.configurator();
        Database db = new Database(configurator.configuration().getString("org.neo4j.database.location"));
        WebServer webServer = webServer();
        NeoServer neoServer = new NeoServer(configurator, db, webServer);
        neoServer.start( null );
        return neoServer;
    }

    private WebServer webServer() {
        WebServer webServer = new Jetty6WebServer();
        //webServer.addStaticContent("html", NeoServer.WEBADMIN_PATH);
        //webServer.addJAXRSPackages(listFrom(new String[] {NeoServer.REST_API_PACKAGE}), NeoServer.REST_API_PATH);
        //webServer.addJAXRSPackages(listFrom(new String[] {NeoServer.WEB_ADMIN_REST_API_PACKAGE}), NeoServer.MANAGE_PATH);
        return webServer;
    }
    
    private List<String> listFrom(String[] strings) {
        ArrayList<String> al = new ArrayList<String>();
        
        if(strings != null) {
            for(String str : strings) {
                al.add(str);
            }
         }
        
        return al;
    }

}
