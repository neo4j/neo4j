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
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;


public class NeoServerFunctionalTest {
    
    private static final String ORG_NEO4J_SERVER_PROPERTIES = "org.neo4j.server.properties";
    private static final String DEFAULT_PORT = "7474";
    
    @Before
    public void setup() throws Exception {
        System.setProperty(ORG_NEO4J_SERVER_PROPERTIES, "src/functionaltest/resources/etc/neo-server/neo-server.properties");
        String dbDir = configurator().configuration().getString("org.neo4j.database.location");
        FileUtils.deleteDirectory(new File(dbDir));
    }
    
    @After
    public void tearDown() {
        System.clearProperty(ORG_NEO4J_SERVER_PROPERTIES);
    }
    
    @Test
    public void whenHealthCheckFailsServerShouldNotStart() {
        NeoServer server = new NeoServer();
        server.start(null);
    }
    
    @Test
    public void shouldDefaultToSensiblePortIfNoneSpecifiedInConfig() throws Exception {
        Configurator config = configWithoutWebServerPort();
        
        Database database = database();
        WebServer webServer = new JettyWebServer();
        webServer.addPackages("org.neo4j.server.web");
        
        NeoServer server = new NeoServer(config, database, webServer);
        server.start(null);
        
        Client client = Client.create();
        ClientResponse response = client.resource("http://localhost:" + DEFAULT_PORT + "/welcome.html").get(ClientResponse.class);

        assertThat(response.getStatus(), is(200));
        assertThat(response.getEntity(String.class), containsString("Welcome"));
    }
    
    
    @Test
    public void serverShouldProvideAWelcomePage() {
        Configurator configurator = configurator();
        Database database = database();
        WebServer webServer = webServer();
        webServer.addPackages("org.neo4j.server.web");
        
        NeoServer server = new NeoServer(configurator, database, webServer);
        server.start(null);
        
        Client client = Client.create();
        ClientResponse response = client.resource("http://localhost:" + webServer.getPort() + "/welcome.html").get(ClientResponse.class);
        
        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Server"), containsString("Jetty"));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
        
        server.stop();
    }

    @Test
    public void shouldMakeJAXRSClassesSpecifiedInTheConfigFileAvailableViaHTTP() throws URISyntaxException {       
        Configurator configurator = configurator();
        Database database = database();
        WebServer webServer = webServer();
        
        
        NeoServer server = new NeoServer(configurator, database, webServer);
        server.start(null);
        
        Client client = Client.create();
        ClientResponse petShopResponse = client.resource("http://localhost:" + server.webServer().getPort() + "/petshop/prices").get(ClientResponse.class);
        
        assertEquals(200, petShopResponse.getStatus());
        assertThat(petShopResponse.getEntity(String.class), containsString("dogs for a tenner"));
        
        client = Client.create();
        ClientResponse coffeeShopResponse = client.resource("http://localhost:" + server.webServer().getPort() + "/coffeeshop/menu").get(ClientResponse.class);
        
        assertEquals(200, coffeeShopResponse.getStatus());
        assertThat(coffeeShopResponse.getEntity(String.class), containsString("espresso for a quid"));
        
        server.stop();

    }
    
    @Test
    public void shouldLogShutdown() {
        NeoServer neoServer = new NeoServer(configurator(), database(), webServer());
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        neoServer.start(null);
        neoServer.stop();
        
        assertThat(appender.toString(), containsString("INFO - Successfully shutdown Neo Server on port [5555], database [/tmp/neo/functionaltest.db]"));
    }
    
    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException {
        NeoServer s1 = new NeoServer(configurator(), database(), webServer());
        s1.start(null);
        
        Configurator conflictingConfig = portClashingConfigurator();

        
        NeoServer s2 = new NeoServer(portClashingConfigurator(), new Database(conflictingConfig.configuration().getString("org.neo4j.database.location")), webServer());
        InMemoryAppender appender = new InMemoryAppender(NeoServer.log);
        s2.start(null);
        
        assertThat(appender.toString(), containsString(String.format("ERROR - Failed to start Neo Server on port [%s]", conflictingConfig.configuration().getString("org.neo4j.webserver.port"))));
        s1.stop();
        s2.stop();
    }
    
    private WebServer webServer() {
        JettyWebServer server = new JettyWebServer();
        server.setPort(configurator().configuration().getInt("org.neo4j.webserver.port"));
        return server;
    }

    private Database database() {
        String graphStoreDirectory = configurator().configuration().getString("org.neo4j.database.location");
        return new Database(graphStoreDirectory);
    }

    private Configurator configurator() {
        File configFile = new File("src/functionaltest/resources/etc/neo-server/neo-server.properties");
        Configurator configurator = new Configurator(configFile);
        return configurator;
    }
    
    private Configurator portClashingConfigurator() throws IOException {
        File tempPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile("org.neo4j.database.location", "/tmp/neo/functionaltest-clashing.db", tempPropertyFile);
        ServerTestUtils.writePropertyToFile("org.neo4j.webserver.port", "5555", tempPropertyFile);
        ServerTestUtils.writePropertyToFile("org.neo4j.webservice.packages", "org.example.coffeeshop, org.example.petshop", tempPropertyFile);
        
        return new Configurator(tempPropertyFile);
    }
    
    private Configurator configWithoutWebServerPort() throws IOException {
        File tempPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile("org.neo4j.database.location", "/tmp/neo/no-webserver-port.db", tempPropertyFile);
        ServerTestUtils.writePropertyToFile("org.neo4j.webservice.packages", "org.neo4j.server.web", tempPropertyFile);
        
        return new Configurator(tempPropertyFile);
    }
}
