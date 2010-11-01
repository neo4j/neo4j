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
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;


public class NeoServerFunctionalTest {
    @Before
    public void setup() throws Exception {
        String dbDir = configurator().configuration().getString("database.location");
        FileUtils.deleteDirectory(new File(dbDir));
    }
    
    @Test
    public void serverShouldProvideAWelcomePage() {
        
        Configurator configurator = configurator();
        Database database = database();
        WebServer webServer = webServer();
        webServer.setPort(6666);
        webServer.addPackages("org.neo4j.server.web");
        
        NeoServer server = new NeoServer(configurator, database, webServer);
        server.start();
        
        Client client = Client.create();
        ClientResponse response = client.resource("http://localhost:" + webServer.getPort() + "/welcome.html").get(ClientResponse.class);
        
        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Server"), containsString("Jetty"));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
        
        server.shutdown();
    }

    @Test
    public void shouldDeployValidJAXRSClassesSpecifiedInTheConfigFile() throws URISyntaxException {
        System.setProperty("org.neo4j.server.properties", "src/functionaltest/resources/etc/neo-server/neo-server.properties");
        
        NeoServer.main(null);
        
        Client client = Client.create();
        ClientResponse petShopResponse = client.resource("http://localhost:" + NeoServer.server().webServer().getPort() + "/petshop/prices").get(ClientResponse.class);
        
        assertEquals(200, petShopResponse.getStatus());
        assertThat(petShopResponse.getEntity(String.class), containsString("dogs for a tenner"));
        
        client = Client.create();
        ClientResponse coffeeShopResponse = client.resource("http://localhost:" + NeoServer.server().webServer().getPort() + "/coffeeshop/menu").get(ClientResponse.class);
        
        assertEquals(200, coffeeShopResponse.getStatus());
        assertThat(coffeeShopResponse.getEntity(String.class), containsString("espresso for a quid"));
        
        NeoServer.server().shutdown();

    }
    
    private WebServer webServer() {
        JettyWebServer server = new JettyWebServer();
        server.setPort(configurator().configuration().getInt("webserver.port"));
        return server;
    }

    private Database database() {
        String graphStoreDirectory = configurator().configuration().getString("database.location");
        return new Database(graphStoreDirectory);
    }

    private Configurator configurator() {
        File configFile = new File("src/functionaltest/resources/etc/neo-server/neo-server.properties");
        Configurator configurator = new Configurator(configFile);
        return configurator;
    }
}
