package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.HashSet;

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
        HashSet<String> packages = new HashSet<String>();
        packages.add("org.neo4j.server.web");
        webServer.addPackages(packages);
        
        NeoServer server = new NeoServer(configurator, database, webServer);
        server.start();
        
        Client client = Client.create();
        ClientResponse response = client.resource("http://localhost:" + webServer.getPort() + "/welcome.html").get(ClientResponse.class);
        
        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Server"), containsString("Jetty"));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
        
        server.shutdown();
    }

    private WebServer webServer() {
        JettyWebServer server = new JettyWebServer();
        server.setPort(WebTestUtils.nextAvailablePortNumber());
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
