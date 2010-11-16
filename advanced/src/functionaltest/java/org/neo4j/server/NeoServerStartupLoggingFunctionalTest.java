package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.web.Jetty6WebServer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class NeoServerStartupLoggingFunctionalTest {
    private File tempDir;

    @Before
    public void moveHtmlFiles() throws IOException {
        tempDir = new File(ServerTestUtils.createTempDir().getAbsolutePath() + File.separator + "html");
        FileUtils.moveDirectory(new File("target/classes/html"), tempDir);
    }

    @After
    public void restoreHtmlFiles() throws IOException {
        FileUtils.moveDirectory(tempDir, new File("target/classes/html"));
    }

    @Test
    public void whenNoStaticContentAvailableServerShouldLogAndContinueGracefully() throws IOException {
        InMemoryAppender appender = new InMemoryAppender(Jetty6WebServer.log);
        final int PORT_NO = 7474;

        // Bring up a server with no static content
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectoryOnDefaultPort();

        // Check the logs
        assertThat(appender.toString(),
                containsString("ERROR - No static content available for Neo Server at port [7474], management console may not be available."));

        // Check the server is alive
        Client client = Client.create();
        client.setFollowRedirects(false);
        ClientResponse response = client.resource("http://localhost:" + PORT_NO + "/").get(ClientResponse.class);
        assertThat(response.getStatus(), is(greaterThan(199)));

        // Kill server
        ServerTestUtils.nukeServer();
    }
}
