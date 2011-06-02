package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.ServerSocket;

import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

public class NeoServerPortConflictFunctionalTest
{

    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException
    {
        int contestedPort = 9999;
        ServerSocket socket = new ServerSocket( contestedPort );

        InMemoryAppender appender = new InMemoryAppender( NeoServerWithEmbeddedWebServer.log );
        NeoServerWithEmbeddedWebServer server = ServerBuilder.server()
                .withPassingStartupHealthcheck()
                .onPort( contestedPort )
                .withRandomDatabaseDir()
                .build();
        server.start();

        // Don't include the SEVERE string since it's
        // OS-regional-settings-specific
        assertThat(
                appender.toString(),
                containsString( String.format( ": Failed to start Neo Server on port [%s]", server.getWebServerPort() ) ) );
        socket.close();
        server.stop();
    }
}
