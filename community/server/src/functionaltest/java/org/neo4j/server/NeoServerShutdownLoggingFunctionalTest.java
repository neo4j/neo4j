package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.logging.InMemoryAppender;

public class NeoServerShutdownLoggingFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        ServerHelper.cleanTheDatabase( server );
    }
    
    @Test
    public void shouldLogShutdown() throws Exception
    {
        InMemoryAppender appender = new InMemoryAppender( NeoServerWithEmbeddedWebServer.log );
        server.stop();
        assertThat( appender.toString(), containsString( "INFO: Successfully shutdown database [" ) );
    }
}
