package org.neo4j.server.steps;

import java.io.IOException;

import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;

public class ServerIntegrationTestFacade
{

    private static final String EXTERNAL_SERVER_URL = "http://localhost:7474/";
    private static final String USE_EXTERNAL_SERVER_KEY = "testWithExternalServer";
    
    private NeoServerWithEmbeddedWebServer server;

    public String getServerUrl()
    {
        if(isUsingExternalServer() ) {
            return EXTERNAL_SERVER_URL;
        } else {
            return server.baseUri().toString();
        }
    }

    public void ensureServerIsRunning() throws IOException
    {
        if ( !isUsingExternalServer() )
        {
            server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
            server.start();
        }
    }

    public void cleanup()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
        }
    }

    public boolean isUsingExternalServer()
    {
        return System.getProperty( USE_EXTERNAL_SERVER_KEY, "false" ) == "true";
    }

}
