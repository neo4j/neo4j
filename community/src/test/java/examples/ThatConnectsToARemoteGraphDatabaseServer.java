package examples;

// START SNIPPET: class
import java.net.URISyntaxException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.RemoteGraphDatabase;

public class ThatConnectsToARemoteGraphDatabaseServer
{
    private static final String RESOURCE_URI = "rmi://rmi-server/neo4j-graphdb";

    public static GraphDatabaseService connect() throws URISyntaxException
    {
        return new RemoteGraphDatabase( RESOURCE_URI );
    }
}
// END SNIPPET: class
