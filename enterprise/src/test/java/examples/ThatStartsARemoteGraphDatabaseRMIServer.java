package examples;

// START SNIPPET: class
import java.net.MalformedURLException;
import java.rmi.RemoteException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.transports.LocalGraphDatabase;
import org.neo4j.remote.transports.RmiTransport;

public class ThatStartsARemoteGraphDatabaseRMIServer
{
    private static final String RESOURCE_URI = "rmi://rmi-server/neo4j-graphdb";

    public static void publishServer( GraphDatabaseService neo )
            throws MalformedURLException, RemoteException
    {
        RmiTransport.register( new LocalGraphDatabase( neo ), RESOURCE_URI );
    }
}
// END SNIPPET: class
