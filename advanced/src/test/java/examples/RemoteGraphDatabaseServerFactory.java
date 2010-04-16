package examples;

// START SNIPPET: class
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.transports.LocalGraphDatabase;

public class RemoteGraphDatabaseServerFactory
{
    private GraphDatabaseService graphDb;
    private Map<String, IndexService> indexes = new HashMap<String, IndexService>();

    public RemoteGraphDatabaseServerFactory( GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;
    }

    public BasicGraphDatabaseServer create()
    {
        BasicGraphDatabaseServer server = new LocalGraphDatabase( graphDb );
        for ( Map.Entry<String, IndexService> entry : indexes.entrySet() )
        {
            server.registerIndexService( entry.getKey(), entry.getValue() );
        }
        return server;
    }

    public RemoteGraphDatabaseServerFactory addIndex( String id,
            IndexService service )
    {
        indexes.put( id, service );
        return this;
    }
}
// END SNIPPET: class
