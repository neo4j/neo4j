package examples;

// START SNIPPET: class
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.RemoteIndexService;

public class ThatAccessesARemoteIndexService
{
    public static IndexService getIndexService( GraphDatabaseService remoteDb,
            String indexId )
    {
        return new RemoteIndexService( remoteDb, indexId );
    }
}
// END SNIPPET: class
