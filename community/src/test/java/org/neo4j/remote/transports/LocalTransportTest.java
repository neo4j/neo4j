package org.neo4j.remote.transports;

import java.util.concurrent.Callable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.RemoteGraphDatabase;
import org.neo4j.remote.RemoteGraphDbTransportTestSuite;

public class LocalTransportTest extends RemoteGraphDbTransportTestSuite
{
    @Override
    protected Callable<RemoteGraphDatabase> prepareServer(
            GraphDatabaseService graphDb, IndexService index ) throws Exception
    {
        final BasicGraphDatabaseServer connection = basicServer( graphDb, index );
        return new Callable<RemoteGraphDatabase>()
        {
            public RemoteGraphDatabase call() throws Exception
            {
                return new RemoteGraphDatabase( connection );
            }
        };
    }

    @Override
    protected String createServer( GraphDatabaseService graphDb,
            IndexService index )
            throws Exception
    {
        throw new UnsupportedOperationException();
    }
}
