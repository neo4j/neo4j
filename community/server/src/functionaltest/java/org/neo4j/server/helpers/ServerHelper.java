package org.neo4j.server.helpers;

import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;

public class ServerHelper
{
    private static NeoServerWithEmbeddedWebServer server;

    public static void cleanTheDatabase( final NeoServerWithEmbeddedWebServer server )
    {
        new Transactor( server.getDatabase().graph, new UnitOfWork()
        {

            @Override
            public void doWork()
            {
                Iterable<Node> allNodes = server.getDatabase().graph.getAllNodes();
                for ( Node n : allNodes )
                {
                    Iterable<Relationship> relationships = n.getRelationships();
                    for ( Relationship rel : relationships )
                    {
                        rel.delete();
                    }
                    if ( n.getId() != 0 )
                    { // Don't delete the reference node - tests depend on it
                      // :-(
                        n.delete();
                    }
                    else
                    { // Remove all state from the reference node instead
                        for ( String key : n.getPropertyKeys() )
                        {
                            n.removeProperty( key );
                        }
                    }
                }

            }
        } ).execute();
    }

    @SuppressWarnings( "unchecked" )
    public static NeoServerWithEmbeddedWebServer createServer() throws IOException
    {
        if ( server == null )
        {
            server = ServerBuilder.server()
                    .withRandomDatabaseDir()
                    .withAllServerModules()
                    .withPassingStartupHealthcheck()
                    .build();
            server.start();
        }
        return server;
    }
}
