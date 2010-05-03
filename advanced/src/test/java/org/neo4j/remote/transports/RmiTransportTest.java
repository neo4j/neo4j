package org.neo4j.remote.transports;

import static java.rmi.registry.LocateRegistry.createRegistry;
import static java.rmi.registry.LocateRegistry.getRegistry;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;

import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.RemoteGraphDbTransportTestSuite;

public class RmiTransportTest extends RemoteGraphDbTransportTestSuite
{
    private static final String RESOURCE_URI = "rmi://localhost/Neo4jGraphDatabase";

    @BeforeClass
    public static void setupRmiRegistry() throws Exception
    {
        try
        {
            createRegistry( Registry.REGISTRY_PORT );
        }
        catch ( RemoteException e )
        {
            getRegistry( Registry.REGISTRY_PORT );
        }
    }

    @Override
    protected String createServer( GraphDatabaseService graphDb,
            IndexService index )
            throws Exception
    {
        RmiTransport.register( basicServer( graphDb, index ), RESOURCE_URI );
        return RESOURCE_URI;
    }
}
