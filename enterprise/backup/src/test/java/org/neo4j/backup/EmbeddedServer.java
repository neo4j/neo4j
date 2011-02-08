package org.neo4j.backup;

import org.neo4j.kernel.EmbeddedGraphDatabase;

public class EmbeddedServer implements ServerInterface
{
    private EmbeddedGraphDatabase db;

    public EmbeddedServer( String storeDir )
    {
        this.db = new EmbeddedGraphDatabase( storeDir );
    }
    
    public void shutdown()
    {
        db.shutdown();
    }

    public void awaitStarted()
    {
    }
}
