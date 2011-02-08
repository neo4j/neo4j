package org.neo4j.backup;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.SubProcess;

public class ServerProcess extends SubProcess<ServerInterface, String> implements ServerInterface
{
    private volatile transient GraphDatabaseService db;
    
    @Override
    protected void startup( String parameter ) throws Throwable
    {
        db = new EmbeddedGraphDatabase( parameter );
    }
    
    public void awaitStarted()
    {
        while ( db == null )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
    
    @Override
    public void shutdown()
    {
        db.shutdown();
        new Thread()
        {
            public void run()
            {
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                shutdownProcess();
            }
        }.start();
    }

    protected void shutdownProcess()
    {
        super.shutdown();
    }
}
