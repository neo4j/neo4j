package org.neo4j.com;

import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.test.SubProcess;

public class MadeUpServerProcess extends SubProcess<ServerInterface, Long[]> implements ServerInterface
{
    public static final int PORT = 8888;
    
    private volatile transient MadeUpServer server;
    
    @Override
    protected void startup( Long[] creationTimeAndStoreId ) throws Throwable
    {
        MadeUpCommunicationInterface implementation = new MadeUpImplementation(
                new StoreId( creationTimeAndStoreId[0], creationTimeAndStoreId[1] ) );
        server = new MadeUpServer( implementation, 8888 );
    }

    @Override
    public void awaitStarted()
    {
        try
        {
            while ( server == null )
            {
                Thread.sleep( 10 );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    public void shutdown()
    {
        if ( server != null )
        {
            server.shutdown();
        }
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
