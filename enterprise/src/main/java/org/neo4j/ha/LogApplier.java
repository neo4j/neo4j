package org.neo4j.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;

public class LogApplier extends Thread
{
    private final Queue<Long> queue = new ConcurrentLinkedQueue<Long>();
    
    private volatile boolean run = true;
    
    private final NeoStoreXaDataSource xaDs;
    
    LogApplier( NeoStoreXaDataSource xaDs )
    {
        this.xaDs = xaDs;
    }
    
    public boolean applyLog( long version )
    {
        if ( !run )
        {
            throw new IllegalStateException( "Log applier not running" );
        }
        return queue.offer( version );
    }
    
    public void run()
    {
        try
        {
            while ( run )
            {
                Long logVersion = queue.poll();
                if ( logVersion != null )
                {
                    if ( logVersion == xaDs.getCurrentLogVersion() )
                    {
                        ReadableByteChannel logChannel = 
                            xaDs.getLogicalLog( logVersion );
                        xaDs.applyLog( logChannel );
                    }
                }
                else
                {
                    synchronized ( this )
                    {
                        try
                        {
                            this.wait( 250 );
                        }
                        catch ( InterruptedException e )
                        {
                            interrupted();
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            System.err.println( "Failed to apply log: " + e );
            e.printStackTrace();
        }
        finally
        {
            run = false;
        }
    }
    
    public void stopApplyLogs()
    {
        run = false;
    }
}