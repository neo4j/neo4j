package org.neo4j.onlinebackup.ha;

import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class LogApplier extends Thread
{
    private volatile boolean run = true;
    
    private final XaDataSource[] xaDataSources;
    
    LogApplier( XaDataSource[] xaDataSources )
    {
        this.xaDataSources = xaDataSources;
    }
    
    public void run()
    {
        try
        {
            while ( run )
            {
                for ( XaDataSource xaDs : xaDataSources )
                {
                    long logVersion = xaDs.getCurrentLogVersion();
                    if ( xaDs.hasLogicalLog( logVersion ) )
                    {
                        ReadableByteChannel logChannel = 
                            xaDs.getLogicalLog( logVersion );
                        xaDs.applyLog( logChannel );
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
        }
        catch ( Exception e )
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