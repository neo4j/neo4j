package org.neo4j.ha;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class AcceptJob extends Job
{
    private final ServerSocketChannel serverChannel;
    
    private SocketChannel acceptedChannel = null;
    
    public AcceptJob( Callback callback, ServerSocketChannel channel )
    {
        super( callback );
        this.serverChannel = channel;
    }
    
    public SocketChannel getAcceptedChannel()
    {
        return acceptedChannel; 
    }
    
    @Override
    public boolean performJob()
    {
        try
        {
            if ( serverChannel.isOpen() )
            {
                acceptedChannel = serverChannel.accept();
            }
            else
            {
                setNoRequeue();
            }
        }
        catch ( IOException e )
        {
            throw new SocketException( 
                "Unable to accept incoming connection", e );
        }
        if ( acceptedChannel != null )
        {
            executeCallback();
            acceptedChannel = null;
            return true;
        }
        return false;
    }
}