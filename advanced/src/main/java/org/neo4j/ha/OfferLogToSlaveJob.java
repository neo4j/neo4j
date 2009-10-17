package org.neo4j.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

public class OfferLogToSlaveJob extends ConnectionJob
{
    private static enum Status implements JobStatus
    {
        SETUP_OFFER,
        SEND_OFFER,
        GET_OK,
        SEND_LOG
    }
    
    private final Master master;
    
    private int retries = 0;
    private ReadableByteChannel logToSend = null;
    private long logLength = -1;
    private long version;
    
    public OfferLogToSlaveJob( Connection connection, Master master, 
        long version )
    {
        super( connection, master );
        this.master = master;
        this.version = version;
    }
    
    private boolean getOk()
    {
        if ( !acquireReadBuffer() )
        {
            return false;
        }
        try
        {
            buffer.limit( 1 );
            int read = connection.read();
            if ( read == 1 )
            {
                buffer.flip();
                byte response = buffer.get();
                if ( response == HeaderConstants.OK )
                {
                    setStatus( Status.SEND_LOG );
                }
                else if ( response == HeaderConstants.NOT_OK )
                {
                    setNoRequeue();
                }
                else
                {
                    log( "Unkown response: " + response );
                    close();
                }
                return true;
            }
            else
            {
                retries++;
                return false;
            }
        }
        finally
        {
            releaseReadBuffer();
        }
    }
    
    private boolean setupOffer()
    {
        if ( retries > 20 )
        {
            close();
        }
        if ( !acquireWriteBuffer() )
        {
            retries++;
            return false;
        }
        buffer.put( HeaderConstants.OFFER_LOG );
        buffer.putLong( logLength );
        buffer.flip();
        setStatus( Status.SEND_OFFER );
        retries = 0;
        return true;
    }
    
    private boolean sendOffer()
    {
        if ( retries > 20 )
        {
            close();
        }
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            setStatus( Status.SEND_LOG );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean sendLog()
    {
        if ( retries > 20 )
        {
            close();
        }
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            buffer.clear();
            try
            {
                if ( logToSend.read( buffer ) <= 0 )
                {
                    releaseWriteBuffer();
                    setNoRequeue();
                    return true;
                }
                buffer.flip();
            }
            catch ( IOException e )
            {
                log( "Error reading log", e );
                close();
                return true;
            }
        }
        retries++;
        return false;
    }
    
    @Override
    public boolean performJob()
    {
        switch ( (Status) getStatus() )
        {
            case SETUP_OFFER: return setupOffer();
            case SEND_OFFER: return sendOffer();
            case GET_OK: return getOk();
            case SEND_LOG: return sendLog();
            default:
                throw new IllegalStateException( "Unkown status: " + 
                    getStatus() );
        }
    }

    @Override
    void connectionClosed()
    {
        System.out.println( "Connection closed " + connection );
    }
}