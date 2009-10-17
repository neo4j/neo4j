package org.neo4j.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

public class HandleSlaveConnection extends ConnectionJob
{
    private static enum Status implements JobStatus
    {
        GET_MESSAGE,
        SETUP_OFFER_LOG,
        SEND_OFFER,
        GET_OK,
        SETUP_SEND_LOG,
        SEND_LOG,
    }
    
    private final Master master;
    
    private int retries = 0;
    private long logVersionToSend = -1;
    private ReadableByteChannel logToSend = null;
    private long logLength = -1;
    
    public HandleSlaveConnection( Connection connection, Master master, 
        long slaveVersion )
    {
        super( connection, master );
        this.master = master;
        if ( master.hasLog( slaveVersion ) )
        {
            logVersionToSend = slaveVersion;
            logLength = master.getLogLength( slaveVersion );
            
            try
            {
                logToSend = master.getLog( slaveVersion );
            }
            catch ( IOException e )
            {
                throw new SocketException( "Error getting next log", e );
            }
        }
        setStatus( Status.GET_MESSAGE );
    }
    
    private synchronized boolean getMessage()
    {
        if ( logVersionToSend > -1 && logLength > -1 && logToSend != null )
        {
            setStatus( Status.SETUP_OFFER_LOG );
            return true;
        }
        if ( !acquireReadBuffer() )
        {
            return false;
        }
        try
        {
            buffer.limit( 9 );
            int read = connection.read();
            if ( read == 9 )
            {
                buffer.flip();
                byte request = buffer.get();
                if ( request != HeaderConstants.REQUEST_LOG )
                {
                    log( "Unkown request: " + request );
                    close();
                    return true;
                }
                logVersionToSend = buffer.getLong();
                if ( logVersionToSend > master.getVersion() )
                {
                    log( "Got wrong version [" + logVersionToSend + "]" );
                    return true;
                }
                log( "Slave request: " + logVersionToSend );
                if ( master.hasLog( logVersionToSend ) )
                {
                    try
                    {
                        logToSend = master.getLog( logVersionToSend );
                        logLength = master.getLogLength( logVersionToSend );
                    }
                    catch ( IOException e )
                    {
                        close();
                        throw new SocketException( 
                            "Unable to get logical log[" + logVersionToSend + 
                            "]", e );
                    }
                    setStatus( Status.SETUP_OFFER_LOG );
                }
                else
                {
                    log( "No such log version[" + logVersionToSend + "]" );
                    return true;
                }
                retries = 0;
                return true;
            }
            else
            {
                if ( read > 0 )
                {
                    connection.pushBackAllReadData();
                }
                return false;
            }
        }
        finally
        {
            releaseReadBuffer();
        }
    }
    
    private boolean setupOfferLog()
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
        log( "Setup offer: " + logVersionToSend + "," + logLength );
        buffer.put( HeaderConstants.OFFER_LOG );
        buffer.putLong( logVersionToSend );
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
        log( "Send offer: " + logVersionToSend + "," + logLength );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            releaseWriteBuffer();
            setStatus( Status.GET_OK );
            return true;
        }
        retries++;
        return false;
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
                byte status = buffer.get();
                if ( status == HeaderConstants.OK )
                {
                    setStatus( Status.SETUP_SEND_LOG );
                }
                else if ( status == HeaderConstants.NOT_OK )
                {
                    setStatus( Status.GET_MESSAGE );
                    logVersionToSend = -1; 
                    logLength = -1;
                    logToSend = null;
                }
                else
                {
                    log( "Unkown ok message: " + status );
                    close();
                }
                log( "Get ok: " + status );
                retries = 0;
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
    
    private boolean setupSendLog()
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
        log( "Setup log: " + logVersionToSend );
        try
        {
            System.out.println( logToSend.read( buffer ) );
        }
        catch ( IOException e )
        {
            log( "Error reading log", e );
            close();
            return true;
        }
        buffer.flip();
        setStatus( Status.SEND_LOG );
        retries = 0;
        return true;
    }
    
    private boolean sendLog()
    {
        if ( retries > 20 )
        {
            close();
        }
        connection.write();
        log( "Send log: " + logVersionToSend );
        if ( !buffer.hasRemaining() )
        {
            buffer.clear();
            try
            {
                if ( logToSend.read( buffer ) <= 0 )
                {
                    releaseWriteBuffer();
                    setStatus( Status.GET_MESSAGE );
                    if ( master.hasLog( logVersionToSend + 1 ) )
                    {
                        logVersionToSend = logVersionToSend++;
                        logLength = master.getLogLength( logVersionToSend );
                        logToSend = master.getLog( logVersionToSend );
                    }
                    else
                    {
                        logLength = -1;
                        logVersionToSend = -1;
                        logToSend = null;
                    }
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
            case GET_MESSAGE: return getMessage();
            case SETUP_OFFER_LOG: return setupOfferLog();
            case SEND_OFFER: return sendOffer();
            case GET_OK: return getOk();
            case SETUP_SEND_LOG: return setupSendLog();
            case SEND_LOG: return sendLog();
            default:
                throw new IllegalStateException( "Unkown status: " + 
                    getStatus() );
        }
    }
    
    public synchronized boolean offerLogToSlave( long version )
    {
        if ( logLength != -1 || logVersionToSend != -1 || logToSend != null )
        {
            return false;
        }
        try
        {
            logToSend = master.getLog( version );
            logLength = master.getLogLength( version );
            logVersionToSend = version;
            return true;
        }
        catch ( IOException e )
        {
            throw new SocketException( 
                "Unable to get logical log[" + logVersionToSend + 
                "]", e );
        }
    }

    @Override
    void connectionClosed()
    {
        System.out.println( "Connection closed " + connection );
    }
}