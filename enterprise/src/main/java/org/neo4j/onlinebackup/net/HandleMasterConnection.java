package org.neo4j.onlinebackup.net;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.onlinebackup.ha.AbstractSlave;

public class HandleMasterConnection extends ConnectionJob
{
    private static final Random r = new Random( System.currentTimeMillis() );
    
    private static enum Status implements JobStatus
    {
        GET_LOG,
        GET_MESSAGE,
        SETUP_NOT_OK,
        SEND_NOT_OK,
        SETUP_OK,
        SEND_OK,
        SETUP_REQUEST,
        SEND_REQUEST,
    }
    
    private final XaDataSource xaDs;
    
    private int retries = 0;
    private File tempFile;
    private FileChannel logToWrite = null;
    private long logLength = -1;
    private long logVersionWriting = -1;
    private long masterVersion = -1;
    
    public HandleMasterConnection( Connection connection, AbstractSlave slave, 
        long masterVersion, XaDataSource xaDs )
    {
        super( connection, slave );
        this.xaDs = xaDs;
        this.masterVersion = masterVersion;
        if ( xaDs.getCurrentLogVersion() < (masterVersion - 1) )
        {
            setStatus( Status.SETUP_REQUEST );
        }
        else
        {
            setStatus( Status.GET_MESSAGE );
        }
    }
    
    private boolean getMessage()
    {
        if ( !acquireReadBuffer() )
        {
            return false;
        }
        try
        {
            // HEADER(1) + VERSION(8) + LOG_LENGTH(8)
            buffer.limit( 17 );
            int read = connection.read();
            if ( read == 17 )
            {
                buffer.flip();
                byte request = buffer.get();
                if ( request != HeaderConstants.OFFER_LOG )
                {
                    log( "Unkown request: " + request );
                    close();
                    return true;
                }
                long version = buffer.getLong();
                if ( version < xaDs.getCurrentLogVersion() )
                {
                    log( "Got wrong version [" + version + "]" );
                    setStatus( Status.SETUP_NOT_OK );
                    return true;
                }
                if ( version > masterVersion )
                {
                    masterVersion = version;
                }
                logLength = buffer.getLong();
                log( "Got offer: " + version + "," + logLength );
                if ( !xaDs.hasLogicalLog( version ) )
                {
                    try
                    {
                        logVersionWriting = version;
                        do 
                        {
                            tempFile = new File( xaDs.getName() + 
                                    "-logical-transfer.v" + 
                                    Long.toString( version ) + "_" +
                                    r.nextLong() );
                        } while ( tempFile.exists() );
                        logToWrite = new RandomAccessFile( tempFile, 
                            "rw").getChannel();
                        logToWrite.truncate( 0 );
                    }
                    catch ( IOException e )
                    {
                        close();
                        throw new SocketException( 
                            "Unable to setup logical log[" + version + 
                            "] for writing", e );
                    }
                    setStatus( Status.SETUP_OK );
                }
                else
                {
                    log( "We already have log version[" + version + "]" );
                    setStatus( Status.SETUP_NOT_OK );
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
    
    private boolean setupRequest()
    {
        long version = xaDs.getCurrentLogVersion();
        while ( version < masterVersion )
        {
            if ( xaDs.hasLogicalLog( version ) )
            {
                version++;
            }
            else
            {
                break;
            }
        }
        if ( version == masterVersion )
        {
            setStatus( Status.GET_MESSAGE );
            return true;
        }
        if ( retries > 20 )
        {
            close();
        }
        if ( !acquireWriteBuffer() )
        {
            retries++;
            return false;
        }
        buffer.put( HeaderConstants.REQUEST_LOG );
        buffer.putLong( version );
        buffer.flip();
        log( "Setup request: " + version );
        setStatus( Status.SEND_REQUEST );
        retries = 0;
        return true;
    }
    
    private boolean sendRequest()
    {
        if ( retries > 20 )
        {
            close();
        }
        log( "Send request" );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            buffer.clear();
            releaseWriteBuffer();
            setStatus( Status.GET_MESSAGE );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean setupNotOk()
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
        buffer.put( HeaderConstants.NOT_OK );
        buffer.flip();
        log( "Setup not ok" );
        setStatus( Status.SEND_NOT_OK );
        retries = 0;
        return true;
    }
    
    private boolean sendNotOk()
    {
        if ( retries > 20 )
        {
            close();
        }
        log( "Send not ok" );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            buffer.clear();
            releaseWriteBuffer();
            setStatus( Status.GET_MESSAGE );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean setupOk()
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
        buffer.put( HeaderConstants.OK );
        buffer.flip();
        log( "Setup ok" );
        setStatus( Status.SEND_OK );
        retries = 0;
        return true;
    }
    
    private boolean sendOk()
    {
        if ( retries > 20 )
        {
            close();
        }
        log( "Send ok" );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            buffer.clear();
            releaseWriteBuffer();
            setStatus( Status.GET_LOG );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean getLog()
    {
        if ( !acquireReadBuffer() )
        {
            return false;
        }
        log( "Get log" );
        try
        {
            int read = connection.read();
            if ( read > 0 )
            {
                buffer.flip();
                if ( logToWrite.position() + read <= logLength )
                {
                    logToWrite.write( buffer );
                }
                else
                {
                    int readLimit = buffer.limit();
                    int logLimit = (int) (logLength - logToWrite.position()); 
                    buffer.limit( logLimit );
                    logToWrite.write( buffer );
                    buffer.limit( readLimit );
                    byte[] pushData = new byte[readLimit - logLimit];
                    buffer.get( pushData );
                    connection.pushBackReadData( pushData );
                }
                if ( logToWrite.position() >= logLength )
                {
                    log( "Log transfer complete" );
                    if ( xaDs.getCurrentLogVersion() < (masterVersion - 1) )
                    {
                        setStatus( Status.SETUP_REQUEST );
                    }
                    else
                    {
                        setStatus( Status.GET_MESSAGE );
                    }
                    logToWrite.close();
                    String newName = xaDs.getFileName( logVersionWriting );
                    File newLog = new File( newName );
                    if ( newLog.exists() )
                    {
                        log( "Error new log file[" + newName + 
                                "] already exist" );
                        close();
                    }
                    if ( !tempFile.renameTo( new File( newName ) ) )
                    {
                        log( "Unable to move log to " + newName );
                        close();
                    }
                    logVersionWriting = -1;
                    tempFile = null;
                    logToWrite = null;
                    // slave.tryApplyNewLog();
                }
                return true;
            }
            else
            {
                return false;
            }
        }
        catch ( IOException e )
        {
            close();
            log( "Error getting log.", e );
            return true;
        }
        finally
        {
            releaseReadBuffer();
        }
    }
    
    @Override
    public boolean performJob()
    {
        switch ( (Status) getStatus() )
        {
            case GET_LOG: return getLog();
            case GET_MESSAGE: return getMessage();
            case SETUP_REQUEST: return setupRequest();
            case SEND_REQUEST: return sendRequest();
            case SETUP_OK: return setupOk();
            case SEND_OK: return sendOk();
            case SETUP_NOT_OK: return setupNotOk();
            case SEND_NOT_OK: return sendNotOk();
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