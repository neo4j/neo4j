package org.neo4j.onlinebackup.net;

import org.neo4j.onlinebackup.ha.Master;

public class HandleIncommingSlaveJob extends ConnectionJob
{
    private static enum Status implements JobStatus
    {
        GET_GREETING,
        SETUP_GREETING,
        SEND_GREETING,
        SEND_BYE,
    }
    
    private final Master master;
    
    private long slaveVersion;
    private String xaDsName;
    private int retries = 0;
    
    public HandleIncommingSlaveJob( Connection connection, Master master )
    {
        super( connection, master );
        this.master = master;
        setStatus( Status.GET_GREETING );
    }
    
    private boolean getGreeting()
    {
        if ( retries > 20 )
        {
            close();
        }
        if ( !acquireReadBuffer() )
        {
            return false;
        }
        try
        {
            // HEADER(1) + DB_ID(8) + DB_TIMESTAMP(8) + DB_VERISON(8) + 
            // NAME_length(4) + NAME
            // buffer.limit( 25 );
            int read = connection.read();
            if ( read > 29 )
            {
                buffer.flip();
                byte slaveGreeting = buffer.get();
                if ( slaveGreeting != HeaderConstants.SLAVE_GREETING )
                {
                    setStatus( Status.SEND_BYE );
                    return true;
                }
                long id = buffer.getLong();
                long timestamp = buffer.getLong();
                long version = buffer.getLong();
                int strLen = buffer.getInt();
                byte[] bytes = new byte[strLen];
                buffer.get( bytes );
                xaDsName = new String( bytes );
                long masterId = master.getIdentifier( xaDsName );
                long masterTimestamp = master.getCreationTime( xaDsName );
                long masterVersion = master.getVersion( xaDsName );
                if ( id != masterId || timestamp != masterTimestamp || 
                    version > masterVersion )
                {
                    log( "Got wrong id/time/version [" + id + "/" + timestamp + 
                        "/" + version + "]" + "[" + masterId + "/" + 
                        masterTimestamp + "/" + masterVersion + 
                        "] for data source " + xaDsName );
                    setStatus( Status.SEND_BYE );
                    return true;
                }
                log( "Got slave version[" + version + "]. I am version[" + 
                    master.getVersion( xaDsName ) + "] for data source " + xaDsName );
                slaveVersion = version;
                setStatus( Status.SETUP_GREETING );
                retries = 0;
                return true;
            }
            else
            {
                retries++;
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
    
    private boolean setupGreeting()
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
        buffer.put( HeaderConstants.MASTER_GREETING );
        buffer.putLong( master.getVersion( xaDsName ) );
        buffer.flip();
        log( "Setup greeting" );
        setStatus( Status.SEND_GREETING );
        retries = 0;
        return true;
    }
    
    private boolean sendGreeting()
    {
        if ( retries > 20 )
        {
            close();
        }
        log( "Send greeting" );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            releaseWriteBuffer();
            setNoRequeue();
            setChainJob( new HandleSlaveConnection( 
                    connection, master, xaDsName ) );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean sendBye()
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
        try
        {
            log( "Send bye" );
            buffer.put( HeaderConstants.BYE );
            buffer.flip();
            connection.write();
            return true;
        }
        finally
        {
            releaseWriteBuffer();
            close();
        }
    }
    
    @Override
    public boolean performJob()
    {
        switch ( (Status) getStatus() )
        {
            case GET_GREETING: return getGreeting();
            case SETUP_GREETING: return setupGreeting();
            case SEND_GREETING: return sendGreeting();
            case SEND_BYE: return sendBye();
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