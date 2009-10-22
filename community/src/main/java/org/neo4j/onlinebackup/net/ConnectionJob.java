package org.neo4j.onlinebackup.net;

import java.nio.ByteBuffer;

public abstract class ConnectionJob extends Job
{
    protected final Connection connection;
    
    protected ByteBuffer buffer; // buffer in use, may be null
    
    public ConnectionJob( Connection connection, Callback callback )
    {
        super( callback );
        this.connection = connection;
    }
    
    public ConnectionJob( String ip, int port, Callback callback )
    {
        super( callback );
        connection = new Connection( ip, port );
    }
    
    public Connection getConnection()
    {
        return connection;
    }
    
    @Override
    protected void setStatus( JobStatus newStatus )
    {
        super.setStatus( newStatus );
        connection.setCurrentAction( newStatus.name() );
    }

    protected boolean acquireReadBuffer()
    {
        if ( buffer != null )
        {
            throw new IllegalStateException( "buffer not null" );
        }
        buffer = connection.tryAcquireReadBuffer();
        return buffer != null;
    }
    
    protected void releaseReadBuffer()
    {
        buffer = null;
        connection.releaseReadBuffer();
    }
    
    protected boolean acquireWriteBuffer()
    {
        if ( buffer != null )
        {
            throw new IllegalStateException( "buffer not null" );
        }
        buffer = connection.tryAcquireWriteBuffer();
        return buffer != null;
    }
    
    protected void releaseWriteBuffer()
    {
        buffer = null;
        connection.releaseWriteBuffer();
    }

    protected void close()
    {
        setNoRequeue();
        connection.close();
        connectionClosed();
    }
    
    abstract void connectionClosed();
    
    public boolean isPeer()
    {
        return false;
    }
}