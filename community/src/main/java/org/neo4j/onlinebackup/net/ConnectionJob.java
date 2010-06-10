/*
 * Copyright (c) 2009-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
}