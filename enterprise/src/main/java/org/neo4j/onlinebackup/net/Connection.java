/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

public class Connection
{
    private final String ip;
    private final int port;
    
    private final SocketChannel channel; 
    
    private boolean connectionRefused = false;
    
    private final ByteBuffer readBuffer = ByteBuffer.allocate( 32 * 1024 );
    private final ReentrantLock readLock = new ReentrantLock();
    
    private final ByteBuffer writeBuffer = ByteBuffer.allocate( 32 * 1024 );
    private final ReentrantLock writeLock = new ReentrantLock();
    
    private boolean pushBackActive = false;
    private byte[] pushBackData = null;
    
    public Connection( String ip, int port ) throws SocketException
    {
        this.ip = ip;
        this.port = port;
        SocketAddress remoteHost = new InetSocketAddress( ip, port );
        try
        {
            channel = SocketChannel.open();
            channel.configureBlocking( false );
            channel.connect( remoteHost );
        }
        catch ( IOException e )
        {
            connectionRefused = true;
            throw new SocketException( 
                "Could not connect to host[" + ip + ":" + port + "]", e );
        }
    }
    
    public Connection( SocketChannel channel )
    {
        if ( channel == null )
        {
            throw new IllegalArgumentException( "Null channel" );
        }
        this.channel = channel;
        this.port = channel.socket().getPort();
        this.ip = channel.socket().getInetAddress().getHostAddress();
        setNonBlocking();
    }
    
    private void setNonBlocking()
    {
        try
        {
            channel.configureBlocking( false );
        }
        catch ( IOException e )
        {
            throw new SocketException( "Unable to configure non blocking", e );
        }
    }
    
    private String currentAction;
    
    public String getAction()
    {
        return currentAction;
    }
    
    public void setCurrentAction( String action )
    {
        this.currentAction = action;
    }
    
    public boolean connected()
    {
        if ( !channel.isBlocking() && !channel.isConnected() && 
            channel.isConnectionPending() )
        {
            try
            {
                return channel.finishConnect();
            }
            catch ( IOException e )
            {
                connectionRefused = true;
                // System.out.println( toString() + " connection refused" );
            }
        }
        return channel.isConnected();
    }
    
    public void close() throws SocketException
    {
        readLock.lock();
        readBuffer.clear();
        writeLock.lock();
        writeBuffer.clear();
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
            throw new SocketException( 
                "Error closing channel to [" + ip + ":" + port + "]", e );
        }
        readLock.unlock();
        writeLock.unlock();
    }
    
/*    public synchronized void reConnect()
    {
        if ( direction is incomming )
        {
            throw new IllegalStateException( "Incomming direction" );
        }
        if( !channel.isConnectionPending() && !channel.isConnected() )
        {
            try
            {
                connectionRefused = false;
                isPeer = false;
                readBuffer.clear();
                writeBuffer.clear();
                channel.socket().connect( new InetSocketAddress( ip, port ) );
            }
            catch ( IOException e )
            {
                connectionRefused = true;
                System.out.println( toString() + " connection refused" );
            }
        }
    }*/
    
    public boolean connectionRefused()
    {
        return connectionRefused;
    }
    
    public String toString()
    {
        return "Connection[" + ip + ":" + port + "]"; 
    }
    
    
    public ByteBuffer tryAcquireReadBuffer()
    {
        if ( readLock.tryLock() )
        {
            readBuffer.clear();
            return readBuffer;
        }
        return null;
    }
    
    public void releaseReadBuffer()
    {
        readLock.unlock();
    }
    
    public ByteBuffer tryAcquireWriteBuffer()
    {
        if ( writeLock.tryLock() )
        {
            writeBuffer.clear();
            return writeBuffer;
        }
        return null;
    }
    
    public void releaseWriteBuffer()
    {
        writeLock.unlock();
    }
    
    public int read()
    {
        try
        {
            if ( pushBackActive )
            {
                if ( readBuffer.limit() < pushBackData.length )
                {
                    readBuffer.put( pushBackData, 0, readBuffer.limit() );
                    int length = pushBackData.length - readBuffer.limit();
                    byte[] restData = new byte[length];
                    System.arraycopy( pushBackData, readBuffer.limit(), 
                        restData, 0, length );
                    pushBackData = restData;
                    return readBuffer.position();
                }
                pushBackActive = false;
                readBuffer.put( pushBackData );
                pushBackData = null;
            }
            channel.read( readBuffer );
            return readBuffer.position();
        }
        catch ( IOException e )
        {
            throw new SocketException( toString() + " error reading", e );
        }
    }
    
    public void pushBackReadData( byte[] data )
    {
        if ( data.length > 0 )
        {
            pushBackActive = true;
            pushBackData = data;
        }
    }
    
    public void pushBackAllReadData()
    {
        readBuffer.flip();
        if ( readBuffer.limit() > 0 )
        {
            pushBackActive = true;
            pushBackData = new byte[readBuffer.limit()];
            readBuffer.get( pushBackData );
        }
    }
    
    public int write()
    {
        try
        {
            return channel.write( writeBuffer );
        }
        catch ( IOException e )
        {
            close();
            throw new SocketException( toString() + " error writing", e );
        }
    }
    
    public String getIp()
    {
        return ip;
    }
    
    public int getPort()
    {
        return port;
    }
    
    public String getHostAsString()
    {
        return ip + ":" + port;
    }
}