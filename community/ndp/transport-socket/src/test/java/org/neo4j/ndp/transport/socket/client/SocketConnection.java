/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ndp.transport.socket.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.neo4j.helpers.HostnamePort;

public class SocketConnection implements Connection
{
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    public SocketConnection()
    {
        this(new Socket());
    }

    public SocketConnection( Socket socket )
    {
        this.socket = socket;
    }

    @Override
    public Connection connect( HostnamePort address ) throws Exception
    {
        socket.setSoTimeout( 30 * 1000 );

        socket.connect( new InetSocketAddress( address.getHost(), address.getPort() ) );
        in = socket.getInputStream();
        out = socket.getOutputStream();
        return this;
    }

    @Override
    public Connection send( byte[] rawBytes ) throws IOException
    {
        out.write( rawBytes );
        return this;
    }

    @Override
    public byte[] recv( int length ) throws IOException, InterruptedException
    {
        byte[] bytes = new byte[length];
        int left = length, read;

        while ( (read = in.read( bytes, length - left, left )) != -1 && left > 0 )
        {
            left -= read;
        }
        return bytes;
    }

    @Override
    public void discard( int length ) throws IOException
    {
        for ( int i = 0; i < length; i++ )
        {
            in.read();
        }
    }

    @Override
    public void disconnect() throws IOException
    {
        if ( socket != null && socket.isConnected() )
        {
            socket.close();
        }
    }

    @Override
    public void close() throws Exception
    {
        disconnect();
    }
}
