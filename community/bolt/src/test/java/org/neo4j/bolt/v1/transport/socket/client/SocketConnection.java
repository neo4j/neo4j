/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.transport.socket.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.util.HexPrinter;

public class SocketConnection implements TransportConnection
{
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    public SocketConnection()
    {
        this( new Socket() );
    }

    public SocketConnection( Socket socket )
    {
        this.socket = socket;
    }

    protected void setSocket( Socket socket )
    {
        this.socket = socket;
    }

    @Override
    public TransportConnection connect( HostnamePort address ) throws IOException
    {
        socket.setSoTimeout( 30000 * 1000 ); // TOOD

        socket.connect( new InetSocketAddress( address.getHost(), address.getPort() ) );
        in = socket.getInputStream();
        out = socket.getOutputStream();
        return this;
    }

    @Override
    public TransportConnection send( byte[] rawBytes ) throws IOException
    {
        out.write( rawBytes );
        return this;
    }

    @Override
    public byte[] recv( int length ) throws IOException
    {
        byte[] bytes = new byte[length];
        int left = length;
        int read;

        try
        {
            while ( left > 0 && (read = in.read( bytes, length - left, left )) != -1 )
            {
                left -= read;
            }
        }
        catch ( SocketTimeoutException e )
        {
            throw new SocketTimeoutException( "Reading data timed out, missing " + left + " bytes. Buffer: " + HexPrinter.hex( bytes ) );
        }
        //all the bytes could not be read, fail
        if ( left != 0 )
        {
            throw new IOException( "Failed to read " + length + " bytes, missing " + left + " bytes. Buffer: " + HexPrinter.hex( bytes ) );
        }
        return bytes;
    }

    @Override
    public void disconnect() throws IOException
    {
        if ( socket != null && socket.isConnected() )
        {
            socket.close();
        }
    }
}
