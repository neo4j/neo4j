/**
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
package org.neo4j.ndp.transport.socket.integration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.neo4j.helpers.HostnamePort;

public class NDPConn
{
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    public NDPConn connect( HostnamePort address ) throws IOException
    {
        socket = new Socket();
        socket.connect( new InetSocketAddress( address.getHost(), address.getPort() ) );
        in = socket.getInputStream();
        out = socket.getOutputStream();
        return this;
    }

    public NDPConn send( byte[] rawBytes ) throws IOException
    {
        out.write( rawBytes );
        return this;
    }

    public byte[] recv( int length ) throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        byte[] bytes = new byte[length];
        int left = length, read;
        while ( (read = in.read( bytes, length - left, left )) != -1 && left > 0 )
        {
            if ( System.currentTimeMillis() > timeout )
            {
                throw new IOException( "Waited 30 seconds for " + left + " bytes, recieved " + (length - left) );
            }
            left -= read;
            if ( left > 0 )
            {
                Thread.sleep( 10 );
            }
        }
        return bytes;
    }
}
