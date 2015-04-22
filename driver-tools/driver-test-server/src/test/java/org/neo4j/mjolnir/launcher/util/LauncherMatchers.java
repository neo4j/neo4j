/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.mjolnir.launcher.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

import org.neo4j.function.Supplier;

public class LauncherMatchers
{
    public static void assertEventually( String failure,  Supplier<Boolean> condition ) throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + 30 * 1000;
        while ( !condition.get() )
        {
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
                throw e;
            }
            if ( System.currentTimeMillis() > deadline )
            {
                throw new AssertionError( failure );
            }
        }
    }

    public static Supplier<Boolean> serverListensTo( final String host, final int port )
    {
        return new Supplier<Boolean>()
        {
            @Override
            public Boolean get()
            {
                try ( Socket socket = new Socket() )
                {
                    // Ok, we can connect - can we perform the version handshake?
                    socket.connect( new InetSocketAddress( host, port ) );
                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

                    // Hard-coded handshake, a general "test client" would be useful further on.
                    out.write( new byte[] { 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } );
                    byte[] accepted = new byte[4];

                    in.read( accepted );
                    return Arrays.equals( accepted, new byte[] { 0, 0, 0, 1 } );
                }
                catch ( ConnectException e )
                {
                    return false;
                }
                catch ( IOException e )
                {
                    return false;
                }
            }
        };
    }
}
