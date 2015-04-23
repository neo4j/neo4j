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
package org.neo4j.ext;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

public class NDPExtensionIT
{
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    private GraphDatabaseService db;

    @Test
    public void shouldLaunchNDP() throws Throwable
    {
        // When I run Neo4j with the ndp extension on the class path, and experimental ndp config on
        db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( tmpDir.getRoot().getAbsolutePath() )
                .setConfig( NDPKernelExtension.Settings.ndp_enabled, "true" )
                .newGraphDatabase();

        // Then
        assertEventuallyServerResponds( "localhost", 7687 );
    }

    @Test
    public void shouldBeAbleToSpecifyHostAndPort() throws Throwable
    {
        // When I run Neo4j with the ndp extension on the class path
        db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( tmpDir.getRoot().getAbsolutePath() )
                .setConfig( NDPKernelExtension.Settings.ndp_enabled, "true" )
                .setConfig( NDPKernelExtension.Settings.ndp_socket_address, "localhost:8776" )
                .newGraphDatabase();

        // Then
        assertEventuallyServerResponds( "localhost", 8776 );
    }

    private void assertEventuallyServerResponds( String host, int port ) throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        for (; ; )
        {
            if ( serverResponds( host, port ) )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }

            // Make sure process still is alive
            if ( System.currentTimeMillis() > timeout )
            {
                throw new RuntimeException( "Waited for 30 seconds for server to respond to HTTP calls, " +
                                            "but no response, timing out to avoid blocking forever." );
            }
        }
    }

    private boolean serverResponds( String host, int port ) throws IOException, InterruptedException
    {
        try
        {
            try ( Socket socket = new Socket() )
            {
                // Ok, we can connect - can we perform the version handshake?
                socket.connect( new InetSocketAddress( host, port ) );
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                // Hard-coded handshake, a general "test client" would be useful further on.
                out.write( new byte[]{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} );

                byte[] accepted = new byte[4];
                in.read( accepted );

                return Arrays.equals( accepted, new byte[]{0, 0, 0, 1} );
            }
        }
        catch ( ConnectException e )
        {
            return false;
        }
    }

    @After
    public void cleanup()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }
}
