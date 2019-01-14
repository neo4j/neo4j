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
package org.neo4j.shell;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.function.Predicates.await;

/**
 * Tests that the shell port is freed on database shutdown
 */
public class FreePortIT
{
    private static final String HOST = "localhost";
    private static final int PORT = 13463;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public GraphDatabaseService initialize() throws IOException
    {
        GraphDatabaseService db;
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( temporaryFolder.newFolder() )
                .setConfig( ShellSettings.remote_shell_enabled, "true" )
                .setConfig( ShellSettings.remote_shell_host, HOST )
                .setConfig( ShellSettings.remote_shell_port, Integer.toString( PORT ) ).newGraphDatabase();

        return db;
    }

    private boolean portAvailable()
    {
        return portAvailable( HOST, PORT );
    }

    private boolean portAvailable( String host, int port )
    {
        ServerSocket s = null;
        try
        {
            s = new ServerSocket();
            s.bind( new InetSocketAddress( host, port ) );
            return s.isBound();
        }
        catch ( Throwable ignored )
        {
        }
        finally
        {
            if ( s != null )
            {
                try
                {
                    s.close();
                }
                catch ( Exception ignored )
                {
                }
            }
        }
        return false;
    }

    @Test
    public void testPort() throws Exception
    {
        // Given
        assertTrue( portAvailable() );
        GraphDatabaseService db = initialize();
        assertTrue( db.isAvailable( TimeUnit.SECONDS.toMillis( 5L ) ) );
        assertFalse( portAvailable() );

        // When
        db.shutdown();

        // Then
        await( this::portAvailable, 5L, TimeUnit.SECONDS );
        assertTrue( portAvailable() );
    }
}
