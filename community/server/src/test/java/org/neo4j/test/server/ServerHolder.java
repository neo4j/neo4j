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
package org.neo4j.test.server;

import java.io.IOException;

import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerHelper;

final class ServerHolder extends Thread
{
    private static AssertionError allocation;
    private static NeoServer server;

    static synchronized NeoServer allocate() throws IOException
    {
        if ( allocation != null ) throw allocation;
        if ( server == null ) server = startServer();
        allocation = new AssertionError( "The server was allocated from here but not released properly" );
        return server;
    }

    static synchronized void release( NeoServer server )
    {
        if ( server == null ) return;
        if ( server != ServerHolder.server )
            throw new AssertionError( "trying to suspend a server not allocated from here" );
        if ( allocation == null ) throw new AssertionError( "releasing the server although it is not allocated" );
        allocation = null;
    }

    static synchronized void ensureNotRunning()
    {
        if ( allocation != null ) throw allocation;
        shutdown();
    }

    private static NeoServer startServer() throws IOException
    {
        NeoServer server = ServerHelper.createNonPersistentServer();
        return server;
    }

    private static synchronized void shutdown()
    {
        allocation = null;
        try
        {
            if ( server != null ) server.stop();
        }
        finally
        {
            server = null;
        }
    }

    @Override
    public void run()
    {
        shutdown();
    }

    static
    {
        Runtime.getRuntime().addShutdownHook( new ServerHolder() );
    }

    private ServerHolder()
    {
        super( ServerHolder.class.getName() );
    }
}
