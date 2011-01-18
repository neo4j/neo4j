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
package org.neo4j.remote;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * A remote graph database server that uses a custom protocol for communication with the
 * client.
 * @author Tobias Ivarsson
 */
public final class CustomGraphDatabaseServer
{
    private final GraphDatabaseService graphDb;
    private final ServerSocketChannel channel;
    private final ExecutorService exec;

    /**
     * Create a new Custom protocol remote graph database server.
     * @param graphDb
     *            the {@link GraphDatabaseService} used to back the server.
     * @param endpoint
     *            The address to listen for incoming connections on.
     * @param useSSL
     *            <code>true</code> if the server should secure the
     *            communication through SSL.
     * @throws IOException
     *             If opening or binding the server socket fails.
     */
    public CustomGraphDatabaseServer( GraphDatabaseService graphDb, SocketAddress endpoint,
        boolean useSSL ) throws IOException
    {
        if ( graphDb == null )
        {
            throw new NullPointerException(
                "The graph database implementation may not be null." );
        }
        if ( useSSL )
        {
            throw new UnsupportedOperationException(
                "SSL support has not been implemented for the server." );
        }
        this.graphDb = graphDb;
        channel = ServerSocketChannel.open();
        channel.socket().bind( endpoint );
        exec = Executors.newCachedThreadPool();
        exec.execute( new Runnable()
        {
            public void run()
            {
                accept();
            }
        } );
        exec.execute( new Runnable()
        {
            public void run()
            {
                select();
            }
        } );
    }

    /**
     * Shut down the server.
     */
    public void shutdown()
    {
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
        }
    }

    /**
     * Internal API for setting up a connection to a server.
     * @param remote
     * @param useSSL
     * @return the connection
     */
    public static RemoteConnection connect( SocketAddress remote, boolean useSSL )
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Internal API for setting up a connection to a server.
     * @param remote
     * @param useSSL
     * @param username
     * @param password
     * @return the connection
     */
    public static RemoteConnection connect( SocketAddress remote,
        boolean useSSL, String username, String password )
    {
        // TODO Auto-generated method stub
        return null;
    }

    private void accept()
    {
        // TODO Auto-generated method stub

    }

    private void select()
    {
        // TODO Auto-generated method stub

    }
}
