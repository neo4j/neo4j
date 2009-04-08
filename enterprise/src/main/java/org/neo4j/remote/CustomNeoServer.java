/*
 * Copyright 2008-2009 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.api.core.NeoService;

/**
 * A Remote Neo server that uses a custom protocol for communication with the
 * client.
 * @author Tobias Ivarsson
 */
public final class CustomNeoServer
{
    private final NeoService neo;
    private final ServerSocketChannel channel;
    private final ExecutorService exec;

    /**
     * Create a new Custom protocol Remote Neo server.
     * @param neo
     *            the {@link NeoService} used to back the server.
     * @param endpoint
     *            The address to listen for incoming connections on.
     * @param useSSL
     *            <code>true</code> if the server should secure the
     *            communication through SSL.
     * @throws IOException
     *             If opening or binding the server socket fails.
     */
    public CustomNeoServer( NeoService neo, SocketAddress endpoint,
        boolean useSSL ) throws IOException
    {
        if ( neo == null )
        {
            throw new NullPointerException(
                "The neo implementation may not be null." );
        }
        if ( useSSL )
        {
            throw new UnsupportedOperationException(
                "SSL support has not been implemented for the server." );
        }
        this.neo = neo;
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
