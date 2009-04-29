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
package org.neo4j.remote.sites;

import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;

import org.neo4j.api.core.NeoService;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.BasicNeoServer;
import org.neo4j.remote.RemoteSite;

/**
 * A {@link RemoteSite} that uses RMI for communication. Connecting to an RMI
 * site is as simple as creating a new instance of this class. To expose a
 * {@link NeoService} for remote connections via RMI, use the static
 * {@link #register(BasicNeoServer, String)} method of this class.
 * @author Tobias Ivarsson
 */
public final class RmiSite implements RemoteSite
{
    private final URI uri;

    /**
     * Creates a new {@link RemoteSite} that uses RMI for its communication.
     * @param resourceUri
     *            the RMI resource name in URL form.
     */
    public RmiSite( URI resourceUri )
    {
        uri = resourceUri;
    }

    /**
     * Registers a {@link NeoService} as a {@link RmiSite} with a given name. If
     * a resource is already registered with the specified name, that resources
     * is replaced. Use {@link Naming#unbind(String)} to unregister the
     * {@link NeoService}.
     * @param server
     *            the Neo server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported Neo server as.
     * @throws RemoteException
     *             if the RMI registry could not be contacted.
     * @throws MalformedURLException
     *             if the <code>resourceUri</code> is not properly formatted.
     */
    public static void register( BasicNeoServer server, String resourceUri )
        throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server ) );
    }

    /**
     * Registers a {@link NeoService} as a {@link RmiSite} with a given name on
     * a given port. If a resource is already registered with the specified
     * name, that resources is replaced. Use {@link Naming#unbind(String)} to
     * unregister the {@link NeoService}.
     * @param server
     *            the Neo server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported Neo server as.
     * @param port
     *            the port number on which the remote object receives calls (if
     *            port is zero, an anonymous port is chosen).
     * @throws RemoteException
     *             if the RMI registry could not be contacted.
     * @throws MalformedURLException
     *             if the <code>resourceUri</code> is not properly formatted.
     */
    public static void register( BasicNeoServer server, String resourceUri,
        int port ) throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server, port ) );
    }

    /**
     * Registers a {@link NeoService} as a {@link RmiSite} with a given name on
     * a given port. Uses the specified socket factories to get the sockets for
     * the connections. If a resource is already registered with the specified
     * name, that resources is replaced. Use {@link Naming#unbind(String)} to
     * unregister the {@link NeoService}.
     * @param server
     *            the Neo server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported Neo server as.
     * @param port
     *            the port number on which the remote object receives calls (if
     *            port is zero, an anonymous port is chosen).
     * @param csf
     *            the client-side socket factory for making calls to the remote
     *            object.
     * @param ssf
     *            the server-side socket factory for receiving remote calls.
     * @throws RemoteException
     *             if the RMI registry could not be contacted.
     * @throws MalformedURLException
     *             if the <code>resourceUri</code> is not properly formatted.
     */
    public static void register( BasicNeoServer server, String resourceUri,
        int port, RMIClientSocketFactory csf, RMIServerSocketFactory ssf )
        throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server, port,
            csf, ssf ) );
    }

    /**
     * Start a stand alone Remote Neo / RMI server.
     * 
     * Usage:
     * <pre>
     * java -cp neo.jar:jta.jar:remote-neo.jar org.neo4j.remote.sites.RmiSite PATH RESOURCE_URI
     * </pre>
     * 
     * @param args
     *            The arguments passed on the command line.
     * @throws RemoteException
     *             when the registration of the server fails.
     * @throws IllegalArgumentException
     *             when an error was found in the command line arguments.
     */
    public static void main( String[] args ) throws RemoteException,
        IllegalArgumentException
    {
        String usage = "Usage: " + RmiSite.class.getName()
            + " <Neo dir> <rmi resource uri>";
        if ( args.length != 2 )
        {
            throw new IllegalArgumentException( usage );
        }
        try
        {
            register( new LocalSite( args[ 0 ] ), args[ 1 ] );
        }
        catch ( RuntimeException ex )
        {
            throw new IllegalArgumentException( usage, ex );
        }
        catch ( MalformedURLException ex )
        {
            throw new IllegalArgumentException( usage, ex );
        }
    }

    private RmiLoginSite site()
    {
        try
        {
            return ( RmiLoginSite ) Naming.lookup( uri.toString() );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
        catch ( NotBoundException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
    }

    public RemoteConnection connect()
    {
        RmiLoginSite site = site();
        try
        {
            RmiConnection rmic = site.connect();
            return new RmiConnectionAdapter( rmic );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Could not initiate connection.", e );
        }
    }

    public RemoteConnection connect( String username, String password )
    {
        RmiLoginSite site = site();
        try
        {
            RmiConnection rmic = site.connect( username, password );
            return new RmiConnectionAdapter( rmic );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Could not initiate connection.", e );
        }
    }
}
