/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.transports;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.Transport;

/**
 * A {@link Transport} that communicates with a remote graph database using RMI.
 * 
 * @author Tobias Ivarsson
 */
public final class RmiTransport extends Transport
{
    /**
     * Create a new {@link Transport} for the rmi:// protocol.
     */
    public RmiTransport()
    {
        super( "rmi" );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        return "rmi".equals( resourceUri.getScheme() );
    }

    @Override
    protected ConnectionTarget create( URI resourceUri )
    {
        return new RmiTarget( resourceUri );
    }

    /**
     * Registers a {@link GraphDatabaseService} as an RMI service with a given name. If
     * a resource is already registered with the specified name, that resources
     * is replaced. Use {@link Naming#unbind(String)} to unregister the
     * {@link GraphDatabaseService}.
     * @param server
     *            the graph database server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported graph database server as.
     * @throws RemoteException
     *             if the RMI registry could not be contacted.
     * @throws MalformedURLException
     *             if the <code>resourceUri</code> is not properly formatted.
     */
    public static void register( BasicGraphDatabaseServer server, String resourceUri )
        throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server ) );
    }

    /**
     * Registers a {@link GraphDatabaseService} as an RMI service with a given name on
     * a given port. If a resource is already registered with the specified
     * name, that resources is replaced. Use {@link Naming#unbind(String)} to
     * unregister the {@link GraphDatabaseService}.
     * @param server
     *            the graph database server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported graph database server as.
     * @param port
     *            the port number on which the remote object receives calls (if
     *            port is zero, an anonymous port is chosen).
     * @throws RemoteException
     *             if the RMI registry could not be contacted.
     * @throws MalformedURLException
     *             if the <code>resourceUri</code> is not properly formatted.
     */
    public static void register( BasicGraphDatabaseServer server, String resourceUri,
        int port ) throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server, port ) );
    }

    /**
     * Registers a {@link GraphDatabaseService} as an RMI service with a given name on
     * a given port. Uses the specified socket factories to get the sockets for
     * the connections. If a resource is already registered with the specified
     * name, that resources is replaced. Use {@link Naming#unbind(String)} to
     * unregister the {@link GraphDatabaseService}.
     * @param server
     *            the graph database server to register as an RMI service.
     * @param resourceUri
     *            the name in URL form to register the exported graph database server as.
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
    public static void register( BasicGraphDatabaseServer server, String resourceUri,
        int port, RMIClientSocketFactory csf, RMIServerSocketFactory ssf )
        throws RemoteException, MalformedURLException
    {
        Naming.rebind( resourceUri, RmiConnectionServer.setup( server, port,
            csf, ssf ) );
    }

    /**
     * Start a stand alone Remote graph database / RMI server.
     * <p />
     * Usage:
     * 
     * <pre>
     * java -cp kernel.jar:jta.jar:remote.jar org.neo4j.remote.sites.RmiSite PATH RESOURCE_URI
     * </pre>
     * <p />
     * If the host in the <code>RESOURCE_URI</code> resolves to the local host a
     * registry will be started if none is running.
     * <p />
     * Any further arguments will be used to register index services. These take
     * the form of:
     * 
     * <pre>
     * class.name.for.the.IndexServiceImplementation:index-service-identifier
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
        String usage = "Usage: " + RmiTarget.class.getName()
            + " <GraphDB dir> <rmi resource uri>";
        if ( args.length < 2 )
        {
            throw new IllegalArgumentException( usage );
        }
        // Instantiate the Neo4j server
        final LocalGraphDatabase server;
        try
        {
            server = new LocalGraphDatabase( args[ 0 ] );
        }
        catch ( RuntimeException ex )
        {
            throw new IllegalArgumentException( usage, ex );
        }
        System.out.println( "Created Neo4j server. Store directory: "
            + args[ 0 ] );
        // The rest of the parameters define indexes
        // Loosely couple the indexes, we don't need them unless we use them
        Class<?> indexService;
        Method registerIndexMethod;
        try
        {
            indexService = Class.forName( "org.neo4j.index.IndexService" );
            registerIndexMethod = BasicGraphDatabaseServer.class.getDeclaredMethod(
                "registerIndexService", String.class, indexService );
        }
        catch ( Exception ex )
        {
            indexService = null;
            registerIndexMethod = null;
        }
        // Start each index service
        for ( int i = 2; i < args.length; i++ )
        {
            // ... but not if we don't have the index component
            if ( indexService == null )
            {
                System.err.println( "Could not instantiate index \"" + args[ i ]
                    + "\"\n    The index component is not loaded." );
                continue;
            }
            // Separate the index class name from the index service identifier
            String argument[] = args[ i ].split( ":", 2 );
            String className = argument[ 0 ];
            String indexName = argument.length == 2 ? argument[ 1 ]
                : argument[ 0 ];
            // Instantiate and register the index service
            try
            {
                Class<?> cls = Class.forName( className );
                Constructor<?> ctor = cls.getConstructor( GraphDatabaseService.class );
                Object index = ctor.newInstance( server.container.service );
                registerIndexMethod.invoke( server, indexName, index );
                System.out.println( "Registered index service: " + indexName );
            }
            catch ( Exception ex )
            {
                System.err
                    .println( "Could not instantiate index \"" + args[ i ] );
                ex.printStackTrace( System.err );
            }
        }
        // Check if the resource uri host is localhost
        final URI uri;
        final InetAddress addr;
        final InetAddress localhost;
        final InetAddress[] localhosts;
        try
        {
            uri = new URI( args[ 1 ] );
            addr = InetAddress.getByName( uri.getHost() );
            localhost = InetAddress.getLocalHost();
            localhosts = InetAddress.getAllByName( "localhost" );
        }
        catch ( Exception ex )
        {
            throw new IllegalArgumentException( usage, ex );
        }
        boolean addrIsLocalHost = addr.equals( localhost );
        for ( InetAddress local : localhosts )
        {
            if ( addrIsLocalHost ) break;
            addrIsLocalHost = addr.equals( local );
        }
        // if it is localhost - try to start a registry
        if ( addrIsLocalHost )
        {
            int port = uri.getPort();
            if ( port == -1 ) port = Registry.REGISTRY_PORT;
            try
            {
                LocateRegistry.createRegistry( port );
                System.out.println( "Created RMI registry on localhost." );
            }
            catch ( Exception ex )
            {
                // it could have failed because it is already started
            }
        }
        // Register the server at the specified mounting point in the registry
        try
        {
            register( server, args[ 1 ] );
        }
        catch ( MalformedURLException ex )
        {
            throw new IllegalArgumentException( usage, ex );
        }
        System.out.println( "Neo4j RMI server registered at: " + args[ 1 ] );
        System.out.println( "Press Ctrl+C to stop serving." );
    }
}
