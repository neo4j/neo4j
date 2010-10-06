/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;

/**
 * A remote connection to a running {@link GraphDatabaseService Graph Database}
 * instance, providing access to the Neo4j Graph Database API.
 * @author Tobias Ivarsson
 */
public final class RemoteGraphDatabase implements GraphDatabaseService
{
    private static final ProtocolService protocol = new ProtocolService();

    private final RemoteGraphDbEngine engine;

    /**
     * Creates a new remote graph database connection.
     * @param site
     *            The connection layer to be used.
     */
    public RemoteGraphDatabase( ConnectionTarget site )
    {
        this( null, site );
    }

    /**
     * Creates a new remote graph database connection.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of the graph database.
     * @param site
     *            The connection layer to be used.
     */
    public RemoteGraphDatabase( ConfigurationModule config, ConnectionTarget site )
    {
        this( site.connect(), config );
    }

    /**
     * Creates a new remote graph database connection.
     * @param site
     *            The connection layer to be used.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     */
    public RemoteGraphDatabase( ConnectionTarget site, String username, String password )
    {
        this( null, site, username, password );
    }

    /**
     * Creates a new remote graph database connection.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of the graph database.
     * @param site
     *            The connection layer to be used.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     */
    public RemoteGraphDatabase( ConfigurationModule config, ConnectionTarget site,
        String username, String password )
    {
        this( site.connect( username, password ), config );
    }

    /**
     * Create a remote graph database connection. Select implementation depending on the
     * supplied URI.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteGraphDatabase( String resourceUri ) throws URISyntaxException
    {
        this( null, resourceUri );
    }

    /**
     * Create a remote graph database connection. Select implementation depending on the
     * supplied URI.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of the graph database.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteGraphDatabase( ConfigurationModule config, String resourceUri )
        throws URISyntaxException
    {
        this( config, protocol.get( new URI( resourceUri ) ) );
    }

    /**
     * Create a remote graph database connection. Select implementation depending on the
     * supplied URI.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteGraphDatabase( String resourceUri, String username, String password )
        throws URISyntaxException
    {
        this( null, resourceUri, username, password );
    }

    /**
     * Create a remote graph database connection. Select implementation depending on the
     * supplied URI.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of the graph database.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteGraphDatabase( ConfigurationModule config, String resourceUri,
        String username, String password ) throws URISyntaxException
    {
        this( config, protocol.get( new URI( resourceUri ) ), username,
            password );
    }

    private RemoteGraphDatabase( RemoteConnection connection, ConfigurationModule config )
    {
        this.engine = new RemoteGraphDbEngine( this, connection, config );
    }

    /**
     * Register a {@link ConnectionTarget} implementation with a specified protocol.
     * @param factory
     *            a factory to create the site once it's required.
     */
    public static void registerProtocol( Transport factory )
    {
        protocol.register( factory );
    }

    // GraphDatabaseService implementation

    public Transaction beginTx()
    {
        return engine.beginTx();
    }

    public Node createNode()
    {
        return engine.current().createNode();
    }

    public Node getNodeById( long id )
    {
        // TODO: implement non-transactional path
        return engine.current().getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        // TODO: implement non-transactional path
        return engine.current().getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return engine.current( true ).getReferenceNode();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return engine.current( true ).getRelationshipTypes();
    }

    public Iterable<Node> getAllNodes()
    {
        return engine.current( true ).getAllNodes();
    }

    public void shutdown()
    {
        engine.shutdown();
    }

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException(
                "Event handlers not suppoerted by RemoteGraphDatabase." );
    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException(
                "Event handlers not suppoerted by RemoteGraphDatabase." );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException(
                "Event handlers not suppoerted by RemoteGraphDatabase." );
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException(
                "Event handlers not suppoerted by RemoteGraphDatabase." );
    }

    RemoteGraphDbEngine getEngine()
    {
        return engine;
    }

    // These are scheduled to be removed from the GraphDatabaseService interface.
    public boolean enableRemoteShell()
    {
        // NOTE This might not be something that we wish to support in RemoteGraphDatabase
        return false;
    }

    public boolean enableRemoteShell( Map<String, Serializable> initialProperties )
    {
        // NOTE This might not be something that we wish to support in RemoteGraphDatabase
        return false;
    }
}
