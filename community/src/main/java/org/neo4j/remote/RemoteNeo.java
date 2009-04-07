/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

/**
 * A remote connection to a running Neo instance, providing access to the Neo
 * API.
 * @author Tobias Ivarsson
 */
public final class RemoteNeo implements NeoService
{
    private static final ProtocolService protocol = new ProtocolService();

    private final RemoteNeoEngine engine;

    /**
     * Creates a new remote Neo connection.
     * @param site
     *            The connection layer to be used.
     */
    public RemoteNeo( RemoteSite site )
    {
        this( null, site );
    }

    /**
     * Creates a new remote Neo connection.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of Neo.
     * @param site
     *            The connection layer to be used.
     */
    public RemoteNeo( ConfigurationModule config, RemoteSite site )
    {
        this( site.connect(), config );
    }

    /**
     * Creates a new remote Neo connection.
     * @param site
     *            The connection layer to be used.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     */
    public RemoteNeo( RemoteSite site, String username, String password )
    {
        this( null, site, username, password );
    }

    /**
     * Creates a new remote Neo connection.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of Neo.
     * @param site
     *            The connection layer to be used.
     * @param username
     *            the name of the user to log in as on the remote site. (
     *            <code>null</code> means anonymous)
     * @param password
     *            the password for the user to log in as on the remote site.
     */
    public RemoteNeo( ConfigurationModule config, RemoteSite site,
        String username, String password )
    {
        this( site.connect( username, password ), config );
    }

    /**
     * Create a remote Neo connection. Select implementation depending on the
     * supplied URI.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteNeo( String resourceUri ) throws URISyntaxException
    {
        this( null, resourceUri );
    }

    /**
     * Create a remote Neo connection. Select implementation depending on the
     * supplied URI.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of Neo.
     * @param resourceUri
     *            the URI where the connection resource is located.
     * @throws URISyntaxException
     *             if the resource URI is malformed.
     */
    public RemoteNeo( ConfigurationModule config, String resourceUri )
        throws URISyntaxException
    {
        this( config, protocol.get( new URI( resourceUri ) ) );
    }

    /**
     * Create a remote Neo connection. Select implementation depending on the
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
    public RemoteNeo( String resourceUri, String username, String password )
        throws URISyntaxException
    {
        this( null, resourceUri, username, password );
    }

    /**
     * Create a remote Neo connection. Select implementation depending on the
     * supplied URI.
     * @param config
     *            the {@link ConfigurationModule} containing the configurations
     *            of the subsystems of Neo.
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
    public RemoteNeo( ConfigurationModule config, String resourceUri,
        String username, String password ) throws URISyntaxException
    {
        this( config, protocol.get( new URI( resourceUri ) ), username,
            password );
    }

    private RemoteNeo( RemoteConnection connection, ConfigurationModule config )
    {
        this.engine = new RemoteNeoEngine( connection, config );
    }

    /**
     * Register a {@link RemoteSite} implementation with a specified protocol.
     * @param factory
     *            a factory to create the site once it's required.
     */
    public static void registerProtocol( RemoteSiteFactory factory )
    {
        protocol.register( factory );
    }

    // NeoService implementation

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
        // TODO: implement non-transactional path
        return engine.current().getReferenceNode();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        // TODO: implement non-transactional path
        return engine.current().getRelationshipTypes();
    }

    public void shutdown()
    {
        engine.shutdown();
    }

    public <T> Iterable<ServiceDescriptor<T>> getServices(
        Class<T> iface )
    {
        return engine.getServices( iface );
    }

    // These are scheduled to be removed from the NeoService interface.
    public boolean enableRemoteShell()
    {
        // NOTE This might not be something that we wish to support in RemoteNeo
        return false;
    }

    public boolean enableRemoteShell(
        Map<String, Serializable> initialProperties )
    {
        // NOTE This might not be something that we wish to support in RemoteNeo
        return false;
    }
}
