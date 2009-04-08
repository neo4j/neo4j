/*
 * Copyright 2008-2009 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.util.Iterator;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;

/**
 * An implementation of {@link IndexService} for the client side of
 * {@link RemoteNeo}. This requires that {@link IndexService}s are registered on
 * the server using
 * {@link BasicNeoServer#registerIndexService(String, IndexService)}.
 * 
 * @author Tobias Ivarsson
 */
public final class RemoteIndexService implements IndexService
{
    private RemoteNeoEngine engine;
    private final int id;

    /**
     * Create a new client for an {@link IndexService}.
     * 
     * @param neo
     *            The {@link RemoteNeo} that owns the index.
     * @param name
     *            the token that the {@link IndexService} was registered under
     *            on the server (in
     *            {@link BasicNeoServer#registerIndexService(String, IndexService)}
     *            ).
     */
    public RemoteIndexService( NeoService neo, String name )
    {
        this.engine = ( ( RemoteNeo ) neo ).getEngine();
        this.id = this.engine().getIndexId( name );
    }

    private RemoteNeoEngine engine()
    {
        if ( engine == null )
        {
            throw new IllegalStateException( "Index has been shut down." );
        }
        return engine;
    }

    public void shutdown()
    {
        // This is handled by the life cycle of the server
        // - just drop the connection
        engine = null;
    }

    public Iterable<Node> getNodes( String key, Object value )
    {
        return engine().current().getIndexNodes( id, key, value );
    }

    public Node getSingleNode( String key, Object value )
    {
        Iterator<Node> nodes = getNodes( key, value ).iterator();
        Node node = null;
        if ( nodes.hasNext() )
        {
            node = nodes.next();
            if ( nodes.hasNext() )
            {
                throw new RuntimeException( "Multiple values found." );
            }
        }
        return node;

    }

    public void index( Node node, String key, Object value )
    {
        if ( node instanceof RemoteNode )
        {
            RemoteNode remote = ( RemoteNode ) node;
            if ( remote.engine.equals( engine() ) )
            {
                engine().current().indexNode( id, remote, key, value );
                return;
            }
        }
        throw new IllegalArgumentException( "Node not in same node space." );
    }

    public void removeIndex( Node node, String key, Object value )
    {
        if ( node instanceof RemoteNode )
        {
            RemoteNode remote = ( RemoteNode ) node;
            if ( remote.engine.equals( engine() ) )
            {
                engine().current().removeIndexNode( id, remote, key, value );
                return;
            }
        }
        throw new IllegalArgumentException( "Node not in same node space." );
    }

    /**
     * This operation is not supported by the {@link RemoteIndexService}.
     * 
     * @param level
     *            the {@link Isolation} level to set.
     * @see IndexService#setIsolation(Isolation)
     */
    public void setIsolation( Isolation level )
    {
        throw new UnsupportedOperationException(
            "Configuring the isolation not supported by remote client." );
    }
}
