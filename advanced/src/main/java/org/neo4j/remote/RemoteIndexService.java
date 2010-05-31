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
package org.neo4j.remote;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

/**
 * An implementation of {@link IndexService} for the client side of
 * {@link RemoteGraphDatabase}. This requires that {@link IndexService}s are registered on
 * the server using
 * {@link BasicGraphDatabaseServer#registerIndexService(String, IndexService)}.
 * 
 * @author Tobias Ivarsson
 */
public final class RemoteIndexService implements IndexService
{
    private RemoteGraphDbEngine engine;
    private final int id;

    /**
     * Create a new client for an {@link IndexService}.
     * 
     * @param graphDb
     *            The {@link RemoteGraphDatabase} that owns the index.
     * @param name
     *            the token that the {@link IndexService} was registered under
     *            on the server (in
     *            {@link BasicGraphDatabaseServer#registerIndexService(String, IndexService)}
     *            ).
     */
    public RemoteIndexService( GraphDatabaseService graphDb, String name )
    {
        this.engine = ( ( RemoteGraphDatabase ) graphDb ).getEngine();
        this.id = this.engine().getIndexId( name );
    }

    private RemoteGraphDbEngine engine()
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

    public IndexHits<Node> getNodes( String key, Object value )
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

    public void removeIndex( Node node, String key )
    {
        if ( node instanceof RemoteNode )
        {
            RemoteNode remote = ( RemoteNode ) node;
            if ( remote.engine.equals( engine() ) )
            {
                engine().current().removeIndexNode( id, remote, key );
                return;
            }
        }
        throw new IllegalArgumentException( "Node not in same node space." );
    }
    
    public void removeIndex( String key )
    {
        engine().current().removeIndexNode( id, key );
    }
}
