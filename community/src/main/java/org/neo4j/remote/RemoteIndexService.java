package org.neo4j.remote;

import java.util.Iterator;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;

public final class RemoteIndexService implements IndexService
{
    private RemoteNeoEngine engine;
    private final int id;

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

    public void setIsolation( Isolation level )
    {
        throw new UnsupportedOperationException(
            "Configuring the isolation not supported by remote client." );
    }
}
