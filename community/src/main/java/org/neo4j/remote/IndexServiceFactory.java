package org.neo4j.remote;

import java.util.Iterator;

import org.neo4j.api.core.Node;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;

final class IndexServiceFactory implements ServiceFactory<IndexService>
{
    private final RemoteNeoEngine engine;
    private final IndexingConnection connection;

    IndexServiceFactory( RemoteNeoEngine engine, IndexingConnection connection )
    {
        this.engine = engine;
        this.connection = connection;
    }

    public IndexService createServiceInstance( int serviceId )
    {
        return new IndexServiceClient( this, serviceId );
    }

    private class IndexServiceClient implements IndexService
    {
        private final int serviceId;
        private IndexServiceFactory source;

        IndexServiceClient( IndexServiceFactory source, int serviceId )
        {
            this.serviceId = serviceId;
            this.source = source;
        }

        public void shutdown()
        {
            // This is handled by the life cycle of the server
            // - just drop the connection
            source = null;
        }

        public Iterable<Node> getNodes( String key, Object value )
        {
            return engine.current().getIndexNodes( connection, serviceId, key,
                value );
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
                if ( remote.engine.equals( engine ) )
                {
                    engine.current().indexNode( connection, serviceId, remote,
                        key, value );
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
                if ( remote.engine.equals( engine ) )
                {
                    engine.current().removeIndexNode( connection, serviceId,
                        remote, key, value );
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
}
