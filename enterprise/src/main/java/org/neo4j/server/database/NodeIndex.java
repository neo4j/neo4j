package org.neo4j.server.database;

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

public class NodeIndex implements Index<Node>
{
    private final IndexService indexService;

    public NodeIndex( IndexService indexService )
    {
        this.indexService = indexService;
    }

    public boolean add( Node node, String key, Object value )
    {
        // TODO Implement for real
        indexService.index( node, key, value );
        return true;
    }

    public IndexHits<Node> get( String key, Object value )
    {
        return indexService.getNodes( key, value );
    }

    public boolean remove( Node node, String key, Object value )
    {
        boolean existed = contains( node, key, value );
        indexService.removeIndex( node, key, value );
        return existed;
    }

    public boolean contains( Node node, String key, Object value )
    {
        // TODO When IndexService has a method like this, use it directly instead.
        IndexHits<Node> hits = indexService.getNodes( key, value );
        try
        {
            for ( Node hit : hits )
            {
                if ( hit.equals( node ) )
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            hits.close();
        }
    }
}
