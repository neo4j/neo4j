package org.neo4j.index.impl.lucene;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.commons.iterator.IteratorUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

class LuceneIndexServiceWrapper implements IndexService
{
    public static final int DEFAULT_LAZY_THRESHOLD = LuceneIndexProvider.DEFAULT_LAZY_THRESHOLD;
    
    private final ConcurrentMap<String, LuceneIndex<Node>> indexes =
            new ConcurrentHashMap<String, LuceneIndex<Node>>();

    final GraphDatabaseService graphDb;
    final LuceneIndexProvider provider;

    public LuceneIndexServiceWrapper( LuceneIndexProvider provider )
    {
        // TODO Check if we have an old lucene structure stored in this database,
        // and if so convert... here?
        //
        // Or maybe don't convert at all, but instead supply a hook which makes
        // the new index behave as the old?
        this.graphDb = provider.graphDb;
        this.provider = provider;
    }

    protected LuceneIndex<Node> getIndex( String key )
    {
        LuceneIndex<Node> index = indexes.get( key );
        if ( index != null )
        {
            return index;
        }
        index = (LuceneIndex<Node>) getNodeIndex( key );
        indexes.putIfAbsent( key, index );
        return indexes.get( key );
    }

    protected Index<Node> getNodeIndex( String key )
    {
        return this.provider.nodeIndex( key, LuceneIndexProvider.EXACT_CONFIG );
    }

    public IndexHits<Node> getNodes( String key, Object value )
    {
        return getIndex( key ).get( key, value );
    }

    public IndexHits<Node> getNodesExactMatch( String key, Object value )
    {
        return getIndex( key ).get( key, value );
    }
    
    public Node getSingleNode( String key, Object value )
    {
        return single( getNodes( key, value ) );
    }
    
    private Node single( IndexHits<Node> hits )
    {
        try
        {
            return IteratorUtil.singleValueOrNull( hits );
        }
        finally
        {
            hits.close();
        }
    }
    
    public Node getSingleNodeExactMatch( String key, Object value )
    {
        return single( getNodesExactMatch( key, value ) );
    }

    public void index( Node node, String key, Object value )
    {
        getIndex( key ).add( node, key, value );
    }

    public void removeIndex( Node node, String key, Object value )
    {
        getIndex( key ).remove( node, key, value );
    }

    public void removeIndex( Node node, String key )
    {
        throw new UnsupportedOperationException();
    }

    public void removeIndex( String key )
    {
        getIndex( key ).clear();
    }
    
    public void enableCache( String key, int size )
    {
        getIndex( key ).setCacheCapacity( key, size );
    }
    
    public Integer getEnabledCacheSize( String key )
    {
        return getIndex( key ).getCacheCapacity( key );
    }

    public void shutdown()
    {
    }
}
