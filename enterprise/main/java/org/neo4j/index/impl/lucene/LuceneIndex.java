package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.Hits;
import org.apache.lucene.search.Query;
import org.neo4j.commons.iterator.CombiningIterator;
import org.neo4j.commons.iterator.FilteringIterator;
import org.neo4j.commons.iterator.IteratorUtil;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.IdToEntityIterator;
import org.neo4j.index.impl.PrimitiveUtils;
import org.neo4j.index.impl.SimpleIndexHits;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;

abstract class LuceneIndex<T extends PropertyContainer> implements Index<T>
{
    static final String KEY_DOC_ID = "_id_";
    
    final LuceneIndexProvider service;
    final IndexIdentifier identifier;
    final IndexType type;

    LuceneIndex( LuceneIndexProvider service, IndexIdentifier identifier )
    {
        this.service = service;
        this.identifier = identifier;
        this.type = service.dataSource.getType( identifier );
    }
    
    LuceneXaConnection getConnection()
    {
        if ( service.broker == null )
        {
            throw new ReadOnlyDbException();
        }
        return service.broker.acquireResourceConnection();
    }
    
    LuceneXaConnection getReadOnlyConnection()
    {
        return service.broker == null ? null :
                service.broker.acquireReadOnlyResourceConnection();
    }
    
    /**
     * See {@link Index#add(PropertyContainer, String, Object)} for more generic
     * documentation.
     * 
     * Adds key/value to the {@code entity} in this index. Added values are
     * searchable withing the transaction, but composite {@code AND}
     * queries aren't guaranteed to return added values correctly within that
     * transaction. When the transaction has been committed all such queries
     * are guaranteed to return correct results.
     * 
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to associate the key/value pair with.
     * @param key the key in the key/value pair to associate with the entity.
     * @param value the value in the key/value pair to associate with the
     * entity.
     */
    public void add( T entity, String key, Object value )
    {
        for ( Object oneValue : PrimitiveUtils.asArray( value ) )
        {
            getConnection().add( this, entity, key, oneValue );
        }
    }

    /**
     * See {@link Index#remove(PropertyContainer, String, Object)} for more
     * generic documentation.
     * 
     * Removes key/value to the {@code entity} in this index. Removed values
     * are excluded withing the transaction, but composite {@code AND}
     * queries aren't guaranteed to exclude removed values correctly within
     * that transaction. When the transaction has been committed all such
     * queries are guaranteed to return correct results.
     * 
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to dissociate the key/value pair from.
     * @param key the key in the key/value pair to dissociate from the entity.
     * @param value the value in the key/value pair to dissociate from the
     * entity.
     */
    public void remove( T entity, String key, Object value )
    {
        for ( Object oneValue : PrimitiveUtils.asArray( value ) )
        {
            getConnection().remove( this, entity, key, oneValue );
        }
    }
    
    public void clear()
    {
        getConnection().clear( this );
    }
    
    public IndexHits<T> get( String key, Object value )
    {
        return query( type.get( key, value ), key, value );
    }

    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        return query( type.query( key, queryOrQueryObject ), null, null );
    }

    public IndexHits<T> query( Object queryOrQueryObject )
    {
        return query( type.query( null, queryOrQueryObject ), null, null );
    }
    
    private IndexHits<T> query( Query query, String keyForDirectLookup,
            Object valueForDirectLookup )
    {
        List<Long> ids = new ArrayList<Long>();
        LuceneXaConnection con = getReadOnlyConnection();
        LuceneTransaction luceneTx = null;
        if ( con != null )
        {
            luceneTx = getReadOnlyConnection().getLuceneTx();
        }
        Set<Long> addedIds = Collections.emptySet();
        Set<Long> removedIds = Collections.emptySet();
        Query excludeQuery = null;
        boolean isRemoveAll = false;
        if ( luceneTx != null )
        {
            addedIds = keyForDirectLookup != null ?
                    luceneTx.getAddedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    luceneTx.getAddedIds( this, query );
            ids.addAll( addedIds );
            removedIds = keyForDirectLookup != null ?
                    luceneTx.getRemovedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    luceneTx.getRemovedIds( this, query );
            excludeQuery = luceneTx.getExtraRemoveQuery( this );
            isRemoveAll = luceneTx.isRemoveAll( this );
        }
        service.dataSource.getReadLock();
        Iterator<Long> idIterator = null;
        Integer idIteratorSize = null;
        IndexSearcherRef searcher = null;
        boolean isLazy = false;
        try
        {
            searcher = service.dataSource.getIndexSearcher( identifier );
            if ( !isRemoveAll && searcher != null )
            {
                if ( excludeQuery != null )
                {
                    removedIds = removedIds.isEmpty() ? new HashSet<Long>() : removedIds;
                    readNodesFromHits( new DocToIdIterator( search( searcher, excludeQuery ),
                            null, searcher ), removedIds );
                }
                
                boolean foundInCache = false;
                LruCache<String, Collection<Long>> cachedIdsMap = null;
                if ( keyForDirectLookup != null )
                {
                    cachedIdsMap = service.dataSource.getFromCache(
                            identifier, keyForDirectLookup );
                    foundInCache = fillFromCache( cachedIdsMap, ids,
                            keyForDirectLookup, valueForDirectLookup.toString(), removedIds );
                }
                
                if ( !foundInCache )
                {
                    DocToIdIterator searchedNodeIds = new DocToIdIterator( search( searcher,
                            query ), removedIds, searcher );
                    if ( searchedNodeIds.size() >= service.lazynessThreshold )
                    {
                        // Instantiate a lazy iterator
                        isLazy = true;
                        
                        // TODO Clear cache? why?
                        
                        Collection<Iterator<Long>> iterators = new ArrayList<Iterator<Long>>();
                        iterators.add( ids.iterator() );
                        iterators.add( searchedNodeIds );
                        idIterator = new CombiningIterator<Long>( iterators );
                        idIteratorSize = ids.size() + searchedNodeIds.size();
                    }
                    else
                    {
                        // Loop through result here (and cache it if possible)
                        Collection<Long> readIds = readNodesFromHits( searchedNodeIds, ids );
                        if ( cachedIdsMap != null )
                        {
                            cachedIdsMap.put( valueForDirectLookup.toString(), readIds );
                        }
                    }
                }
            }
        }
        finally
        {
            // The DocToIdIterator closes the IndexSearchRef instance anyways,
            // or the LazyIterator if it's a lazy one. So no need here.
            service.dataSource.releaseReadLock();
        }

        if ( idIterator == null )
        {
            idIterator = ids.iterator();
            idIteratorSize = ids.size();
        }
        idIterator = FilteringIterator.noDuplicates( idIterator );
        IndexHits<T> hits = new SimpleIndexHits<T>(
                IteratorUtil.asIterable( new IdToEntityIterator<T>( idIterator )
                {
                    @Override
                    protected T underlyingObjectToObject( Long id )
                    {
                        return getById( id );
                    }
                } ), idIteratorSize );
        if ( isLazy )
        {
            hits = new LazyIndexHits<T>( hits, searcher );
        }
        return hits;
    }

    private boolean fillFromCache(
            LruCache<String, Collection<Long>> cachedNodesMap,
            List<Long> nodeIds, String key, String valueAsString,
            Set<Long> deletedNodes )
    {
        boolean found = false;
        if ( cachedNodesMap != null )
        {
            Collection<Long> cachedNodes = cachedNodesMap.get( valueAsString );
            if ( cachedNodes != null )
            {
                found = true;
                for ( Long cachedNodeId : cachedNodes )
                {
                    if ( deletedNodes == null ||
                            !deletedNodes.contains( cachedNodeId ) )
                    {
                        nodeIds.add( cachedNodeId );
                    }
                }
            }
        }
        return found;
    }
    
    private SearchResult search( IndexSearcherRef searcher, Query query )
    {
        try
        {
            searcher.incRef();
            Hits hits = new Hits( searcher.getSearcher(), query, null );
            return new SearchResult( new HitsIterator( hits ), hits.length() );
//            TopDocs hits = searcher.getSearcher().search( query, 50 );
//            TopDocsIterator itr = new TopDocsIterator( hits, searcher );
//            return new SearchResult( itr, hits.totalHits );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to query " + this + " with "
                                        + query, e );
        }
    }
    
    public void setCacheCapacity( String key, int capacity )
    {
        service.dataSource.setCacheCapacity( identifier, key, capacity );
    }
    
    public Integer getCacheCapacity( String key )
    {
        return service.dataSource.getCacheCapacity( identifier, key );
    }
    
    private Collection<Long> readNodesFromHits( DocToIdIterator searchedIds,
            Collection<Long> ids )
    {
        ArrayList<Long> readIds = new ArrayList<Long>();
        while ( searchedIds.hasNext() )
        {
            Long readId = searchedIds.next();
            ids.add( readId );
            readIds.add( readId );
        }
        return readIds;
    }
    
    protected abstract T getById( long id );
    
    protected abstract long getEntityId( T entity );
    
    static class NodeIndex extends LuceneIndex<Node>
    {
        NodeIndex( LuceneIndexProvider service,
                IndexIdentifier identifier )
        {
            super( service, identifier );
        }

        @Override
        protected Node getById( long id )
        {
            return service.graphDb.getNodeById( id );
        }
        
        @Override
        protected long getEntityId( Node entity )
        {
            return entity.getId();
        }
    }
    
    static class RelationshipIndex extends LuceneIndex<Relationship>
    {
        RelationshipIndex( LuceneIndexProvider service,
                IndexIdentifier identifier )
        {
            super( service, identifier );
        }

        @Override
        protected Relationship getById( long id )
        {
            return service.graphDb.getRelationshipById( id );
        }
        
        @Override
        protected long getEntityId( Relationship entity )
        {
            return entity.getId();
        }
    }
}
