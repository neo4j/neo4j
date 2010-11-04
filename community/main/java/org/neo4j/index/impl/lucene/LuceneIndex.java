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

package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.index.impl.IdToEntityIterator;
import org.neo4j.index.impl.IndexHitsImpl;
import org.neo4j.index.impl.PrimitiveUtils;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;

public abstract class LuceneIndex<T extends PropertyContainer> implements Index<T>
{
    static final String KEY_DOC_ID = "_id_";
    static final String KEY_START_NODE_ID = "_start_node_id_";
    static final String KEY_END_NODE_ID = "_end_node_id_";
    
    final LuceneIndexProvider service;
    private IndexIdentifier identifier;
    final IndexType type;
    private volatile boolean deleted;
    
    // Will contain ids which were found to be missing from the graph when doing queries
    // Write transactions can fetch from this list and add to their transactions to
    // allow for self-healing properties.
    final Collection<Long> abandonedIds = new CopyOnWriteArraySet<Long>();

    LuceneIndex( LuceneIndexProvider service, IndexIdentifier identifier )
    {
        this.service = service;
        this.identifier = identifier;
        this.type = service.dataSource().getType( identifier );
    }
    
    LuceneXaConnection getConnection()
    {
        assertNotDeleted();
        if ( service.broker() == null )
        {
            throw new ReadOnlyDbException();
        }
        return service.broker().acquireResourceConnection();
    }
    
    private void assertNotDeleted()
    {
        if ( deleted )
        {
            throw new IllegalStateException( "This index (" + identifier + ") has been deleted" );
        }
    }

    LuceneXaConnection getReadOnlyConnection()
    {
        assertNotDeleted();
        return service.broker() == null ? null :
                service.broker().acquireReadOnlyResourceConnection();
    }
    
    void markAsDeleted()
    {
        this.deleted = true;
        this.abandonedIds.clear();
    }
    
    public String getName()
    {
        return this.identifier.indexName;
    }
    
    public Map<String, String> getConfiguration()
    {
        return this.identifier.config;
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
        LuceneXaConnection connection = getConnection();
        for ( Object oneValue : PrimitiveUtils.asArray( value ) )
        {
            connection.add( this, entity, key, oneValue );
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
        LuceneXaConnection connection = getConnection();
        for ( Object oneValue : PrimitiveUtils.asArray( value ) )
        {
            connection.remove( this, entity, key, oneValue );
        }
    }
    
    public void delete()
    {
        getConnection().deleteIndex( this );
    }
    
    public IndexHits<T> get( String key, Object value )
    {
        return query( type.get( key, value ), key, value, null );
    }

    /**
     * {@inheritDoc}
     * 
     * {@code queryOrQueryObject} can be a {@link String} containing the query
     * in Lucene syntax format, http://lucene.apache.org/java/3_0_2/queryparsersyntax.html.
     * Or it can be a {@link Query} object. If can even be a {@link QueryContext}
     * object which can contain a query ({@link String} or {@link Query}) and
     * additional parameters, such as {@link Sort}.
     * 
     * Because of performance issues, including uncommitted transaction modifications
     * in the result is disabled by default, but can be enabled using
     * {@link QueryContext#allowQueryingModifications()}. 
     */
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        QueryContext context = queryOrQueryObject instanceof QueryContext ?
                (QueryContext) queryOrQueryObject : null;
        return query( type.query( key, context != null ?
                context.queryOrQueryObject : queryOrQueryObject, context ), null, null, context );
    }

    /**
     * {@inheritDoc}
     * 
     * @see #query(String, Object)
     */
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        return query( null, queryOrQueryObject );
    }
    
    protected IndexHits<T> query( Query query, String keyForDirectLookup,
            Object valueForDirectLookup, QueryContext additionalParametersOrNull )
    {
        List<Long> ids = new ArrayList<Long>();
        LuceneXaConnection con = getReadOnlyConnection();
        LuceneTransaction luceneTx = con != null ? con.getLuceneTx() : null;
        Collection<Long> addedIds = Collections.emptySet();
        Collection<Long> removedIds = Collections.emptySet();
        if ( luceneTx != null )
        {
            addedIds = keyForDirectLookup != null ?
                    luceneTx.getAddedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    luceneTx.getAddedIds( this, query, additionalParametersOrNull );
            ids.addAll( addedIds );
            removedIds = keyForDirectLookup != null ?
                    luceneTx.getRemovedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    luceneTx.getRemovedIds( this, query );
        }
        service.dataSource().getReadLock();
        Iterator<Long> idIterator = null;
        int idIteratorSize = -1;
        IndexSearcherRef searcher = null;
        boolean isLazy = false;
        try
        {
            searcher = service.dataSource().getIndexSearcher( identifier );
            if ( searcher != null )
            {
                boolean foundInCache = false;
                LruCache<String, Collection<Long>> cachedIdsMap = null;
                if ( keyForDirectLookup != null )
                {
                    cachedIdsMap = service.dataSource().getFromCache(
                            identifier, keyForDirectLookup );
                    foundInCache = fillFromCache( cachedIdsMap, ids,
                            keyForDirectLookup, valueForDirectLookup.toString(), removedIds );
                }
                
                if ( !foundInCache )
                {
                    DocToIdIterator searchedNodeIds = new DocToIdIterator( search( searcher,
                            query, additionalParametersOrNull ), removedIds, searcher );
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
            service.dataSource().releaseReadLock();
        }

        if ( idIterator == null )
        {
            idIterator = ids.iterator();
            idIteratorSize = ids.size();
        }
        idIterator = FilteringIterator.noDuplicates( idIterator );
        IndexHits<T> hits = new IndexHitsImpl<T>(
                IteratorUtil.asIterable( new IdToEntityIterator<T>( idIterator )
                {
                    @Override
                    protected T underlyingObjectToObject( Long id )
                    {
                        return getById( id );
                    }
                    
                    protected void itemDodged( Long item )
                    {
                        abandonedIds.add( item );
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
            Collection<Long> deletedNodes )
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
    
    private SearchResult search( IndexSearcherRef searcher, Query query,
            QueryContext additionalParametersOrNull )
    {
        try
        {
            searcher.incRef();
            Sort sorting = additionalParametersOrNull != null ?
                    additionalParametersOrNull.sorting : null;
            Hits hits = new Hits( searcher.getSearcher(), query, null, sorting );
            return new SearchResult( new HitsIterator( hits ), hits.length() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to query " + this + " with "
                                        + query, e );
        }
    }
    
    public void setCacheCapacity( String key, int capacity )
    {
        service.dataSource().setCacheCapacity( identifier, key, capacity );
    }
    
    public Integer getCacheCapacity( String key )
    {
        return service.dataSource().getCacheCapacity( identifier, key );
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
    
    protected abstract LuceneCommand newAddCommand( PropertyContainer entity,
            String key, Object value );
    
    protected abstract LuceneCommand newRemoveCommand( PropertyContainer entity,
            String key, Object value );
    
    IndexIdentifier getIdentifier()
    {
        return this.identifier;
    }

    void setIdentifier( IndexIdentifier identifier )
    {
        this.identifier = identifier;
    }
    
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
            return service.graphDb().getNodeById( id );
        }
        
        @Override
        protected long getEntityId( Node entity )
        {
            return entity.getId();
        }

        @Override
        protected LuceneCommand newAddCommand( PropertyContainer entity, String key, Object value )
        {
            return new LuceneCommand.AddCommand( getIdentifier(), LuceneCommand.NODE,
                    ((Node) entity).getId(), key, value );
        }

        @Override
        protected LuceneCommand newRemoveCommand( PropertyContainer entity, String key, Object value )
        {
            return new LuceneCommand.RemoveCommand( getIdentifier(), LuceneCommand.NODE,
                    ((Node) entity).getId(), key, value );
        }
    }
    
    static class RelationshipIndex extends LuceneIndex<Relationship>
            implements org.neo4j.graphdb.index.RelationshipIndex
    {
        RelationshipIndex( LuceneIndexProvider service,
                IndexIdentifier identifier )
        {
            super( service, identifier );
        }

        @Override
        protected Relationship getById( long id )
        {
            return service.graphDb().getRelationshipById( id );
        }
        
        @Override
        protected long getEntityId( Relationship entity )
        {
            return entity.getId();
        }
        
        public void add( Relationship relationship )
        {
            
        }

        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
                Node endNodeOrNull )
        {
            BooleanQuery query = new BooleanQuery();
            if ( key != null && valueOrNull != null )
            {
                query.add( type.get( key, valueOrNull ), Occur.MUST );
            }
            addIfNotNull( query, startNodeOrNull, KEY_START_NODE_ID );
            addIfNotNull( query, endNodeOrNull, KEY_END_NODE_ID );
            return query( query, (String) null, null, null );
        }

        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            QueryContext context = queryOrQueryObjectOrNull != null &&
                    queryOrQueryObjectOrNull instanceof QueryContext ?
                            (QueryContext) queryOrQueryObjectOrNull : null;
                    
            BooleanQuery query = new BooleanQuery();
            if ( (context != null && context.queryOrQueryObject != null) ||
                    (context == null && queryOrQueryObjectOrNull != null ) )
            {
                query.add( type.query( key, context != null ?
                        context.queryOrQueryObject : queryOrQueryObjectOrNull, context ), Occur.MUST );
            }
            addIfNotNull( query, startNodeOrNull, KEY_START_NODE_ID );
            addIfNotNull( query, endNodeOrNull, KEY_END_NODE_ID );
            return query( query, (String) null, null, context );
        }
        
        private static void addIfNotNull( BooleanQuery query, Node nodeOrNull, String field )
        {
            if ( nodeOrNull != null )
            {
                query.add( new TermQuery( new Term( field, "" + nodeOrNull.getId() ) ),
                        Occur.MUST );
            }
        }

        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            return query( null, queryOrQueryObjectOrNull, startNodeOrNull, endNodeOrNull );
        }

        @Override
        protected LuceneCommand newAddCommand( PropertyContainer entity, String key, Object value )
        {
            Relationship rel = (Relationship) entity;
            return new LuceneCommand.AddRelationshipCommand( getIdentifier(), LuceneCommand.RELATIONSHIP,
                    RelationshipId.of( rel ), key, value );
        }

        @Override
        protected LuceneCommand newRemoveCommand( PropertyContainer entity, String key, Object value )
        {
            Relationship rel = (Relationship) entity;
            return new LuceneCommand.RemoveCommand( getIdentifier(), LuceneCommand.RELATIONSHIP,
                    RelationshipId.of( rel ), key, value );
        }
    }
}
