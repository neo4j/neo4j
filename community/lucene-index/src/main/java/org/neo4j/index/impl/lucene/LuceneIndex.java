/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public abstract class LuceneIndex<T extends PropertyContainer> implements Index<T>
{
    static final String KEY_DOC_ID = "_id_";
    static final String KEY_START_NODE_ID = "_start_node_id_";
    static final String KEY_END_NODE_ID = "_end_node_id_";

    private static Set<String> FORBIDDEN_KEYS = new HashSet<String>( Arrays.asList( null, KEY_DOC_ID, KEY_START_NODE_ID, KEY_END_NODE_ID ) );

    final LuceneIndexImplementation service;
    private final IndexIdentifier identifier;
    final IndexType type;
    private volatile boolean deleted;

    // Will contain ids which were found to be missing from the graph when doing queries
    // Write transactions can fetch from this list and add to their transactions to
    // allow for self-healing properties.
    final Collection<Long> abandonedIds = new CopyOnWriteArraySet<Long>();

    LuceneIndex( LuceneIndexImplementation service, IndexIdentifier identifier )
    {
        this.service = service;
        this.identifier = identifier;
        this.type = service.dataSource().getType( identifier, false );
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

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return service.graphDb();
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

    @Override
    public String getName()
    {
        return this.identifier.indexName;
    }

    /**
     * See {@link Index#add(PropertyContainer, String, Object)} for more generic
     * documentation.
     *
     * Adds key/value to the {@code entity} in this index. Added values are
     * searchable within the transaction, but composite {@code AND}
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
    @Override
    public void add( T entity, String key, Object value )
    {
        LuceneXaConnection connection = getConnection();
        assertValidKey( key );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            connection.add( this, entity, key, oneValue );
        }
    }

    @Override
    public T putIfAbsent( T entity, String key, Object value )
    {
        // TODO This should not be in NodeManager. Make a separate service that does this, which can be passed into index implementations
        return ((GraphDatabaseAPI)service.graphDb()).getNodeManager().indexPutIfAbsent( this, entity, key, value );
    }

    private void assertValidKey( String key )
    {
        if ( FORBIDDEN_KEYS.contains( key ) )
        {
            throw new IllegalArgumentException( "Key " + key + " forbidden" );
        }
    }

    /**
     * See {@link Index#remove(PropertyContainer, String, Object)} for more
     * generic documentation.
     *
     * Removes key/value to the {@code entity} in this index. Removed values
     * are excluded within the transaction, but composite {@code AND}
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
    @Override
    public void remove( T entity, String key, Object value )
    {
        LuceneXaConnection connection = getConnection();
        assertValidKey( key );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            connection.remove( this, entity, key, oneValue );
        }
    }

    @Override
    public void remove( T entity, String key )
    {
        LuceneXaConnection connection = getConnection();
        assertValidKey( key );
        connection.remove( this, entity, key );
    }

    @Override
    public void remove( T entity )
    {
        LuceneXaConnection connection = getConnection();
        connection.remove( this, entity );
    }

    @Override
    public void delete()
    {
        getConnection().deleteIndex( this );
    }

    @Override
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
     * {@link QueryContext#tradeCorrectnessForSpeed()}.
     */
    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        QueryContext context = queryOrQueryObject instanceof QueryContext ?
                (QueryContext) queryOrQueryObject : null;
        return query( type.query( key, context != null ?
                context.getQueryOrQueryObject() : queryOrQueryObject, context ), null, null, context );
    }

    /**
     * {@inheritDoc}
     *
     * @see #query(String, Object)
     */
    @Override
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
        Collection<Long> removedIds = Collections.emptySet();
        IndexSearcher additionsSearcher = null;
        if ( luceneTx != null )
        {
            if ( keyForDirectLookup != null )
            {
                ids.addAll( luceneTx.getAddedIds( this, keyForDirectLookup, valueForDirectLookup ) );
            }
            else
            {
                additionsSearcher = luceneTx.getAdditionsAsSearcher( this, additionalParametersOrNull );
            }
            removedIds = keyForDirectLookup != null ?
                    luceneTx.getRemovedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    luceneTx.getRemovedIds( this, query );
        }
        IndexHits<Long> idIterator = null;
        IndexReference searcher = null;
        service.dataSource().getReadLock();
        try
        {
            searcher = service.dataSource().getIndexSearcher( identifier );
        }
        finally
        {
            service.dataSource().releaseReadLock();
        }
        
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
                DocToIdIterator searchedIds = new DocToIdIterator( search( searcher,
                        query, additionalParametersOrNull, additionsSearcher, removedIds ), removedIds, searcher );
                if ( ids.isEmpty() )
                {
                    idIterator = searchedIds;
                }
                else
                {
                    Collection<IndexHits<Long>> iterators = new ArrayList<IndexHits<Long>>();
                    iterators.add( searchedIds );
                    iterators.add( new ConstantScoreIterator<Long>( ids, Float.NaN ) );
                    idIterator = new CombinedIndexHits<Long>( iterators );
                }
            }
        }

        idIterator = idIterator == null ? new ConstantScoreIterator<Long>( ids, 0 ) : idIterator;
        return newEntityIterator( idIterator );
    }

    @Override
    public boolean isWriteable()
    {
        return true;
    }

    private IndexHits<T> newEntityIterator( IndexHits<Long> idIterator )
    {
        return new IdToEntityIterator<T>( idIterator )
        {
            @Override
            protected T underlyingObjectToObject( Long id )
            {
                return getById( id );
            }

            @Override
            protected void itemDodged( Long item )
            {
                abandonedIds.add( item );
            }
        };
    }

    private boolean fillFromCache(
            LruCache<String, Collection<Long>> cachedNodesMap,
            List<Long> ids, String key, String valueAsString,
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
                    if ( !deletedNodes.contains( cachedNodeId ) )
                    {
                        ids.add( cachedNodeId );
                    }
                }
            }
        }
        return found;
    }

    private IndexHits<Document> search( IndexReference searcherRef, Query query,
            QueryContext additionalParametersOrNull, IndexSearcher additionsSearcher, Collection<Long> removed )
    {
        try
        {
            if ( additionsSearcher != null && !removed.isEmpty() )
            {
                letThroughAdditions( additionsSearcher, query, removed );
            }

            IndexSearcher searcher = additionsSearcher == null ? searcherRef.getSearcher() :
                    new IndexSearcher( new MultiReader( searcherRef.getSearcher().getIndexReader(),
                            additionsSearcher.getIndexReader() ) );
            IndexHits<Document> result = null;
            if ( additionalParametersOrNull != null && additionalParametersOrNull.getTop() > 0 )
            {
                result = new TopDocsIterator( query, additionalParametersOrNull, searcher );
            }
            else
            {
                Sort sorting = additionalParametersOrNull != null ?
                        additionalParametersOrNull.getSorting() : null;
                boolean forceScore = additionalParametersOrNull == null ||
                        !additionalParametersOrNull.getTradeCorrectnessForSpeed();
                Hits hits = new Hits( searcher, query, null, sorting, forceScore );
                result = new HitsIterator( hits );
            }
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to query " + this + " with "
                                        + query, e );
        }
    }

    private void letThroughAdditions( IndexSearcher additionsSearcher, Query query, Collection<Long> removed )
            throws IOException
    {
        Hits hits = new Hits( additionsSearcher, query, null );
        HitsIterator iterator = new HitsIterator( hits );
        while ( iterator.hasNext() )
        {
            String idString = iterator.next().getField( KEY_DOC_ID ).stringValue();
            removed.remove( Long.valueOf( idString ) );
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

    static class NodeIndex extends LuceneIndex<Node>
    {
        private final GraphDatabaseService gdb;

        NodeIndex( LuceneIndexImplementation service,
                   GraphDatabaseService gdb,
                IndexIdentifier identifier )
        {
            super( service, identifier );
            this.gdb = gdb;
        }

        @Override
        protected Node getById( long id )
        {
            return gdb.getNodeById(id);
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

        @Override
        public Class<Node> getEntityType()
        {
            return Node.class;
        }
    }

    static class RelationshipIndex extends LuceneIndex<Relationship>
            implements org.neo4j.graphdb.index.RelationshipIndex
    {
        private final GraphDatabaseService gdb;

        RelationshipIndex( LuceneIndexImplementation service,
                           GraphDatabaseService gdb,
                IndexIdentifier identifier )
        {
            super( service, identifier );
            this.gdb = gdb;
        }

        @Override
        protected Relationship getById( long id )
        {
            return gdb.getRelationshipById(id);
        }

        @Override
        protected long getEntityId( Relationship entity )
        {
            return entity.getId();
        }

        @Override
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

        @Override
        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            QueryContext context = queryOrQueryObjectOrNull != null &&
                    queryOrQueryObjectOrNull instanceof QueryContext ?
                            (QueryContext) queryOrQueryObjectOrNull : null;

            BooleanQuery query = new BooleanQuery();
            if ( (context != null && context.getQueryOrQueryObject() != null) ||
                    (context == null && queryOrQueryObjectOrNull != null ) )
            {
                query.add( type.query( key, context != null ?
                        context.getQueryOrQueryObject() : queryOrQueryObjectOrNull, context ), Occur.MUST );
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

        @Override
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

        @Override
        public Class<Relationship> getEntityType()
        {
            return Relationship.class;
        }
    }
}
