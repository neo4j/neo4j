/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public abstract class LuceneIndex implements LegacyIndex
{
    static final String KEY_DOC_ID = "_id_";
    static final String KEY_START_NODE_ID = "_start_node_id_";
    static final String KEY_END_NODE_ID = "_end_node_id_";

    private static Set<String> FORBIDDEN_KEYS = new HashSet<>( Arrays.asList(
            null, KEY_DOC_ID, KEY_START_NODE_ID, KEY_END_NODE_ID ) );

    protected final IndexIdentifier identifier;
    final IndexType type;
    private volatile boolean deleted;

    // Will contain ids which were found to be missing from the graph when doing queries
    // Write transactions can fetch from this list and add to their transactions to
    // allow for self-healing properties.
    final Collection<Long> abandonedIds = new CopyOnWriteArraySet<>();
    protected final LuceneTransactionState transaction;
    private final LuceneDataSource dataSource;
    protected final IndexCommandFactory commandFactory;

    LuceneIndex( LuceneDataSource dataSource, IndexIdentifier identifier, LuceneTransactionState transaction,
            IndexType type, IndexCommandFactory commandFactory )
    {
        this.dataSource = dataSource;
        this.identifier = identifier;
        this.transaction = transaction;
        this.type = type;
        this.commandFactory = commandFactory;
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
    public void addNode( long entityId, String key, Object value )
    {
        assertValidKey( key );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            oneValue = getCorrectValue( oneValue );
            transaction.add( this, entityId, key, oneValue );
            commandFactory.addNode( identifier.indexName, entityId, key, oneValue );
        }
    }

    protected Object getCorrectValue( Object value )
    {
        if ( value instanceof ValueContext )
        {
            return ((ValueContext) value).getCorrectValue();
        }
        return value.toString();
    }

    private static void assertValidKey( String key )
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
    public void remove( long entity, String key, Object value )
    {
        assertValidKey( key );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            oneValue = getCorrectValue( oneValue );
            transaction.remove( this, entity, key, oneValue );
            addRemoveCommand( entity, key, oneValue );
        }
    }

    @Override
    public void remove( long entity, String key )
    {
        assertValidKey( key );
        transaction.remove( this, entity, key );
        addRemoveCommand( entity, key, null );
    }

    @Override
    public void remove( long entity )
    {
        transaction.remove( this, entity );
        addRemoveCommand( entity, null, null );
    }

    @Override
    public void drop()
    {
        transaction.delete( this );
    }

    @Override
    public LegacyIndexHits get( String key, Object value )
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
    public LegacyIndexHits query( String key, Object queryOrQueryObject )
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
    public LegacyIndexHits query( Object queryOrQueryObject )
    {
        return query( null, queryOrQueryObject );
    }

    protected LegacyIndexHits query( Query query, String keyForDirectLookup,
            Object valueForDirectLookup, QueryContext additionalParametersOrNull )
    {
        List<Long> ids = new ArrayList<>();
        Collection<Long> removedIds = Collections.emptySet();
        IndexSearcher additionsSearcher = null;
        if ( transaction != null )
        {
            if ( keyForDirectLookup != null )
            {
                ids.addAll( transaction.getAddedIds( this, keyForDirectLookup, valueForDirectLookup ) );
            }
            else
            {
                additionsSearcher = transaction.getAdditionsAsSearcher( this, additionalParametersOrNull );
            }
            removedIds = keyForDirectLookup != null ?
                    transaction.getRemovedIds( this, keyForDirectLookup, valueForDirectLookup ) :
                    transaction.getRemovedIds( this, query );
        }
        LegacyIndexHits idIterator = null;
        IndexReference searcher = null;
        dataSource.getReadLock();
        try
        {
            searcher = dataSource.getIndexSearcher( identifier );
        }
        finally
        {
            dataSource.releaseReadLock();
        }

        if ( searcher != null )
        {
            boolean foundInCache = false;
            LruCache<String, Collection<Long>> cachedIdsMap = null;
            if ( keyForDirectLookup != null )
            {
                cachedIdsMap = dataSource.getFromCache(
                        identifier, keyForDirectLookup );
                foundInCache = fillFromCache( cachedIdsMap, ids,
                        valueForDirectLookup.toString(), removedIds );
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
                    Collection<LegacyIndexHits> iterators = new ArrayList<>();
                    iterators.add( searchedIds );
                    iterators.add( new ConstantScoreIterator( ids, Float.NaN ) );
                    idIterator = new CombinedIndexHits( iterators );
                }
            }
        }

        idIterator = idIterator == null ? new ConstantScoreIterator( ids, 0 ) : idIterator;
        return idIterator;
    }

    private boolean fillFromCache(
            LruCache<String, Collection<Long>> cachedNodesMap,
            List<Long> ids, String valueAsString,
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
        dataSource.setCacheCapacity( identifier, key, capacity );
    }

    public Integer getCacheCapacity( String key )
    {
        return dataSource.getCacheCapacity( identifier, key );
    }

    IndexIdentifier getIdentifier()
    {
        return this.identifier;
    }

    protected abstract void addRemoveCommand( long entity, String key, Object value );

    static class NodeIndex extends LuceneIndex
    {
        NodeIndex( LuceneDataSource dataSource, IndexIdentifier identifier,
                LuceneTransactionState transaction, IndexType type, IndexCommandFactory commandFactory )
        {
            super( dataSource, identifier, transaction, type, commandFactory );
        }

        @Override
        public LegacyIndexHits get( String key, Object value, long startNode, long endNode )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public LegacyIndexHits query( String key, Object queryOrQueryObject, long startNode, long endNode )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public LegacyIndexHits query( Object queryOrQueryObject, long startNode, long endNode )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public void addRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        protected void addRemoveCommand( long entity, String key, Object value )
        {
            commandFactory.removeNode( identifier.indexName, entity, key, value );
        }
    }

    static class RelationshipIndex extends LuceneIndex
    {
        RelationshipIndex( LuceneDataSource dataSource, IndexIdentifier identifier,
                LuceneTransactionState transaction, IndexType type, IndexCommandFactory commandFactory )
        {
            super( dataSource, identifier, transaction, type, commandFactory );
        }

        @Override
        public void addRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            assertValidKey( key );
            for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
            {
                oneValue = getCorrectValue( oneValue );
                transaction.add( this, RelationshipId.of( entity, startNode, endNode ), key, oneValue );
                commandFactory.addRelationship( identifier.indexName, entity, key, oneValue, startNode, endNode );
            }
        }

        @Override
        public LegacyIndexHits get( String key, Object valueOrNull, long startNode, long endNode )
        {
            BooleanQuery query = new BooleanQuery();
            if ( key != null && valueOrNull != null )
            {
                query.add( type.get( key, valueOrNull ), Occur.MUST );
            }
            addIfAssigned( query, startNode, KEY_START_NODE_ID );
            addIfAssigned( query, endNode, KEY_END_NODE_ID );
            return query( query, null, null, null );
        }

        @Override
        protected void addRemoveCommand( long entity, String key, Object value )
        {
            commandFactory.removeRelationship( identifier.indexName, entity, key, value );
        }

        @Override
        public LegacyIndexHits query( String key, Object queryOrQueryObjectOrNull, long startNode, long endNode )
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
            addIfAssigned( query, startNode, KEY_START_NODE_ID );
            addIfAssigned( query, endNode, KEY_END_NODE_ID );
            return query( query, null, null, context );
        }

        private static void addIfAssigned( BooleanQuery query, long node, String field )
        {
            if ( node != -1 )
            {
                query.add( new TermQuery( new Term( field, "" + node ) ), Occur.MUST );
            }
        }

        @Override
        public LegacyIndexHits query( Object queryOrQueryObjectOrNull, long startNode, long endNode )
        {
            return query( null, queryOrQueryObjectOrNull, startNode, endNode );
        }
    }
}
