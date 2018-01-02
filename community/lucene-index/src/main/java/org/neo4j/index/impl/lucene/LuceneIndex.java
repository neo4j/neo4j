/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.EntityId.IdData;
import org.neo4j.index.impl.lucene.EntityId.RelationshipData;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

import static org.neo4j.collection.primitive.Primitive.longSet;
import static org.neo4j.index.impl.lucene.DocToIdIterator.idFromDoc;

public abstract class LuceneIndex implements LegacyIndex
{
    static final String KEY_DOC_ID = "_id_";
    static final String KEY_START_NODE_ID = "_start_node_id_";
    static final String KEY_END_NODE_ID = "_end_node_id_";

    private static Set<String> FORBIDDEN_KEYS = new HashSet<>( Arrays.asList(
            null, KEY_DOC_ID, KEY_START_NODE_ID, KEY_END_NODE_ID ) );

    protected final IndexIdentifier identifier;
    final IndexType type;

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
     * @param key the key in the key/value pair to associate with the entity.
     * @param value the value in the key/value pair to associate with the
     * entity.
     */
    @Override
    public void addNode( long entityId, String key, Object value )
    {
        assertValidKey( key );
        assertValidValue( value );
        EntityId entity = new IdData( entityId );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            oneValue = getCorrectValue( oneValue );
            transaction.add( this, entity, key, oneValue );
            commandFactory.addNode( identifier.indexName, entityId, key, oneValue );
        }
    }

    protected Object getCorrectValue( Object value )
    {
        assertValidValue( value );
        Object result = value instanceof ValueContext
                ? ((ValueContext) value).getCorrectValue()
                : value.toString();
        assertValidValue( value );
        return result;
    }

    private static void assertValidKey( String key )
    {
        if ( FORBIDDEN_KEYS.contains( key ) )
        {
            throw new IllegalArgumentException( "Key " + key + " forbidden" );
        }
    }

    private static void assertValidValue( Object singleValue )
    {
        if ( singleValue == null )
        {
            throw new IllegalArgumentException( "Null value" );
        }
        if ( !(singleValue instanceof Number) && singleValue.toString() == null )
        {
            throw new IllegalArgumentException( "Value of type " + singleValue.getClass() + " has null toString" );
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
     * @param entityId the entity (i.e {@link Node} or {@link Relationship})
     * to dissociate the key/value pair from.
     * @param key the key in the key/value pair to dissociate from the entity.
     * @param value the value in the key/value pair to dissociate from the
     * entity.
     */
    @Override
    public void remove( long entityId, String key, Object value )
    {
        assertValidKey( key );
        EntityId entity = new IdData( entityId );
        for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
        {
            oneValue = getCorrectValue( oneValue );
            transaction.remove( this, entity, key, oneValue );
            addRemoveCommand( entityId, key, oneValue );
        }
    }

    @Override
    public void remove( long entityId, String key )
    {
        assertValidKey( key );
        EntityId entity = new IdData( entityId );
        transaction.remove( this, entity, key );
        addRemoveCommand( entityId, key, null );
    }

    @Override
    public void remove( long entityId )
    {
        EntityId entity = new IdData( entityId );
        transaction.remove( this, entity );
        addRemoveCommand( entityId, null, null );
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
        List<EntityId> simpleTransactionStateIds = new ArrayList<>();
        Collection<EntityId> removedIdsFromTransactionState = Collections.emptySet();
        IndexSearcher fulltextTransactionStateSearcher = null;
        if ( transaction != null )
        {
            if ( keyForDirectLookup != null )
            {
                simpleTransactionStateIds.addAll( transaction.getAddedIds( this, keyForDirectLookup, valueForDirectLookup ) );
            }
            else
            {
                fulltextTransactionStateSearcher = transaction.getAdditionsAsSearcher( this, additionalParametersOrNull );
            }
            removedIdsFromTransactionState = keyForDirectLookup != null ?
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
            try
            {
                // Gather all added ids from fulltextTransactionStateSearcher and simpleTransactionStateIds.
                PrimitiveLongSet idsModifiedInTransactionState = gatherIdsModifiedInTransactionState(
                        simpleTransactionStateIds, fulltextTransactionStateSearcher, query );

                // Do the combined search from store and fulltext tx state
                DocToIdIterator hits = new DocToIdIterator( search( searcher, fulltextTransactionStateSearcher,
                        query, additionalParametersOrNull, removedIdsFromTransactionState ),
                        removedIdsFromTransactionState, searcher, idsModifiedInTransactionState );

                idIterator = simpleTransactionStateIds.isEmpty() ? hits : new CombinedIndexHits(
                        Arrays.<LegacyIndexHits>asList( hits,
                                new ConstantScoreIterator( simpleTransactionStateIds, Float.NaN ) ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to query " + this + " with " + query, e );
            }
        }

        // We've only got transaction state
        idIterator = idIterator == null ? new ConstantScoreIterator( simpleTransactionStateIds, 0 ) : idIterator;
        return idIterator;
    }

    private PrimitiveLongSet gatherIdsModifiedInTransactionState( List<EntityId> simpleTransactionStateIds,
            IndexSearcher fulltextTransactionStateSearcher, Query query ) throws IOException
    {
        // If there's no state them don't bother gathering it
        if ( simpleTransactionStateIds.isEmpty() && fulltextTransactionStateSearcher == null )
        {
            return PrimitiveLongCollections.emptySet();
        }
        // There's potentially some state
        Hits hits = null;
        int fulltextSize = 0;
        if ( fulltextTransactionStateSearcher != null )
        {
            hits = new Hits( fulltextTransactionStateSearcher, query, null, null, false );
            fulltextSize = hits.length();
            // Nah, no state
            if ( simpleTransactionStateIds.isEmpty() && fulltextSize == 0 )
            {
                return PrimitiveLongCollections.emptySet();
            }
        }

        PrimitiveLongSet set = longSet( simpleTransactionStateIds.size() + fulltextSize );

        // Add from simple tx state
        for ( EntityId id : simpleTransactionStateIds )
        {
            set.add( id.id() );
        }

        // Add from fulltext tx state
        for ( int i = 0; i < fulltextSize; i++ )
        {
            set.add( idFromDoc( hits.doc( i ) ) );
        }
        return set;
    }

    private IndexHits<Document> search( IndexReference searcherRef, IndexSearcher fulltextTransactionStateSearcher,
            Query query, QueryContext additionalParametersOrNull, Collection<EntityId> removed ) throws IOException
    {
        if ( fulltextTransactionStateSearcher != null && !removed.isEmpty() )
        {
            letThroughAdditions( fulltextTransactionStateSearcher, query, removed );
        }

        IndexSearcher searcher = fulltextTransactionStateSearcher == null ? searcherRef.getSearcher() :
                new IndexSearcher( new MultiReader( searcherRef.getSearcher().getIndexReader(),
                        fulltextTransactionStateSearcher.getIndexReader() ) );
        IndexHits<Document> result;
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

    private void letThroughAdditions( IndexSearcher additionsSearcher, Query query, Collection<EntityId> removed )
            throws IOException
    {
        Hits hits = new Hits( additionsSearcher, query, null );
        HitsIterator iterator = new HitsIterator( hits );
        EntityId.LongCostume id = new EntityId.LongCostume();
        while ( iterator.hasNext() )
        {
            String idString = iterator.next().getField( KEY_DOC_ID ).stringValue();
            removed.remove( id.setId( Long.parseLong( idString ) ) );
        }
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
            throw new UnsupportedOperationException();
        }

        @Override
        public LegacyIndexHits query( String key, Object queryOrQueryObject, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public LegacyIndexHits query( Object queryOrQueryObject, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeRelationship( long entity, String key, Object value, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeRelationship( long entity, String key, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeRelationship( long entity, long startNode, long endNode )
        {
            throw new UnsupportedOperationException();
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
        public void addRelationship( long entityId, String key, Object value, long startNode, long endNode )
        {
            assertValidKey( key );
            assertValidValue( value );
            RelationshipData entity = new RelationshipData( entityId, startNode, endNode );
            for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
            {
                oneValue = getCorrectValue( oneValue );
                transaction.add( this, entity, key, oneValue );
                commandFactory.addRelationship( identifier.indexName, entityId, key, oneValue, startNode, endNode );
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

        @Override
        public void removeRelationship( long entityId, String key, Object value, long startNode, long endNode )
        {
            assertValidKey( key );
            RelationshipData entity = new RelationshipData( entityId, startNode, endNode );
            for ( Object oneValue : IoPrimitiveUtils.asArray( value ) )
            {
                oneValue = getCorrectValue( oneValue );
                transaction.remove( this, entity, key, oneValue );
                addRemoveCommand( entityId, key, oneValue );
            }
        }

        @Override
        public void removeRelationship( long entityId, String key, long startNode, long endNode )
        {
            assertValidKey( key );
            RelationshipData entity = new RelationshipData( entityId, startNode, endNode );
            transaction.remove( this, entity, key );
            addRemoveCommand( entityId, key, null );
        }

        @Override
        public void removeRelationship( long entityId, long startNode, long endNode )
        {
            RelationshipData entity = new RelationshipData( entityId, startNode, endNode );
            transaction.remove( this, entity );
            addRemoveCommand( entityId, null, null );
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
