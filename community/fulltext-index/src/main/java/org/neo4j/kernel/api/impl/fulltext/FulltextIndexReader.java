/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.storageengine.api.schema.QueryContext;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class FulltextIndexReader implements IndexReader
{
    private final List<SearcherReference> searchers;
    private final TokenHolder propertyKeyTokenHolder;
    private final FulltextIndexDescriptor descriptor;
    private final FulltextIndexTransactionState transactionState;

    FulltextIndexReader( List<SearcherReference> searchers, TokenHolder propertyKeyTokenHolder, FulltextIndexDescriptor descriptor )
    {
        this.searchers = searchers;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.descriptor = descriptor;
        this.transactionState = new FulltextIndexTransactionState( descriptor );
    }

    private Query parseFulltextQuery( String query ) throws ParseException
    {
        FulltextIndexDescriptor descriptor = getDescriptor();
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( descriptor.propertyNames(), descriptor.analyzer() );
        return multiFieldQueryParser.parse( query );
    }

    private ScoreEntityIterator indexQuery( Query query )
    {
        List<ScoreEntityIterator> results = new ArrayList<>();
        for ( SearcherReference searcher : searchers )
        {
            ScoreEntityIterator iterator = searchLucene( searcher, query );
            results.add( iterator );
        }
        return ScoreEntityIterator.mergeIterators( results );
    }

    static ScoreEntityIterator searchLucene( SearcherReference searcher, Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            searcher.getIndexSearcher().search( query, docValuesCollector );
            ValuesIterator sortedValuesIterator =
                    docValuesCollector.getSortedValuesIterator( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID, Sort.RELEVANCE );
            return new ScoreEntityIterator( sortedValuesIterator );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public boolean indexIncludesTransactionState()
    {
        return true;
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... queries )
            throws IndexNotApplicableKernelException
    {
        QueryBuilder queryFactory = new QueryBuilder( getDescriptor().analyzer() );
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for ( IndexQuery indexQuery : queries )
        {
            if ( indexQuery.type() == IndexQuery.IndexQueryType.fulltextSearch )
            {
                IndexQuery.FulltextSearchPredicate fulltextSearch = (IndexQuery.FulltextSearchPredicate) indexQuery;
                try
                {
                    queryBuilder.add( parseFulltextQuery( fulltextSearch.query() ), BooleanClause.Occur.SHOULD );
                }
                catch ( ParseException e )
                {
                    throw new RuntimeException( "Could not parse the given fulltext search query: '" + fulltextSearch.query() + "'.", e );
                }
                continue;
            }
            String propertyKeyName;
            String searchTerm;
            try
            {
                propertyKeyName = getPropertyKeyName( indexQuery.propertyKeyId() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new IndexNotApplicableKernelException( "No property key name found for property key token: " + indexQuery.propertyKeyId() );
            }

            switch ( indexQuery.type() )
            {
            case exact:
                IndexQuery.ExactPredicate predicate = (IndexQuery.ExactPredicate) indexQuery;
                if ( predicate.valueGroup() != ValueGroup.TEXT )
                {
                    throw new IndexNotApplicableKernelException( "A fulltext schema index cannot be used to search for exact matches of non-string typed " +
                            "values, but the given value was in type-group " + predicate.valueGroup() + "." );
                }
                String stringValue = predicate.value().asObject().toString();
                searchTerm = QueryParser.escape( stringValue );
                queryBuilder.add( queryFactory.createBooleanQuery( propertyKeyName, searchTerm ), BooleanClause.Occur.SHOULD );
                break;
            case stringContains:
                searchTerm = QueryParser.escape( ((IndexQuery.StringContainsPredicate) indexQuery).contains() );
                queryBuilder.add( queryFactory.createBooleanQuery( propertyKeyName, "*" + searchTerm + "*" ), BooleanClause.Occur.SHOULD );
                break;
            case stringPrefix:
                searchTerm = QueryParser.escape( ((IndexQuery.StringPrefixPredicate) indexQuery).prefix() );
                queryBuilder.add( queryFactory.createBooleanQuery( propertyKeyName, searchTerm + "*" ), BooleanClause.Occur.SHOULD );
                break;
            case stringSuffix:
                searchTerm = QueryParser.escape( ((IndexQuery.StringSuffixPredicate) indexQuery).suffix() );
                queryBuilder.add( queryFactory.createBooleanQuery( propertyKeyName, "*" + searchTerm ), BooleanClause.Occur.SHOULD );
                break;
            default:
                throw new IndexNotApplicableKernelException( "A fulltext schema index cannot answer " + indexQuery.type() + " queries." );
            }
        }
        BooleanQuery query = queryBuilder.build();
        ScoreEntityIterator itr = indexQuery( query );
        ReadableTransactionState state = context.getTransactionStateOrNull();
        if ( state != null && !descriptor.isEventuallyConsistent() )
        {
            transactionState.maybeUpdate( context );
            itr = transactionState.filter( itr, query );
        }
        IndexProgressor progressor = new FulltextIndexProgressor( itr, client );
        client.initialize( getDescriptor(), progressor, queries, indexOrder, needsValues, true );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        long count = 0;
        for ( SearcherReference searcher : searchers )
        {
            try
            {
                String[] propertyKeys = new String[propertyKeyIds.length];
                for ( int i = 0; i < propertyKeyIds.length; i++ )
                {
                    propertyKeys[i] = getPropertyKeyName( propertyKeyIds[i] );
                }
                Query query = LuceneFulltextDocumentStructure.newCountNodeEntriesQuery( nodeId, propertyKeys, propertyValues );
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.getIndexSearcher().search( query, collector );
                count += collector.getTotalHits();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
        return count;
    }

    @Override
    public void close()
    {
        List<AutoCloseable> resources = new ArrayList<>( searchers.size() + 1 );
        resources.addAll( searchers );
        resources.add( transactionState );
        IOUtils.close( IndexReaderCloseException::new, resources );
    }

    private String getPropertyKeyName( int propertyKey ) throws TokenNotFoundException
    {
        return propertyKeyTokenHolder.getTokenById( propertyKey ).name();
    }

    private FulltextIndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    private static class FulltextIndexProgressor implements IndexProgressor
    {
        private final ScoreEntityIterator itr;
        private final EntityValueClient client;

        private FulltextIndexProgressor( ScoreEntityIterator itr, EntityValueClient client )
        {
            this.itr = itr;
            this.client = client;
        }

        @Override
        public boolean next()
        {
            if ( !itr.hasNext() )
            {
                return false;
            }
            ScoreEntityIterator.ScoreEntry entry;
            boolean accepted;
            do
            {
                entry = itr.next();
                accepted = client.acceptEntity( entry.entityId(), entry.score(), (Value[]) null );
            }
            while ( !accepted && itr.hasNext() );
            return accepted;
        }

        @Override
        public void close()
        {
        }
    }
}
