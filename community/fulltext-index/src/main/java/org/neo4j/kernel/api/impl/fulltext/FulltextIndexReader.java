/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.isEventuallyConsistent;

public class FulltextIndexReader implements IndexReader
{
    private final List<SearcherReference> searchers;
    private final TokenHolder propertyKeyTokenHolder;
    private final IndexDescriptor descriptor;
    private final Analyzer analyzer;
    private final String[] propertyNames;
    private final FulltextIndexTransactionState transactionState;

    FulltextIndexReader( List<SearcherReference> searchers, TokenHolder propertyKeyTokenHolder, IndexDescriptor descriptor,
            Analyzer analyzer, String[] propertyNames )
    {
        this.searchers = searchers;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.descriptor = descriptor;
        this.analyzer = analyzer;
        this.propertyNames = propertyNames;
        this.transactionState = new FulltextIndexTransactionState( descriptor, analyzer, propertyNames );
    }

    private Query parseFulltextQuery( String query ) throws ParseException
    {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( propertyNames, analyzer );
        multiFieldQueryParser.setAllowLeadingWildcard( true );
        return multiFieldQueryParser.parse( query );
    }

    private ValuesIterator indexQuery( Query query )
    {
        List<ValuesIterator> results = new ArrayList<>();
        for ( SearcherReference searcher : searchers )
        {
            ValuesIterator iterator = searchLucene( searcher, query );
            results.add( iterator );
        }
        return ScoreEntityIterator.mergeIterators( results );
    }

    static ValuesIterator searchLucene( SearcherReference searcher, Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            searcher.getIndexSearcher().search( query, docValuesCollector );
            return docValuesCollector.getValuesSortedByRelevance( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID );
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
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... queries )
            throws IndexNotApplicableKernelException
    {
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
            }
            else
            {
                throw new IndexNotApplicableKernelException( "A fulltext schema index cannot answer " + indexQuery.type() + " queries." );
            }
        }
        BooleanQuery query = queryBuilder.build();
        ValuesIterator itr = indexQuery( query );
        ReadableTransactionState state = context.getTransactionStateOrNull();
        if ( state != null && !isEventuallyConsistent( descriptor.schema() ) )
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
    public void distinctValues( IndexProgressor.EntityValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        throw new UnsupportedOperationException( "Fulltext indexes does not support distinctValues queries" );
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

    private IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    private static class FulltextIndexProgressor implements IndexProgressor
    {
        private final ValuesIterator itr;
        private final EntityValueClient client;

        private FulltextIndexProgressor( ValuesIterator itr, EntityValueClient client )
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
            boolean accepted;
            do
            {
                long entityId = itr.next();
                float score = itr.currentScore();
                accepted = client.acceptEntity( entityId, score, (Value[]) null );
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
