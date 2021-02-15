/*
 * Copyright (c) "Neo4j"
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

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.isEventuallyConsistent;

public class FulltextIndexReader implements ValueIndexReader
{
    static final LongPredicate ALWAYS_FALSE = value -> false;
    private final List<SearcherReference> searchers;
    private final TokenHolder propertyKeyTokenHolder;
    private final IndexDescriptor index;
    private final Analyzer analyzer;
    private final String[] propertyNames;
    private final FulltextIndexTransactionState transactionState;

    FulltextIndexReader( List<SearcherReference> searchers, TokenHolder propertyKeyTokenHolder, IndexDescriptor descriptor, Config config,
            Analyzer analyzer, String[] propertyNames )
    {
        this.searchers = searchers;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.index = descriptor;
        this.analyzer = analyzer;
        this.propertyNames = propertyNames;
        this.transactionState = new FulltextIndexTransactionState( descriptor, config, analyzer, propertyNames );
    }

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexQueryConstraints constraints,
            PropertyIndexQuery... queries ) throws IndexNotApplicableKernelException
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for ( PropertyIndexQuery indexQuery : queries )
        {
            if ( indexQuery.type() == PropertyIndexQuery.IndexQueryType.fulltextSearch )
            {
                PropertyIndexQuery.FulltextSearchPredicate fulltextSearch = (PropertyIndexQuery.FulltextSearchPredicate) indexQuery;
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
                // Not fulltext query
                assertNotComposite( queries );
                assertCypherCompatible();
                Query query;
                if ( indexQuery.type() == PropertyIndexQuery.IndexQueryType.stringContains )
                {
                    PropertyIndexQuery.StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) indexQuery;
                    String searchTerm = QueryParser.escape( scp.contains().stringValue() );
                    Term term = new Term( propertyNames[0], "*" + searchTerm + "*" );
                    query = new WildcardQuery( term );
                }
                else if ( indexQuery.type() == PropertyIndexQuery.IndexQueryType.stringSuffix )
                {
                    PropertyIndexQuery.StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) indexQuery;
                    String searchTerm = QueryParser.escape( ssp.suffix().stringValue() );
                    Term term = new Term( propertyNames[0], "*" + searchTerm );
                    query = new WildcardQuery( term );
                }
                else if ( indexQuery.type() == PropertyIndexQuery.IndexQueryType.stringPrefix )
                {
                    PropertyIndexQuery.StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) indexQuery;
                    String searchTerm = spp.prefix().stringValue();
                    Term term = new Term( propertyNames[0], searchTerm );
                    query = new LuceneDocumentStructure.PrefixMultiTermsQuery( term );
                }
                else if ( indexQuery.getClass() == PropertyIndexQuery.ExactPredicate.class && indexQuery.valueGroup() == ValueGroup.TEXT )
                {
                    PropertyIndexQuery.ExactPredicate exact = (PropertyIndexQuery.ExactPredicate) indexQuery;
                    String searchTerm = ((TextValue) exact.value()).stringValue();
                    Term term = new Term( propertyNames[0], searchTerm );
                    query = new ConstantScoreQuery( new TermQuery( term ) );
                }
                else if ( indexQuery.getClass() == PropertyIndexQuery.TextRangePredicate.class )
                {
                    PropertyIndexQuery.TextRangePredicate sp = (PropertyIndexQuery.TextRangePredicate) indexQuery;
                    query = newRangeSeekByStringQuery( propertyNames[0], sp.from(), sp.fromInclusive(), sp.to(), sp.toInclusive() );
                }
                else
                {
                    throw new IndexNotApplicableKernelException(
                            "A fulltext schema index cannot answer " + indexQuery.type() + " queries on " + indexQuery.valueCategory() + " values." );
                }
                queryBuilder.add( query, BooleanClause.Occur.MUST );
            }
        }
        Query query = queryBuilder.build();
        ValuesIterator itr = searchLucene( query, constraints, context, context.cursorTracer(), context.memoryTracker() );
        IndexProgressor progressor = new FulltextIndexProgressor( itr, client, constraints );
        client.initialize( index, progressor, queries, constraints, true );
    }

    /**
     * When matching entities in the fulltext index there are some special cases that makes it hard to check that entities
     * actually have the expected property values. To match we use the entityId and only take entries that doesn't contain any
     * unexpected properties. But we don't check that expected properties are present, see
     * {@link LuceneFulltextDocumentStructure#newCountEntityEntriesQuery} for more details.
     */
    @Override
    public long countIndexedEntities( long entityId, PageCursorTracer cursorTracer, int[] propertyKeyIds, Value... propertyValues )
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
                Query query = LuceneFulltextDocumentStructure.newCountEntityEntriesQuery( entityId, propertyKeys, propertyValues );
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

    private Query parseFulltextQuery( String query ) throws ParseException
    {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( propertyNames, analyzer );
        multiFieldQueryParser.setAllowLeadingWildcard( true );
        return multiFieldQueryParser.parse( query );
    }

    private ValuesIterator searchLucene( Query query, IndexQueryConstraints constraints, QueryContext context, PageCursorTracer cursorTracer,
            MemoryTracker memoryTracker )
    {
        try
        {
            // We are replicating the behaviour of IndexSearcher.search(Query, Collector), which starts out by re-writing the query,
            // then creates a weight based on the query and index reader context, and then we finally search the leaf contexts with
            // the weight we created.
            // The query rewrite does not really depend on any data in the index searcher (we don't produce such queries), so it's fine
            // that we only rewrite the query once with the first searcher in our partition list.
            query = searchers.get( 0 ).getIndexSearcher().rewrite( query );
            boolean includeTransactionState = context.getTransactionStateOrNull() != null && !isEventuallyConsistent( index );
            // If we have transaction state, then we need to make our result collector filter out all results touched by the transaction state.
            // The reason we filter them out entirely, is that we will query the transaction state separately.
            LongPredicate filter = includeTransactionState ? transactionState.isModifiedInTransactionPredicate() : ALWAYS_FALSE;
            List<PreparedSearch> searches = new ArrayList<>( searchers.size() + 1 );
            for ( SearcherReference searcher : searchers )
            {
                Neo4jIndexSearcher indexSearcher = searcher.getIndexSearcher();
                searches.add( new PreparedSearch( indexSearcher, filter ) );
            }
            if ( includeTransactionState )
            {
                SearcherReference reference = transactionState.maybeUpdate( context, cursorTracer, memoryTracker );
                searches.add( new PreparedSearch( reference.getIndexSearcher(), ALWAYS_FALSE ) );
            }

            // The StatsCollector aggregates index statistics across all our partitions.
            // Weights created based on these statistics will produce scores that are comparable across partitions.
            StatsCollector statsCollector = new StatsCollector( searches );
            List<ValuesIterator> results = new ArrayList<>( searches.size() );

            for ( PreparedSearch search : searches )
            {
                // Weights are bonded with the top IndexReaderContext of the index searcher that they are created for.
                // That's why we have to create a new StatsCachingIndexSearcher, and a new weight, for every index partition.
                // However, the important thing is that we re-use the statsCollector.
                StatsCachingIndexSearcher statsCachingIndexSearcher = new StatsCachingIndexSearcher( search, statsCollector );
                Weight weight = statsCachingIndexSearcher.createWeight( query, ScoreMode.COMPLETE, 1 );
                results.add( search.search( weight, constraints ) );
            }

            return ScoreEntityIterator.mergeIterators( results );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String getPropertyKeyName( int propertyKey ) throws TokenNotFoundException
    {
        return propertyKeyTokenHolder.getTokenById( propertyKey ).name();
    }

    private static void assertNotComposite( PropertyIndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new IllegalStateException( "composite indexes not yet supported for this operation" );
        }
    }

    private static Query newRangeSeekByStringQuery( String propertyName, String lower, boolean includeLower, String upper, boolean includeUpper )
    {
        boolean includeLowerBoundary = StringUtils.EMPTY.equals( lower ) || includeLower;
        boolean includeUpperBoundary = StringUtils.EMPTY.equals( upper ) || includeUpper;
        TermRangeQuery termRangeQuery =
                TermRangeQuery.newStringRange( propertyName, lower, upper, includeLowerBoundary, includeUpperBoundary );

        if ( (includeLowerBoundary != includeLower) || (includeUpperBoundary != includeUpper) )
        {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            if ( includeLowerBoundary != includeLower )
            {
                builder.add( new TermQuery( new Term( propertyName, lower ) ), BooleanClause.Occur.MUST_NOT );
            }
            if ( includeUpperBoundary != includeUpper )
            {
                builder.add( new TermQuery( new Term( propertyName, upper ) ), BooleanClause.Occur.MUST_NOT );
            }
            builder.add( termRangeQuery, BooleanClause.Occur.FILTER );
            return new ConstantScoreQuery( builder.build() );
        }
        return termRangeQuery;
    }

    private void assertCypherCompatible()
    {
        String reason = "";
        Object configuredAnalyzer = index.getIndexConfig().get( FulltextIndexSettingsKeys.ANALYZER ).asObject();
        if ( !"cypher".equals( configuredAnalyzer ) || !(analyzer.getClass() == KeywordAnalyzer.class) )
        {
            reason = "configured analyzer '" + configuredAnalyzer + "' is not Cypher compatible";
        }
        else if ( !(propertyNames.length == 1) )
        {
            reason = "index is composite";
        }
        else if ( !(index.schema().entityType() == EntityType.NODE) )
        {
            reason = "index does not target nodes";
        }
        else if ( !(index.schema().getEntityTokenIds().length == 1) )
        {
            reason = "index target more than one label";
        }
        else if ( isEventuallyConsistent( index ) )
        {
            reason = "index is eventually consistent";
        }

        if ( !reason.equals( "" ) )
        {
            throw new IllegalStateException( "This fulltext index does not have support for Cypher semantics because " + reason + "." );
        }
    }
}
