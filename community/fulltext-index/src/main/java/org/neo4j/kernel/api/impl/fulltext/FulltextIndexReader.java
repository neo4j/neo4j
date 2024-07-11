/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.isEventuallyConsistent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityIterator;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneScoredEntityIndexProgressor;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

public class FulltextIndexReader implements ValueIndexReader {
    private static final LongPredicate ALWAYS_FALSE = LongPredicates.alwaysFalse();
    private final List<SearcherReference> searchers;
    private final TokenHolder propertyKeyTokenHolder;
    private final IndexDescriptor index;
    private final Analyzer analyzer;
    private final String[] propertyNames;
    private final FulltextIndexTransactionState transactionState;
    private final IndexUsageTracking usageTracker;

    FulltextIndexReader(
            List<SearcherReference> searchers,
            TokenHolder propertyKeyTokenHolder,
            IndexDescriptor descriptor,
            Config config,
            Analyzer analyzer,
            String[] propertyNames,
            IndexUsageTracking usageTracker) {
        this.searchers = searchers;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.index = descriptor;
        this.analyzer = analyzer;
        this.propertyNames = propertyNames;
        this.usageTracker = usageTracker;
        this.transactionState = new FulltextIndexTransactionState(descriptor, config, analyzer, propertyNames);
    }

    @Override
    public IndexSampler createSampler() {
        return IndexSampler.EMPTY;
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (PropertyIndexQuery indexQuery : queries) {
            if (indexQuery.type() == IndexQueryType.FULLTEXT_SEARCH) {
                PropertyIndexQuery.FulltextSearchPredicate fulltextSearch =
                        (PropertyIndexQuery.FulltextSearchPredicate) indexQuery;
                try {
                    queryBuilder.add(
                            parseFulltextQuery(fulltextSearch.query(), fulltextSearch.queryAnalyzer()),
                            BooleanClause.Occur.SHOULD);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            "Could not parse the given fulltext search query: '" + fulltextSearch.query() + "'.", e);
                }
            } else {
                throw new IndexNotApplicableKernelException("A fulltext schema index cannot answer " + indexQuery.type()
                        + " queries on " + indexQuery.valueCategory() + " values.");
            }
        }
        Query query = queryBuilder.build();
        context.monitor().queried(index);
        usageTracker.queried();
        ValuesIterator itr =
                searchLucene(query, constraints, context, context.cursorContext(), context.memoryTracker());
        IndexProgressor progressor = new LuceneScoredEntityIndexProgressor(itr, client, constraints);
        client.initializeQuery(index, progressor, true, false, constraints, queries);
    }

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    /**
     * When matching entities in the fulltext index there are some special cases that makes it hard to check that entities
     * actually have the expected property values. To match we use the entityId and only take entries that doesn't contain any
     * unexpected properties. But we don't check that expected properties are present, see
     * {@link LuceneFulltextDocumentStructure#newCountEntityEntriesQuery} for more details.
     */
    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        long count = 0;
        for (SearcherReference searcher : searchers) {
            try {
                String[] propertyKeys = new String[propertyKeyIds.length];
                for (int i = 0; i < propertyKeyIds.length; i++) {
                    propertyKeys[i] = getPropertyKeyName(propertyKeyIds[i]);
                }
                Query query = LuceneFulltextDocumentStructure.newCountEntityEntriesQuery(
                        entityId, propertyKeys, propertyValues);
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.getIndexSearcher().search(query, collector);
                count += collector.getTotalHits();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return count;
    }

    @Override
    public void close() {
        List<AutoCloseable> resources = new ArrayList<>(searchers.size() + 1);
        resources.addAll(searchers);
        resources.add(transactionState);
        IOUtils.close(IndexReaderCloseException::new, resources);
    }

    private Query parseFulltextQuery(String query, String queryAnalyzer) throws ParseException {
        Analyzer actualQueryAnalyzer = queryAnalyzer != null
                ? FulltextIndexAnalyzerLoader.INSTANCE.createAnalyzerFromString(queryAnalyzer)
                : analyzer;

        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser(propertyNames, actualQueryAnalyzer);
        multiFieldQueryParser.setAllowLeadingWildcard(true);
        return multiFieldQueryParser.parse(query);
    }

    private ValuesIterator searchLucene(
            Query query,
            IndexQueryConstraints constraints,
            QueryContext context,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        try {
            // We are replicating the behaviour of IndexSearcher.search(Query, Collector), which starts out by
            // re-writing the query,
            // then creates a weight based on the query and index reader context, and then we finally search the leaf
            // contexts with
            // the weight we created.
            // The query rewrite does not really depend on any data in the index searcher (we don't produce such
            // queries), so it's fine
            // that we only rewrite the query once with the first searcher in our partition list.
            query = searchers.get(0).getIndexSearcher().rewrite(query);
            boolean includeTransactionState =
                    context.getTransactionStateOrNull() != null && !isEventuallyConsistent(index);
            // If we have transaction state, then we need to make our result collector filter out all results touched by
            // the transaction state.
            // The reason we filter them out entirely, is that we will query the transaction state separately.
            LongPredicate filter =
                    includeTransactionState ? transactionState.isModifiedInTransactionPredicate() : ALWAYS_FALSE;
            List<PreparedSearch> searches = new ArrayList<>(searchers.size() + 1);
            for (SearcherReference searcher : searchers) {
                Neo4jIndexSearcher indexSearcher = searcher.getIndexSearcher();
                searches.add(new PreparedSearch(indexSearcher, filter));
            }
            if (includeTransactionState) {
                SearcherReference reference = transactionState.maybeUpdate(context, cursorContext, memoryTracker);
                searches.add(new PreparedSearch(reference.getIndexSearcher(), ALWAYS_FALSE));
            }

            // The StatsCollector aggregates index statistics across all our partitions.
            // Weights created based on these statistics will produce scores that are comparable across partitions.
            StatsCollector statsCollector = new StatsCollector(searches);
            List<ValuesIterator> results = new ArrayList<>(searches.size());

            for (PreparedSearch search : searches) {
                results.add(search.search(query, constraints, statsCollector));
            }

            return ScoredEntityIterator.mergeIterators(results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getPropertyKeyName(int propertyKey) throws TokenNotFoundException {
        return propertyKeyTokenHolder.getTokenById(propertyKey).name();
    }
}
