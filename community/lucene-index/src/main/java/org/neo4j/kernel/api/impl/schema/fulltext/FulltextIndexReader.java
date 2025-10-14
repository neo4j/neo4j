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
package org.neo4j.kernel.api.impl.schema.fulltext;

import static org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexSettings.isEventuallyConsistent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.FulltextSearchPredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LucenePartitionedSearch;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryParseException;
import org.neo4j.kernel.api.impl.schema.LuceneScoredEntityIndexProgressor;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
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
    private final Log log;

    FulltextIndexReader(
            List<SearcherReference> searchers,
            LuceneContext luceneContext,
            TokenHolder propertyKeyTokenHolder,
            IndexDescriptor descriptor,
            Config config,
            Analyzer analyzer,
            String[] propertyNames,
            IndexUsageTracking usageTracker,
            LogProvider logProvider) {
        this.searchers = searchers;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.index = descriptor;
        this.analyzer = analyzer;
        this.propertyNames = propertyNames;
        this.usageTracker = usageTracker;
        this.transactionState =
                new FulltextIndexTransactionState(luceneContext, descriptor, config, analyzer, propertyNames);
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public IndexSampler createSampler() {
        return IndexSampler.EMPTY;
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext queryContext,
            CursorContext cursorContext,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        validateQuery(constraints, queries);
        final var predicate = queries[0];
        ValuesIterator iterator;
        if (searchers.isEmpty()) {
            // We are replicating the behaviour of IndexSearcher.search(Query, Collector), which starts out by
            // re-writing the query, then creates a weight based on the query and index reader context, and then
            // we finally search the leaf contexts with the weight we created. The query rewrite does not really
            // depend on any data in the index searcher (we don't produce such queries), so it's fine that we
            // only rewrite the query once with the first searcher in our partition list.
            iterator = ValuesIterator.EMPTY;
        } else {
            iterator = searchLucene(
                    toLuceneQuery(predicate),
                    constraints,
                    queryContext,
                    queryContext.cursorContext(),
                    queryContext.memoryTracker());
        }

        queryContext.monitor().queried(index);
        usageTracker.queried();

        final var progressor = new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
        client.initializeQuery(index, progressor, true, false, constraints, queries);
    }

    @Override
    public void validateQuery(IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        if (predicates.length > 1) {
            throw invalidCompositeQuery(
                    msg -> IndexNotApplicableKernelException.indexNotApplicable(log, index.getName(), msg), predicates);
        }

        final var predicate = predicates[0];
        if (!index.getCapability().isQuerySupported(predicate.type(), predicate.valueCategory())) {
            throw invalidQuery(
                    msg -> IndexNotApplicableKernelException.indexNotApplicable(log, index.getName(), msg), predicate);
        }
    }

    private <E extends Exception> E invalidCompositeQuery(
            Function<String, E> constructor, PropertyIndexQuery... predicates) {
        final var indexType = index.getIndexType();
        return constructor.apply(("Tried to query a %s index with a composite query. "
                        + "Composite queries are not supported by a %s index. "
                        + "Query was: %s ")
                .formatted(indexType, indexType, Arrays.toString(predicates)));
    }

    private LuceneQueryContext toLuceneQuery(PropertyIndexQuery predicate) {
        LuceneIndexSearcher indexSearcher = searchers.getFirst().getIndexSearcher();
        return switch (predicate.type()) {
            case ALL_ENTRIES -> indexSearcher.newQueryContext().matchAll();
            case FULLTEXT_SEARCH -> {
                final var fulltextSearchPredicate = (FulltextSearchPredicate) predicate;
                try {
                    // todo: is the boolean query needed?
                    LuceneQueryContext queryContext = indexSearcher.newQueryContext();

                    String queryAnalyzer = fulltextSearchPredicate.queryAnalyzer();
                    Analyzer actualQueryAnalyzer = queryAnalyzer != null
                            ? FulltextIndexAnalyzerLoader.INSTANCE.createAnalyzerFromString(queryAnalyzer)
                            : analyzer;

                    queryContext.addShouldQueryText(
                            fulltextSearchPredicate.query(), propertyNames, actualQueryAnalyzer);

                    yield queryContext;
                } catch (LuceneQueryParseException parseException) {
                    throw new RuntimeException(
                            "Could not parse the given fulltext search query: '%s'."
                                    .formatted(fulltextSearchPredicate.query()),
                            parseException);
                }
            }
            default -> throw invalidQuery(IllegalArgumentException::new, predicate);
        };
    }

    private <E extends Exception> E invalidQuery(Function<String, E> constructor, PropertyIndexQuery query) {
        return constructor.apply("A fulltext schema index cannot answer %s queries on %s values."
                .formatted(query.type(), query.valueCategory()));
    }

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexUsageTracking usageTracking() {
        return usageTracker;
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
                LuceneIndexSearcher indexSearcher = searcher.getIndexSearcher();
                LuceneQueryContext queryContext = LuceneFulltextDocumentStructure.newCountEntityEntriesQuery(
                        indexSearcher, entityId, propertyKeys, propertyValues);
                count += indexSearcher.count(queryContext);
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

    private ValuesIterator searchLucene(
            LuceneQueryContext queryContext,
            IndexQueryConstraints constraints,
            QueryContext context,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        try {
            LuceneIndexSearcher firstSearcher = searchers.getFirst().getIndexSearcher();
            queryContext = firstSearcher.rewrite(queryContext);
            boolean includeTransactionState =
                    context.getTransactionStateOrNull() != null && !isEventuallyConsistent(index);
            // If we have transaction state, then we need to make our result collector filter out all results touched by
            // the transaction state.
            // The reason we filter them out entirely, is that we will query the transaction state separately.
            LongPredicate filter =
                    includeTransactionState ? transactionState.isModifiedInTransactionPredicate() : ALWAYS_FALSE;

            LucenePartitionedSearch partitionedSearch = firstSearcher.newPartitionedSearcher(searchers.size() + 1);
            for (SearcherReference searcher : searchers) {
                partitionedSearch.addPartitionSearcher(searcher.getIndexSearcher(), filter);
            }
            if (includeTransactionState) {
                SearcherReference reference = transactionState.maybeUpdate(context, cursorContext, memoryTracker);
                partitionedSearch.addPartitionSearcher(reference.getIndexSearcher(), ALWAYS_FALSE);
            }

            return partitionedSearch.search(queryContext, constraints);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getPropertyKeyName(int propertyKey) throws TokenNotFoundException {
        return propertyKeyTokenHolder.getTokenById(propertyKey).name();
    }
}
