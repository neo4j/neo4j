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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.kernel.api.impl.schema.LuceneQueryFactory.propertyFiltersForAll;
import static org.neo4j.kernel.api.impl.schema.LuceneQueryFactory.propertyFiltersForEach;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityIterator;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexReader;
import org.neo4j.kernel.api.impl.schema.LuceneQueryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneScoredEntityIndexProgressor;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityValueClient;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.storable.Value;

class VectorIndexReader extends AbstractLuceneIndexReader {
    private final OptionalInt dimensions;
    private final List<SearcherReference> searchers;
    private final int maxEfSearch;

    VectorIndexReader(
            IndexDescriptor descriptor,
            VectorIndexConfig vectorIndexConfig,
            VectorDocumentStructure documentStructure,
            int maxEfSearch,
            List<SearcherReference> searchers,
            IndexUsageTracking usageTracker,
            LogProvider logProvider) {
        super(
                descriptor,
                usageTracker,
                new LuceneQueryFactory.VectorQueryFactory(
                        documentStructure,
                        vectorIndexConfig.quantization(),
                        vectorIndexConfig.defaultSearchExpansionFactor(),
                        maxEfSearch),
                logProvider);
        this.dimensions = vectorIndexConfig.dimensions();
        this.searchers = searchers;
        this.maxEfSearch = maxEfSearch;
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        // TODO VECTOR: Currently only checks entity is in the index; it does not check the value is the same.
        //              Investigate finding a method to extract out the value itself from the index
        //              LeafReader::getFloatVectorValues seems promising with something like DocValuesCollector.
        //              Otherwise, perhaps k-ANN of k=1, filter=getById, (score-1) < epsilon?

        if (searchers.isEmpty()) {
            return 0;
        }
        long count = 0L;
        LuceneQueryContext queryContext = searchers
                .getFirst()
                .getIndexSearcher()
                .newQueryContext()
                .exactTerm(LuceneDocumentsFactory.ENTITY_ID_KEY, entityId);
        for (SearcherReference searcher : searchers) {
            try {
                count += searcher.getIndexSearcher().count(queryContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return count;
    }

    @Override
    public IndexSampler createSampler() {
        return IndexSampler.EMPTY;
    }

    @Override
    public void query(
            EntityValueClient client,
            QueryContext queryContext,
            CursorContext cursorContext,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        super.query(client, queryContext, cursorContext, adjustedConstraints(constraints, predicates), predicates);
    }

    @Override
    public void reportIndexQueried(QueryContext context, PropertyIndexQuery... predicates) {
        context.monitor().queried(descriptor);
        boolean queriedWitFilter = propertyFiltersForAll(predicates, p -> p.type() != IndexQueryType.ALL);
        if (queriedWitFilter) {
            usageTracker.queriedWithFilter();
        } else {
            usageTracker.queried();
        }
    }

    @Override
    public void validateQuery(IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        validatePrimaryPredicate(predicates[0]);
        validateFilteredQueryPredicates(predicates);
    }

    @Override
    public PropertyIndexQuery validateSingleQuery(IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        PropertyIndexQuery predicate = super.validateSingleQuery(constraints, predicates);
        if (predicate instanceof NearestNeighborsPredicate nearestNeighbour) {
            float[] queryVector = nearestNeighbour.query();
            if (dimensions.isPresent() && queryVector.length != dimensions.getAsInt()) {
                throw IndexNotApplicableKernelException.vectorIndexDimensionalityMismatch(
                        indexName(), dimensions.getAsInt(), queryVector.length);
            }
        }
        return predicate;
    }

    private static final List<IndexQueryType> validIndexQueryTypes = List.of(
            IndexQueryType.ALL,
            IndexQueryType.EXISTS,
            IndexQueryType.NOT_EXISTS,
            IndexQueryType.EXACT,
            IndexQueryType.RANGE,
            IndexQueryType.IN_SET);

    private void validateFilteredQueryPredicates(PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        propertyFiltersForEach(predicates, predicate -> {
            if (predicate == null) {
                throw nullVectorQueryFilter(
                        msg -> IndexNotApplicableKernelException.indexNotApplicable(log, descriptor.getName(), msg),
                        validIndexQueryTypes,
                        predicates);
            }
            IndexQueryType type = predicate.type();
            switch (type) {
                case ALL, EXISTS, NOT_EXISTS, EXACT, RANGE, IN_SET -> {
                    // Each filter predicate is independent of others;
                    // so we can support arbitrary combinations range and exact predicates
                }
                default ->
                    throw invalidVectorQueryFilter(
                            msg -> IndexNotApplicableKernelException.indexNotApplicable(log, descriptor.getName(), msg),
                            validIndexQueryTypes,
                            predicate,
                            predicates);
            }
        });
    }

    private IndexQueryConstraints adjustedConstraints(
            IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        return validateSingleQuery(constraints, predicates) instanceof NearestNeighborsPredicate nearestNeighbor
                ? constraints.limit(nearestNeighbor.numberOfNeighbors(constraints, maxEfSearch))
                : constraints;
    }

    @Override
    protected IndexProgressor indexProgressor(
            LuceneQueryFactory queryFactory,
            IndexQueryConstraints constraints,
            EntityValueClient client,
            PropertyIndexQuery... predicates) {
        ValuesIterator iterator;
        if (searchers.isEmpty()) {
            iterator = ValuesIterator.EMPTY;
        } else {
            iterator = searchLucene(
                    queryFactory.createQuery(
                            searchers.getFirst().getIndexSearcher(), constraints, descriptor, predicates),
                    constraints);
        }
        return new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
    }

    @Override
    protected String entityIdFieldKey() {
        return LuceneDocumentsFactory.ENTITY_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        // We can't do filtering of false positives after the fact because we would
        // need to know which neighbors we missed to do so. We don't know what we don't know.
        return false;
    }

    @Override
    public void close() {
        AutoCloseables<IndexReaderCloseException> closeables =
                new AutoCloseables<>(IndexReaderCloseException::new, searchers);
        try (closeables) {
            super.close();
        }
    }

    private ValuesIterator searchLucene(LuceneQueryContext queryContext, IndexQueryConstraints constraints) {
        // TODO VECTOR: FulltextIndexReader handles transaction state in a similar way
        //              with QueryContext, CursorContext, MemoryTracker
        try {
            // TODO VECTOR: pre-rewrite query? Not sure what rewriting entails
            List<ValuesIterator> results = new ArrayList<>(searchers.size());
            for (SearcherReference searcher : searchers) {
                ValuesIterator valuesIterator = searcher.getIndexSearcher().searchVectors(queryContext, constraints);
                results.add(valuesIterator);
            }
            return ScoredEntityIterator.mergeIterators(results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    BoundedIterable<Long> newAllEntriesValueReader(long fromIdInclusive, long toIdExclusive) {
        if (searchers.isEmpty()) {
            return BoundedIterable.empty();
        }
        String field = LuceneDocumentsFactory.ENTITY_ID_KEY;
        LuceneQueryContext queryContext =
                searchers.getFirst().getIndexSearcher().newQueryContext().matchAll();
        Collection<BoundedIterable<Long>> iterables = new ArrayList<>(searchers.size());
        for (SearcherReference searcher : searchers) {
            iterables.add(newAllEntriesValueReaderForPartition(
                    field, searcher.getIndexSearcher(), queryContext, fromIdInclusive, toIdExclusive));
        }
        return BoundedIterable.concat(iterables);
    }
}
