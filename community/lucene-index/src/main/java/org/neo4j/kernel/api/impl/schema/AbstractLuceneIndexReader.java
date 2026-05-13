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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher.InRangeEntityConsumer;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class AbstractLuceneIndexReader implements ValueIndexReader {
    private final LuceneQueryFactory queryFactory;
    protected final IndexUsageTracking usageTracker;
    protected final IndexDescriptor descriptor;
    protected final Log log;

    public AbstractLuceneIndexReader(
            IndexDescriptor descriptor,
            IndexUsageTracking usageTracker,
            LuceneQueryFactory queryFactory,
            LogProvider logProvider) {
        this.descriptor = descriptor;
        this.usageTracker = usageTracker;
        this.queryFactory = queryFactory;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext queryContext,
            CursorContext cursorContext,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        PropertyIndexQuery predicate = validateSingleQuery(constraints, predicates);
        reportIndexQueried(queryContext, predicates);

        IndexProgressor progressor = indexProgressor(queryFactory, constraints, client, predicates);
        boolean needStoreFilter = needStoreFilter(predicate);
        client.initializeQuery(descriptor, progressor, false, needStoreFilter, constraints, predicate);
    }

    @Override
    public void reportIndexQueried(QueryContext context, PropertyIndexQuery... predicates) {
        context.monitor().queried(descriptor);
        usageTracker.queried();
    }

    @Override
    public void validateQuery(IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        if (predicates.length > 1) {
            throw invalidCompositeQuery(
                    msg -> IndexNotApplicableKernelException.indexNotApplicable(log, descriptor.getName(), msg),
                    predicates);
        }

        validatePrimaryPredicate(predicates[0]);
    }

    protected void validatePrimaryPredicate(PropertyIndexQuery primaryPredicate)
            throws IndexNotApplicableKernelException {
        if (!descriptor.getCapability().isQuerySupported(primaryPredicate.type(), primaryPredicate.valueCategory())) {
            throw invalidQuery(
                    msg -> IndexNotApplicableKernelException.indexNotApplicable(log, descriptor.getName(), msg),
                    primaryPredicate);
        }
    }

    protected PropertyIndexQuery validateSingleQuery(
            IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        validateQuery(constraints, predicates);
        return predicates[0];
    }

    protected <E extends Exception> E invalidCompositeQuery(
            Function<String, E> constructor, PropertyIndexQuery... predicates) {
        IndexType indexType = descriptor.getIndexType();
        return constructor.apply(("Tried to query a %s index with a composite query. "
                        + "Composite queries are not supported by a %s index. "
                        + "Query was: %s ")
                .formatted(indexType, indexType, Arrays.toString(predicates)));
    }

    protected <E extends Exception> E invalidQuery(Function<String, E> constructor, PropertyIndexQuery predicate) {
        IndexType indexType = descriptor.getIndexType();
        return constructor.apply("Index query not supported for %s index. Query: %s".formatted(indexType, predicate));
    }

    protected <E extends Exception> E invalidVectorQueryProperty(
            Function<String, E> constructor, PropertyIndexQuery invalidPredicate, PropertyIndexQuery... predicates) {
        IndexType indexType = descriptor.getIndexType();
        return constructor.apply(("Tried to query a %s index with a query property which is not part of the index. "
                        + "Invalid property predicate was: %s. "
                        + "Query was: %s ")
                .formatted(indexType, invalidPredicate, Arrays.toString(predicates)));
    }

    protected <E extends Exception> E invalidVectorQueryFilter(
            Function<String, E> constructor,
            List<IndexQueryType> validFilterTypes,
            PropertyIndexQuery invalidPredicate,
            PropertyIndexQuery... predicates) {
        IndexType indexType = descriptor.getIndexType();
        String validTypes = String.join(
                " or ", validFilterTypes.stream().map(Enum::toString).toList());
        return constructor.apply(
                ("Tried to query a %s index with a query predicate which is not an accepted filter type "
                                + "(must be of type "
                                + validTypes
                                + "). "
                                + "Invalid filter type was: %s. "
                                + "Invalid predicate was: %s. "
                                + "Index query was: %s ")
                        .formatted(indexType, invalidPredicate.type(), invalidPredicate, Arrays.toString(predicates)));
    }

    protected <E extends Exception> E nullVectorQueryFilter(
            Function<String, E> constructor, List<IndexQueryType> validFilterTypes, PropertyIndexQuery... predicates) {
        IndexType indexType = descriptor.getIndexType();
        String validTypes = String.join(
                " or ", validFilterTypes.stream().map(Enum::toString).toList());
        return constructor.apply(
                ("Tried to query a %s index with a query predicate which is not an accepted filter type "
                                + "(must be of type "
                                + validTypes
                                + "). "
                                + "Invalid filter type was: null."
                                + "Invalid predicate was: null."
                                + "Index query was: %s ")
                        .formatted(indexType, Arrays.toString(predicates)));
    }

    protected abstract IndexProgressor indexProgressor(
            LuceneQueryFactory query,
            IndexQueryConstraints constraints,
            IndexProgressor.EntityValueClient client,
            PropertyIndexQuery... predicates);

    protected abstract String entityIdFieldKey();

    protected abstract boolean needStoreFilter(PropertyIndexQuery predicate);

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    protected static BoundedIterable<Long> newAllEntriesValueReaderForPartition(
            String field,
            LuceneIndexSearcher searcher,
            LuceneQueryContext queryContext,
            long fromIdInclusive,
            long toIdExclusive) {
        try {
            InRangeEntityConsumer entityConsumer = new InRangeEntityConsumer(fromIdInclusive, toIdExclusive);
            IndexProgressor indexProgressor = searcher.searchDocValues(queryContext, field, entityConsumer);
            return new AllEntriesValueReaderForPartition(indexProgressor, entityConsumer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected final String indexName() {
        return descriptor.getName();
    }

    private record AllEntriesValueReaderForPartition(
            IndexProgressor indexProgressor, InRangeEntityConsumer entityConsumer) implements BoundedIterable<Long> {

        @Override
        public Iterator<Long> iterator() {
            return new PrefetchingIterator<>() {
                @Override
                protected Long fetchNextOrNull() {
                    return indexProgressor.next() ? entityConsumer.reference() : null;
                }
            };
        }

        @Override
        public long maxCount() {
            return UNKNOWN_MAX_COUNT;
        }

        @Override
        public void close() {
            indexProgressor.close();
        }
    }
}
