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
import java.util.function.Function;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector.InRangeEntityConsumer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;

public abstract class AbstractLuceneIndexReader implements ValueIndexReader {
    private final IndexDescriptor descriptor;
    private final IndexUsageTracking usageTracker;

    public AbstractLuceneIndexReader(IndexDescriptor descriptor, IndexUsageTracking usageTracker) {
        this.descriptor = descriptor;
        this.usageTracker = usageTracker;
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        final var predicate = validateQuery(predicates);
        final var query = toLuceneQuery(predicate, constraints);
        context.monitor().queried(descriptor);
        usageTracker.queried();

        final var progressor = indexProgressor(query, constraints, client);
        final var needStoreFilter = needStoreFilter(predicate);
        client.initializeQuery(descriptor, progressor, false, needStoreFilter, constraints, predicate);
    }

    protected PropertyIndexQuery validateQuery(PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        if (predicates.length > 1) {
            throw invalidCompositeQuery(IndexNotApplicableKernelException::new, predicates);
        }

        final var predicate = predicates[0];
        if (!descriptor.getCapability().isQuerySupported(predicate.type(), predicate.valueCategory())) {
            throw invalidQuery(IndexNotApplicableKernelException::new, predicate);
        }

        return predicate;
    }

    protected abstract Query toLuceneQuery(PropertyIndexQuery predicate, IndexQueryConstraints constraints);

    protected <E extends Exception> E invalidCompositeQuery(
            Function<String, E> constructor, PropertyIndexQuery... predicates) {
        final var indexType = descriptor.getIndexType();
        return constructor.apply(("Tried to query a %s index with a composite query. "
                        + "Composite queries are not supported by a %s index. "
                        + "Query was: %s ")
                .formatted(indexType, indexType, Arrays.toString(predicates)));
    }

    protected <E extends Exception> E invalidQuery(Function<String, E> constructor, PropertyIndexQuery predicate) {
        final var indexType = descriptor.getIndexType();
        return constructor.apply("Index query not supported for %s index. Query: %s".formatted(indexType, predicate));
    }

    protected abstract IndexProgressor indexProgressor(
            Query query, IndexQueryConstraints constraints, IndexProgressor.EntityValueClient client);

    protected abstract String entityIdFieldKey();

    protected abstract boolean needStoreFilter(PropertyIndexQuery predicate);

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    protected DocValuesCollector search(IndexSearcher searcher, Query query) {
        try {
            final var docValuesCollector = new DocValuesCollector();
            searcher.search(query, docValuesCollector);
            return docValuesCollector;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected BoundedIterable<Long> newAllEntriesValueReaderForPartition(
            String field, IndexSearcher searcher, Query query, long fromIdInclusive, long toIdExclusive) {
        final var collector = search(searcher, query);
        final var entityConsumer = new InRangeEntityConsumer(fromIdInclusive, toIdExclusive);
        final var indexProgressor = collector.getIndexProgressor(field, entityConsumer);
        return new AllEntriesValueReaderForPartition(indexProgressor, entityConsumer);
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
