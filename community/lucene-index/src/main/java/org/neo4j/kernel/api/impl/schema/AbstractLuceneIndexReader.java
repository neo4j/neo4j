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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector.InRangeEntityConsumer;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;

public abstract class AbstractLuceneIndexReader implements ValueIndexReader {
    protected final IndexDescriptor descriptor;
    protected final SearcherReference searcherReference;
    protected final IndexSamplingConfig samplingConfig;
    protected final TaskCoordinator taskCoordinator;
    protected final IndexUsageTracker usageTracker;
    protected final boolean keepScores;

    public AbstractLuceneIndexReader(
            IndexDescriptor descriptor,
            SearcherReference searcherReference,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator,
            IndexUsageTracker usageTracker,
            boolean keepScores) {
        this.descriptor = descriptor;
        this.searcherReference = searcherReference;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
        this.usageTracker = usageTracker;
        this.keepScores = keepScores;
    }

    protected abstract void validateQuery(PropertyIndexQuery... predicates);

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            AccessMode accessMode,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        validateQuery(predicates);
        context.monitor().queried(descriptor);
        usageTracker.queried();

        final var predicate = predicates[0];
        final var query = toLuceneQuery(predicate);
        final var progressor = search(query).getIndexProgressor(entityIdFieldKey(), client);
        final var needStoreFilter = needStoreFilter(predicate);
        client.initialize(descriptor, progressor, accessMode, false, needStoreFilter, constraints, predicate);
    }

    protected abstract Query toLuceneQuery(PropertyIndexQuery predicate);

    protected IllegalArgumentException invalidCompositeQuery(PropertyIndexQuery... predicates) {
        final var indexType = descriptor.getIndexType();
        return new IllegalArgumentException(("Tried to query a %s index with a composite query. "
                        + "Composite queries are not supported by a %s index. "
                        + "Query was: %s ")
                .formatted(indexType, indexType, Arrays.toString(predicates)));
    }

    protected IllegalArgumentException invalidQuery(PropertyIndexQuery predicate) {
        final var indexType = descriptor.getIndexType();
        return new IllegalArgumentException(
                "Index query not supported for %s index. Query: %s".formatted(indexType, predicate));
    }

    protected abstract String entityIdFieldKey();

    protected abstract boolean needStoreFilter(PropertyIndexQuery predicate);

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("EmptyTryBlock")
    @Override
    public void close() {
        try (usageTracker;
                searcherReference) {
            // low-cost closing without overhead of IOUtils::closeAll
        } catch (IOException e) {
            throw new IndexReaderCloseException(e);
        }
    }

    protected IndexSearcher getIndexSearcher() {
        return searcherReference.getIndexSearcher();
    }

    private DocValuesCollector search(Query query) {
        try {
            final var docValuesCollector = new DocValuesCollector(keepScores);
            getIndexSearcher().search(query, docValuesCollector);
            return docValuesCollector;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected BoundedIterable<Long> newAllEntriesValueReader(
            String field, Query query, long fromIdInclusive, long toIdExclusive) {
        final var collector = search(query);
        final var entityConsumer = new InRangeEntityConsumer(fromIdInclusive, toIdExclusive);
        final var indexProgressor = collector.getIndexProgressor(field, entityConsumer);
        return new AllEntriesValueReader(indexProgressor, entityConsumer);
    }

    private record AllEntriesValueReader(IndexProgressor indexProgressor, InRangeEntityConsumer entityConsumer)
            implements BoundedIterable<Long> {

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
