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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Query;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityIterator;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexReader;
import org.neo4j.kernel.api.impl.schema.LuceneScoredEntityIndexProgressor;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityValueClient;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;
import org.neo4j.values.storable.Value;

class VectorIndexReader extends AbstractLuceneIndexReader {
    private final VectorDocumentStructure documentStructure;
    private final int dimensions;
    private final List<SearcherReference> searchers;

    VectorIndexReader(
            IndexDescriptor descriptor,
            VectorIndexConfig vectorIndexConfig,
            VectorDocumentStructure documentStructure,
            List<SearcherReference> searchers,
            IndexUsageTracker usageTracker) {
        super(descriptor, usageTracker);
        this.documentStructure = documentStructure;
        this.dimensions = vectorIndexConfig.dimensions();
        this.searchers = searchers;
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        // TODO VECTOR: Currently only checks entity is in the index; it does not check the value is the same.
        //              Investigate finding a method to extract out the value itself from the index
        //              LeafReader::getFloatVectorValues seems promising with something like DocValuesCollector.
        //              Otherwise, perhaps k-ANN of k=1, filter=getById, (score-1) < epsilon?

        var count = 0L;
        final var query = VectorQueryFactory.getById(entityId);
        for (final var searcher : searchers) {
            try {
                count += searcher.getIndexSearcher().count(query);
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
            QueryContext context,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        super.query(client, context, adjustedConstraints(constraints, predicates), predicates);
    }

    @Override
    protected PropertyIndexQuery validateQuery(PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        final var predicate = super.validateQuery(predicates);
        if (predicate instanceof final NearestNeighborsPredicate nearestNeighbour) {
            final var queryVector = nearestNeighbour.query();
            if (queryVector.length != dimensions) {
                throw new IndexNotApplicableKernelException(
                        "Index query vector has a dimensionality of %d, but indexed vectors have %d."
                                .formatted(queryVector.length, dimensions));
            }
        }
        return predicate;
    }

    private IndexQueryConstraints adjustedConstraints(
            IndexQueryConstraints constraints, PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        return validateQuery(predicates) instanceof final NearestNeighborsPredicate nearestNeighbour
                ? constraints.limit(Math.min(
                        nearestNeighbour.numberOfNeighbors(),
                        constraints.limit().orElse(Integer.MAX_VALUE)))
                : constraints;
    }

    @Override
    protected Query toLuceneQuery(PropertyIndexQuery predicate, IndexQueryConstraints constraints) {
        return switch (predicate.type()) {
            case ALL_ENTRIES -> VectorQueryFactory.allValues();
            case NEAREST_NEIGHBORS -> {
                final var nearestNeighborsPredicate = (NearestNeighborsPredicate) predicate;
                final var k = Math.min(
                        nearestNeighborsPredicate.numberOfNeighbors(),
                        constraints.limit().orElse(Integer.MAX_VALUE));
                final var effectiveK = k + constraints.skip().orElse(0);
                yield VectorQueryFactory.approximateNearestNeighbors(
                        documentStructure, nearestNeighborsPredicate.query(), Math.toIntExact(effectiveK));
            }
            default -> throw invalidQuery(IllegalArgumentException::new, predicate);
        };
    }

    @Override
    protected IndexProgressor indexProgressor(
            Query query, IndexQueryConstraints constraints, IndexProgressor.EntityValueClient client) {
        final var iterator = searchLucene(query, constraints);
        return new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
    }

    @Override
    protected String entityIdFieldKey() {
        return VectorDocumentStructure.ENTITY_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        // We can't do filtering of false positives after the fact because we would
        // need to know which neighbors we missed to do so. We don't know what we don't know.
        return false;
    }

    @Override
    public void close() {
        final var closeables = new AutoCloseables<>(IndexReaderCloseException::new, searchers);
        try (closeables) {
            super.close();
        }
    }

    private ValuesIterator searchLucene(Query query, IndexQueryConstraints constraints) {
        // TODO VECTOR: FulltextIndexReader handles transaction state in a similar way
        //              with QueryContext, CursorContext, MemoryTracker
        try {
            // TODO VECTOR: pre-rewrite query? Not sure what rewriting entails
            final var results = new ArrayList<ValuesIterator>(searchers.size());
            for (final var searcher : searchers) {
                final var collector = new VectorResultCollector(constraints);
                searcher.getIndexSearcher().search(query, collector);
                results.add(collector.iterator());
            }
            return ScoredEntityIterator.mergeIterators(results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    BoundedIterable<Long> newAllEntriesValueReader(long fromIdInclusive, long toIdExclusive) throws IOException {
        final var field = VectorDocumentStructure.ENTITY_ID_KEY;
        final var query = VectorQueryFactory.allValues();
        final var iterables = new ArrayList<BoundedIterable<Long>>(searchers.size());
        for (final var searcher : searchers) {
            iterables.add(newAllEntriesValueReaderForPartition(
                    field, searcher.getIndexSearcher(), query, fromIdInclusive, toIdExclusive));
        }
        return BoundedIterable.concat(iterables);
    }
}
