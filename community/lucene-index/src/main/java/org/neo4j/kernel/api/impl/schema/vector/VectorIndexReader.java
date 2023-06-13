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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.kernel.api.impl.schema.vector.VectorUtils.vectorDimensionsFrom;

import org.apache.lucene.search.Query;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexReader;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;
import org.neo4j.values.storable.Value;

class VectorIndexReader extends AbstractLuceneIndexReader {
    private final int vectorDimensionality;

    VectorIndexReader(
            IndexDescriptor descriptor,
            SearcherReference searcherReference,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator,
            IndexUsageTracker usageTracker) {
        // TODO VECTOR: should this actually keep scores? Do we care?
        super(descriptor, searcherReference, samplingConfig, taskCoordinator, usageTracker, true);
        this.vectorDimensionality = vectorDimensionsFrom(descriptor.getIndexConfig());
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        // TODO VECTOR: count indexed entities
        return 0;
    }

    @Override
    public IndexSampler createSampler() {
        return IndexSampler.EMPTY;
    }

    @Override
    protected void validateQuery(PropertyIndexQuery... predicates) {
        if (predicates.length > 1) {
            throw invalidCompositeQuery(predicates);
        }

        final var predicate = predicates[0];
        if (predicate.type() != IndexQueryType.NEAREST_NEIGHBORS) {
            throw invalidQuery(predicate);
        }

        final var queryVector = ((NearestNeighborsPredicate) predicate).query();
        if (queryVector.length != vectorDimensionality) {
            throw new IllegalArgumentException(
                    "Index query vector has a dimensionality of %d, but indexed vectors have %d."
                            .formatted(queryVector.length, vectorDimensionality));
        }
    }

    @Override
    protected Query toLuceneQuery(PropertyIndexQuery predicate) {
        return switch (predicate.type()) {
            case ALL_ENTRIES -> VectorQueryFactory.allValues();
            case NEAREST_NEIGHBORS -> {
                final var nearestNeighborsPredicate = (NearestNeighborsPredicate) predicate;
                yield VectorQueryFactory.approximateNearestNeighbors(
                        nearestNeighborsPredicate.query(), nearestNeighborsPredicate.numberOfNeighbors());
            }
            default -> throw invalidQuery(predicate);
        };
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
}
