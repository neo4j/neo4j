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
package org.neo4j.kernel.api.index;

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;

public interface ValueIndexReader extends IndexReader {
    /**
     * @param entityId entity id to match.
     * @param cursorContext underlying page cursor context
     * @param propertyKeyIds the property key ids that correspond to each of the property values.
     * @param propertyValues property values to match.
     * @return number of index entries for the given {@code entityId} and {@code propertyValues}.
     */
    long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues);

    IndexSampler createSampler();

    /**
     * Queries the index for the given {@link PropertyIndexQuery} predicates.
     * @param client the client which will control the progression though query results.
     * @param accessMode security store access mode.
     * @param constraints constraints upon the query result, like ordering and whether the index should fetch property values alongside the entity ids.
     * @param query the query so serve.
     */
    void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            AccessMode accessMode,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException;

    /**
     * Create a partitioning over the result set for the given query. The partitions can be processed in parallel.
     * @param desiredNumberOfPartitions the number of desired partitions.
     * @param queryContext underlying cursor contexts for the partitioning.
     * @param query the query to serve.
     * @return The {@link PartitionedValueSeek} from which partitions can be reserved.
     */
    PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext queryContext, PropertyIndexQuery... query);

    ValueIndexReader EMPTY = new ValueIndexReader() {
        // Used for checking index correctness
        @Override
        public long countIndexedEntities(
                long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
            return 0;
        }

        @Override
        public IndexSampler createSampler() {
            return IndexSampler.EMPTY;
        }

        @Override
        public void query(
                IndexProgressor.EntityValueClient client,
                QueryContext context,
                AccessMode accessMode,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query) {
            // do nothing
        }

        @Override
        public PartitionedValueSeek valueSeek(
                int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
            throw new UnsupportedOperationException("EMPTY implementation does not support this method.");
        }

        @Override
        public void close() {}
    };
}
