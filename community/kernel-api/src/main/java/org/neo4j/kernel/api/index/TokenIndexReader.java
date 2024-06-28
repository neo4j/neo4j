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
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;

public interface TokenIndexReader extends IndexReader {

    /**
     * Queries all the entities and coordinates with the supplied {@link IndexProgressor.EntityTokenClient} to return the results
     *
     * @param client       a handle for the token reader to propagate the queried results.
     * @param constraints  represents all constraints for this query like ordering, limit etc.
     * @param query        the predicate to identify the tokens being queried
     * @param cursorContext underlying page cursor context
     */
    void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext);

    /**
     * Queries a specific range of entities and coordinates with the supplied {@link IndexProgressor.EntityTokenClient} to return the results.
     *
     * @param client       a handle for the token reader to propagate the queried results.
     * @param constraints  represents all constraints for this query like ordering, limit etc.
     * @param query        the predicate to identify the tokens being queried
     * @param range        the range of entities that should be queried.
     * @param cursorContext underlying page cursor context
     */
    void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            EntityRange range,
            CursorContext cursorContext);

    PartitionedTokenScan entityTokenScan(int desiredNumberOfPartitions, CursorContext context, TokenPredicate query);

    PartitionedTokenScan entityTokenScan(PartitionedTokenScan leadingPartition, TokenPredicate query);

    TokenIndexReader EMPTY = new TokenIndexReader() {
        @Override
        public void query(
                IndexProgressor.EntityTokenClient client,
                IndexQueryConstraints constraints,
                TokenPredicate query,
                CursorContext cursorContext) {}

        @Override
        public void query(
                IndexProgressor.EntityTokenClient client,
                IndexQueryConstraints constraints,
                TokenPredicate query,
                EntityRange range,
                CursorContext cursorContext) {}

        @Override
        public PartitionedTokenScan entityTokenScan(
                int desiredNumberOfPartitions, CursorContext context, TokenPredicate query) {
            throw new UnsupportedOperationException("EMPTY implementation does not support this method.");
        }

        @Override
        public PartitionedTokenScan entityTokenScan(PartitionedTokenScan leadingPartition, TokenPredicate query) {
            throw new UnsupportedOperationException("EMPTY implementation does not support this method.");
        }

        @Override
        public void close() {}
    };
}
