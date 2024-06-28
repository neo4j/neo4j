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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;
import org.neo4j.token.api.TokenConstants;

public class StubTokenIndexReader implements TokenIndexReader {
    private final Map<Long, Set<Long>> index = new HashMap<>();

    void index(int[] tokens, long entity) {
        for (long token : tokens) {
            index.computeIfAbsent(token, k -> new TreeSet<>());
            index.get(token).add(entity);
        }
    }

    @Override
    public void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext) {
        index.forEach((token, entities) ->
                client.initialize(new StubIndexProgressor(client, entities), token.intValue(), IndexOrder.NONE));
    }

    @Override
    public void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            EntityRange range,
            CursorContext cursorContext) {
        index.forEach((token, entities) ->
                client.initialize(new StubIndexProgressor(client, entities), token.intValue(), IndexOrder.NONE));
    }

    @Override
    public PartitionedTokenScan entityTokenScan(
            int desiredNumberOfPartitions, CursorContext context, TokenPredicate query) {
        throw new UnsupportedOperationException("Stub implementation does not support this method.");
    }

    @Override
    public PartitionedTokenScan entityTokenScan(PartitionedTokenScan leadingPartition, TokenPredicate query) {
        throw new UnsupportedOperationException("Stub implementation does not support this method.");
    }

    private static class StubIndexProgressor implements IndexProgressor {
        private final IndexProgressor.EntityTokenClient client;
        private final Iterator<Long> entities;

        StubIndexProgressor(IndexProgressor.EntityTokenClient client, Set<Long> entities) {
            this.client = client;
            this.entities = entities.iterator();
        }

        @Override
        public boolean next() {
            if (entities.hasNext()) {
                client.acceptEntity(entities.next(), TokenConstants.NO_TOKEN);
                return true;
            }
            return false;
        }

        @Override
        public void close() {}
    }

    @Override
    public void close() {}
}
