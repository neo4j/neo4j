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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.PrintConfig;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.util.Preconditions;

public class DefaultTokenIndexReader<VALUE> implements TokenIndexReader {

    private final GBPTree<TokenScanKey, VALUE> index;
    private final IndexUsageTracking usageTracker;
    private final TokenIndexFormat<VALUE> format;
    private final TokenIndexIdLayout idLayout;

    public DefaultTokenIndexReader(
            GBPTree<TokenScanKey, VALUE> index, IndexUsageTracking usageTracker, TokenIndexFormat<VALUE> format) {
        this.index = index;
        this.usageTracker = usageTracker;
        this.format = format;
        this.idLayout = format.idLayout();
    }

    @Override
    public void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext) {
        usageTracker.queried();
        query(client, constraints, query, EntityRange.FULL, cursorContext);
    }

    @Override
    public void query(
            IndexProgressor.EntityTokenClient client,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            EntityRange range,
            CursorContext cursorContext) {
        try {
            int tokenId = query.tokenId();
            IndexOrder order = constraints.order();
            var seeker = seekerForToken(range, tokenId, order, cursorContext);
            var progressor = format.newProgressor(seeker, client, order, range, tokenId);
            client.initializeQuery(progressor, tokenId, order);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PartitionedTokenScan entityTokenScan(
            int desiredNumberOfPartitions, CursorContext context, TokenPredicate query) {
        try {
            usageTracker.queried();
            return new NativePartitionedTokenScan(desiredNumberOfPartitions, context, query);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PartitionedTokenScan entityTokenScan(PartitionedTokenScan leadingPartition, TokenPredicate query) {
        usageTracker.queried();
        return new NativePartitionedTokenScan((NativePartitionedTokenScan) leadingPartition, query);
    }

    private Seeker<TokenScanKey, VALUE> seekerForToken(
            EntityRange range, int tokenId, IndexOrder indexOrder, CursorContext cursorContext) throws IOException {
        long idRangeFromInclusive;
        long idRangeToExclusive;

        if (indexOrder == IndexOrder.DESCENDING) {
            idRangeFromInclusive = idLayout.rangeOf(range.toExclusive());
            idRangeToExclusive = idLayout.rangeOf(range.fromInclusive()) - 1;
        } else {
            idRangeFromInclusive = idLayout.rangeOf(range.fromInclusive());
            idRangeToExclusive = idLayout.rangeOf(range.toExclusive()) + 1;
        }

        return index.seek(
                new TokenScanKey(tokenId, idRangeFromInclusive),
                new TokenScanKey(tokenId, idRangeToExclusive),
                cursorContext);
    }

    public void printTree(PrintConfig printConfig) throws IOException {
        index.printTree(printConfig, CursorContext.NULL_CONTEXT);
    }

    @Override
    public void close() {}

    private class NativePartitionedTokenScan implements PartitionedTokenScan {
        private static final EntityRange range = EntityRange.FULL;
        private final List<TokenScanKey> partitionEdges;
        private final AtomicInteger nextFrom = new AtomicInteger();

        NativePartitionedTokenScan(int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
                throws IOException {
            Preconditions.requirePositive(desiredNumberOfPartitions);
            int tokenId = query.tokenId();
            TokenScanKey fromInclusive = new TokenScanKey(tokenId, idLayout.rangeOf(range.fromInclusive()));
            TokenScanKey toExclusive = new TokenScanKey(tokenId, idLayout.rangeOf(range.toExclusive()) + 1);
            partitionEdges =
                    index.partitionedSeek(fromInclusive, toExclusive, desiredNumberOfPartitions, cursorContext);
        }

        NativePartitionedTokenScan(NativePartitionedTokenScan leadingPartition, TokenPredicate query) {
            int tokenId = query.tokenId();
            List<TokenScanKey> leadingEdges = leadingPartition.partitionEdges;
            partitionEdges = new ArrayList<>(leadingEdges.size());
            for (TokenScanKey leadingEdge : leadingEdges) {
                partitionEdges.add(new TokenScanKey(tokenId, leadingEdge.idRange));
            }
        }

        @Override
        public int getNumberOfPartitions() {
            return partitionEdges.size() - 1;
        }

        @Override
        public TokenIndexProgressor reservePartition(
                IndexProgressor.EntityTokenClient client, CursorContext cursorContext) {
            int from = nextFrom.getAndIncrement();
            int to = from + 1;
            if (to >= partitionEdges.size()) {
                return TokenIndexProgressor.EMPTY;
            }
            try {
                TokenScanKey fromInclusive = copyKey(partitionEdges.get(from));
                TokenScanKey toExclusive = copyKey(partitionEdges.get(to));
                return format.newProgressor(
                        index.seek(fromInclusive, toExclusive, cursorContext),
                        client,
                        IndexOrder.NONE,
                        range,
                        fromInclusive.tokenId);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static TokenScanKey copyKey(TokenScanKey key) {
            return new TokenScanKey(key.tokenId, key.idRange);
        }
    }
}
