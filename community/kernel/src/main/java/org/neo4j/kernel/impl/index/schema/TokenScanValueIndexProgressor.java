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
import org.neo4j.graphdb.Resource;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexProgressor;

/**
 * {@link IndexProgressor} which steps over multiple {@link TokenScanValue} and for each
 * iterate over each set bit, returning actual entity ids, i.e. {@code entityIdRange+bitOffset}.
 *
 */
public class TokenScanValueIndexProgressor implements IndexProgressor, Resource {

    /**
     * {@link Seeker} to lazily read new {@link TokenScanValue} from.
     */
    private final Seeker<TokenScanKey, TokenScanValue> cursor;

    /**
     * Current base entityId, i.e. the {@link TokenScanKey#idRange} of the current {@link TokenScanKey}.
     */
    private long baseEntityId;
    /**
     * Bit set of the current {@link TokenScanValue}.
     */
    private long bits;
    /**
     * TokenId of previously retrieved {@link TokenScanKey}, for debugging and asserting purposes.
     */
    private int prevToken = -1;
    /**
     * IdRange of previously retrieved {@link TokenScanKey}, for debugging and asserting purposes.
     */
    private long prevRange = -1;
    /**
     * Indicate provided cursor has been closed.
     */
    private boolean closed;

    private final EntityTokenClient client;
    private final IndexOrder indexOrder;
    private final EntityRange range;
    private final TokenIndexIdLayout idLayout;
    private int tokenId;

    TokenScanValueIndexProgressor(
            Seeker<TokenScanKey, TokenScanValue> cursor,
            EntityTokenClient client,
            IndexOrder indexOrder,
            EntityRange range,
            TokenIndexIdLayout idLayout) {
        this.cursor = cursor;
        this.client = client;
        this.indexOrder = indexOrder;
        this.range = range;
        this.idLayout = idLayout;
    }

    /**
     *  Progress through the index until the next accepted entry.
     *
     *  Progress the cursor to the current {@link TokenScanValue}, if this is not accepted by the client or if current
     *  value has been exhausted it continues to the next {@link TokenScanValue} by progressing the {@link Seeker}.
     * @return <code>true</code> if it found an accepted entry, <code>false</code> otherwise
     */
    @Override
    public boolean next() {
        for (; ; ) {
            while (bits != 0) {
                long idForClient;
                if (indexOrder != IndexOrder.DESCENDING) {
                    // The next idForClient can be found at the next 1-bit from the right.
                    int delta = Long.numberOfTrailingZeros(bits);

                    // We switch that bit to zero, so that we don't find it again the next time.
                    // First, create a mask where that bit is zero (easiest by subtracting 1) and then &
                    // it with bits.
                    bits &= bits - 1;
                    idForClient = baseEntityId + delta;
                } else {
                    // The next idForClient can be found at the next 1-bit from the left.
                    int delta = Long.numberOfLeadingZeros(bits);

                    // We switch that bit to zero, so that we don't find it again the next time.
                    // First, create a mask where only set bit is set (easiest by bitshifting the number one),
                    // and then invert the mask and then & it with bits.
                    long bitToZero = 1L << (Long.SIZE - delta - 1);
                    bits &= ~bitToZero;
                    idForClient = (baseEntityId + Long.SIZE) - delta - 1;
                }

                if (isInRange(idForClient) && client.acceptEntity(idForClient, tokenId)) {
                    return true;
                }
            }
            try {
                if (!cursor.next()) {
                    close();
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            var key = cursor.key();
            baseEntityId = idLayout.firstIdOfRange(key.idRange);
            tokenId = key.tokenId;
            bits = cursor.value().bits;

            //noinspection AssertWithSideEffects
            assert keysInOrder(key, indexOrder);
        }
    }

    /**
     * The entity information in token indexes is stored in a collection of 64 bit bitmaps,
     * The index seek with specified range has a bitmap granularity.
     * In other words, the range of entity IDs coming from the index seeker corresponds to the search range with
     * start of the range rounded down to the nearest multiple of 64 and the end of the range rounded up to the nearest multiple of 64.
     * The purpose of this method is to filter out the extra entity IDs that are present in the seek result because of the rounding.
     */
    private boolean isInRange(long entityId) {
        return entityId >= range.fromInclusive() && entityId < range.toExclusive();
    }

    private boolean keysInOrder(TokenScanKey key, IndexOrder order) {
        if (order == IndexOrder.NONE) {
            return true;
        } else if (prevToken != -1 && prevRange != -1 && order == IndexOrder.ASCENDING) {
            assert key.tokenId >= prevToken
                    : "Expected to get ascending ordered results, got " + key + " where previous token was "
                            + prevToken;
            assert key.idRange > prevRange
                    : "Expected to get ascending ordered results, got " + key + " where previous range was "
                            + prevRange;
        } else if (prevToken != -1 && prevRange != -1 && order == IndexOrder.DESCENDING) {
            assert key.tokenId <= prevToken
                    : "Expected to get descending ordered results, got " + key + " where previous token was "
                            + prevToken;
            assert key.idRange < prevRange
                    : "Expected to get descending ordered results, got " + key + " where previous range was "
                            + prevRange;
        }
        prevToken = key.tokenId;
        prevRange = key.idRange;
        // Made as a method returning boolean so that it can participate in an assert-call.
        return true;
    }

    public void close() {
        if (!closed) {
            try {
                cursor.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                closed = true;
            }
        }
    }
}
