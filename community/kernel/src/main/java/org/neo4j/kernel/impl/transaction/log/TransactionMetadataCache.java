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
package org.neo4j.kernel.impl.transaction.log;

import java.util.Objects;
import org.neo4j.internal.helpers.collection.LfuCache;

public class TransactionMetadataCache {
    private static final int DEFAULT_METADATA_CACHE_SIZE = 10_000;
    private final LfuCache<Long, TransactionMetadata> appendIndexMetadataCache;

    public TransactionMetadataCache() {
        this.appendIndexMetadataCache =
                new LfuCache<>("Append index start position cache", DEFAULT_METADATA_CACHE_SIZE);
    }

    public void clear() {
        appendIndexMetadataCache.clear();
    }

    public TransactionMetadata getTransactionMetadata(long appendIndex) {
        return appendIndexMetadataCache.get(appendIndex);
    }

    public void cacheTransactionMetadata(long appendIndex, LogPosition position) {
        if (LogPosition.UNSPECIFIED == position) {
            throw new IllegalArgumentException("Metadata cache only supports specified log positions.");
        }
        TransactionMetadata result = new TransactionMetadata(position);
        appendIndexMetadataCache.put(appendIndex, result);
    }

    public record TransactionMetadata(LogPosition startPosition) {

        @Override
        public String toString() {
            return "TransactionMetadata{" + ", startPosition=" + startPosition + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TransactionMetadata that = (TransactionMetadata) o;
            return Objects.equals(startPosition, that.startPosition);
        }
    }
}
