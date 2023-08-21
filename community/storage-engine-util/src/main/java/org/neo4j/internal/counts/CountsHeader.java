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
package org.neo4j.internal.counts;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Both reading and writing of a {@link GBPTreeCountsStore} tree header collected into one class.
 */
class CountsHeader {
    static Reader reader() {
        return new Reader();
    }

    static Writer writer(LongSupplier highestGapFreeTxIdSupplier) {
        return new Writer(highestGapFreeTxIdSupplier);
    }

    static class Writer implements Consumer<PageCursor> {
        private final LongSupplier highestGapFreeTxIdSupplier;

        Writer(LongSupplier highestGapFreeTxIdSupplier) {
            this.highestGapFreeTxIdSupplier = highestGapFreeTxIdSupplier;
        }

        @Override
        public void accept(PageCursor cursor) {
            cursor.putLong(highestGapFreeTxIdSupplier.getAsLong());
        }
    }

    static class Reader implements Header.Reader {
        private boolean wasRead;
        private long highestGapFreeTxId;

        @Override
        public void read(ByteBuffer headerBytes) {
            wasRead = true;
            highestGapFreeTxId = headerBytes.getLong();
        }

        boolean wasRead() {
            return wasRead;
        }

        /**
         * @return the highest gap-free transaction id that the tree has counts data for. The tree may include counts data
         * for transaction ids higher than this id, but information about those transaction ids lives in the tree itself.
         */
        long highestGapFreeTxId() {
            return highestGapFreeTxId;
        }
    }
}
