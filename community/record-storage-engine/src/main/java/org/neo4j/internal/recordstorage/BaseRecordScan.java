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
package org.neo4j.internal.recordstorage;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;

/**
 * Maintains state when performing batched scans, potentially from multiple threads.
 * <p>
 * Will break up the scan in ranges depending on the provided size hint.
 */
abstract class BaseRecordScan<C extends PrimitiveRecord> {
    private final AtomicLong nextStart = new AtomicLong(0);

    boolean scanBatch(long sizeHint, C cursor) {
        if (sizeHint > 0) {
            long start = nextStart.getAndUpdate(operand -> overflowSafeAdd(operand, sizeHint));
            long stopInclusive = overflowSafeAdd(start, sizeHint - 1);
            return scanRange(cursor, start, stopInclusive);
        } else {
            long start = nextStart.get();
            return scanRange(cursor, start, start - 1);
        }
    }

    abstract boolean scanRange(C cursor, long start, long stopInclusive);

    private long overflowSafeAdd(long a, long b) {
        long result = a + b;
        return result >= 0 ? result : Long.MAX_VALUE;
    }
}
