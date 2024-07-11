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
package org.neo4j.kernel.impl.index.schema.tracking;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;

public class TrackingReadersIndexAccessor extends IndexAccessor.Delegating {
    private static final AtomicLong openReaders = new AtomicLong();
    private static final AtomicLong closedReaders = new AtomicLong();

    public static long numberOfOpenReaders() {
        return openReaders.get();
    }

    public static long numberOfClosedReaders() {
        return closedReaders.get();
    }

    TrackingReadersIndexAccessor(IndexAccessor accessor) {
        super(accessor);
    }

    @Override
    public ValueIndexReader newValueReader(IndexUsageTracking usageTracker) {
        openReaders.incrementAndGet();
        return new TrackingIndexReader(super.newValueReader(usageTracker), closedReaders);
    }
}
