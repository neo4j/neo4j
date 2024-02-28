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
package org.neo4j.memory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Specialized memory pool used in parallel runtime that keeps track of the maximum reserved heap.
 */
public class HighWaterMarkMemoryPool extends DelegatingMemoryPool {

    public static HighWaterMarkMemoryPool NO_TRACKING = new HighWaterMarkMemoryPool(MemoryPools.NO_TRACKING);

    private final AtomicLong highWaterMark = new AtomicLong();

    public HighWaterMarkMemoryPool(MemoryPool delegate) {
        super(delegate);
    }

    @Override
    public void reserveHeap(long bytes) {
        super.reserveHeap(bytes);
        computeNewHighWaterMark();
    }

    public long heapHighWaterMark() {
        return highWaterMark.getAcquire();
    }

    public void reset() {
        highWaterMark.set(0L);
    }

    private void computeNewHighWaterMark() {
        long currentHeapUsage = usedHeap();
        long currentHighMark = heapHighWaterMark();
        if (currentHeapUsage > currentHighMark) {
            highWaterMark.setRelease(currentHeapUsage);
        }
    }
}
