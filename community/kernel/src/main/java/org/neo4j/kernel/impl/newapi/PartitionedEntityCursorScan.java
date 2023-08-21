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
package org.neo4j.kernel.impl.newapi;

import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.util.Preconditions;

abstract class PartitionedEntityCursorScan<C extends Cursor, S> implements PartitionedScan<C> {
    final S storageScan;
    // Read is generally not a thread-safe object.
    // This one belongs to the thread that initialised the partitioned scan.
    // Even thought the operations performed by the partitioned scan on the Read should be OK, we prefer the threads
    // working with the partition to use their own Reads.
    // In other words, using this "global" Read makes thread-safety of the scan a bit questionable.
    final Read fallbackRead;
    private final int numberOfPartitions;
    private final long batchSize;

    private final AtomicInteger emittedPartitions;

    PartitionedEntityCursorScan(S storageScan, Read read, int desiredNumberOfPartitions, long totalCount) {
        Preconditions.requirePositive(desiredNumberOfPartitions);
        this.storageScan = storageScan;
        this.fallbackRead = read;
        if (desiredNumberOfPartitions < totalCount) {
            this.numberOfPartitions = desiredNumberOfPartitions;
        } else {
            this.numberOfPartitions = Math.max((int) totalCount, 1);
        }
        this.batchSize = MathUtil.ceil(totalCount, numberOfPartitions);
        this.emittedPartitions = new AtomicInteger(0);
    }

    @Override
    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    long computeBatchSize() {
        // We make the last batch infinitely big in order to not miss any nodes that has been committed while the scan
        // is in progress.
        // This means that the last partition can potentially be bigger than the preceding partitions.
        return emittedPartitions.getAndIncrement() == numberOfPartitions - 1 ? Long.MAX_VALUE : batchSize;
    }
}
