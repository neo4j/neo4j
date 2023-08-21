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
package org.neo4j.internal.kernel.api;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;

/**
 * Initializer for spreading a scan operator over multiple cursors for use from different threads in parallel.
 * <p>
 * The {@link PartitionedScan partitioned scan object} is created with a given number of partitions.
 * Those partitions can be processed in parallel by different threads.
 * <p>
 * The number of created partitions is always a number between 1 and the desired number of partitions.
 *
 * @param <Cursor> the type of cursor this object initializes.
 */
public interface PartitionedScan<Cursor extends org.neo4j.internal.kernel.api.Cursor> {
    /**
     * The number of partitions is always a number between 1 and the desired number of partitions.
     *
     * @return the total number of partitions in this scan object
     */
    int getNumberOfPartitions();

    /**
     * See description of {@link #reservePartition(org.neo4j.internal.kernel.api.Cursor, ExecutionContext)}
     * as the general concepts ar the same.
     * <p>
     * This method is considered a legacy one and should be used only by GDS until they migrate to something else.
     *
     * @deprecated replaced by {@link #reservePartition(org.neo4j.internal.kernel.api.Cursor, ExecutionContext)} for
     * thread safety.
     *
     * @param cursor The cursor to be used for reading.
     * @param cursorContext The underlying page cursor context for the thread doing the seek.
     * @param accessMode security store access mode.
     * @throws IllegalStateException if transaction contains changed state.
     * @return <code>true</code> if there are more data to read, otherwise <code>false</code>
     *
     * @see #reservePartition(org.neo4j.internal.kernel.api.Cursor, ExecutionContext)
     */
    @Deprecated(since = "5.7", forRemoval = false)
    boolean reservePartition(Cursor cursor, CursorContext cursorContext, AccessMode accessMode);

    /**
     * Will attempt to reserve a partition to scan.
     * <p>
     * A <code>PartitionedScan</code> instance can be shared among threads and guarantees that each
     * call to <code>reservePartition</code> will reserve an exclusive partition to scan.
     * There are two basic ways to make sure all partitions are processed:
     * <p>
     * Pattern one: Look at {@link #getNumberOfPartitions() number of partitions} and
     * {@link #reservePartition(org.neo4j.internal.kernel.api.Cursor, ExecutionContext) reserve} this many partitions.
     * If using a fixed number of threads it is recommended to try and create the same number of partitions
     * since each individual partition comes with the overhead of traversing the underlying {@link GBPTree tree}.
     * Note that it might not always be possible to create the desired number of partitions and so make sure to always
     * check {@link #getNumberOfPartitions() number of partitions}.
     * Example:
     * <pre>
     * {@code
     *     for ( int i = 0; i < partitionedScan.getNumberOfPartitions(), i++ ) {
     *         executor.execute( processOnePartition( partitionedScan ) );
     *     }
     *
     *     Runnable processOnePartition( PartitionedScan partitionedScan ) {
     *         return () -> {
     *             try ( Cursor cursor = cursors.allocateCursor() ) {
     *                 partitionedScan.reservePartition( cursor, executionContext );
     *                 while ( cursor.next() ) {
     *                     // do things with the cursor
     *                 }
     *             }
     *         }
     *     }
     * }
     * </pre>
     * <p>
     * Pattern two: It is also possible to {@link #reservePartition(org.neo4j.internal.kernel.api.Cursor, ExecutionContext) reserve partitions}
     * until there are no more partitions to reserve by checking the return value. This can be done from one or multiple threads
     * Example:
     * <pre>
     * {@code
     *     try ( Cursor cursor = cursors.allocateCursor() ) {
     *         while ( partitionedScan.reservePartition( cursor, executionContext ) ) {
     *             while ( cursor.next() ) {
     *                 // do things with the cursor
     *             }
     *         }
     *     }
     * }
     * </pre>
     *
     * @param cursor The cursor to be used for reading.
     * @param executionContext execution context associated with the worker performing operations on this partition.
     * @throws IllegalStateException if transaction contains changed state.
     * @return <code>true</code> if there are more data to read, otherwise <code>false</code>
     */
    boolean reservePartition(Cursor cursor, ExecutionContext executionContext);
}
