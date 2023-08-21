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
package org.neo4j.kernel.impl.transaction.stats;

public interface CheckpointCounters {
    /**
     * Total number of checkpoints occurred
     * @return number of checkpoints
     */
    long numberOfCheckPoints();

    /**
     * Accumulated duration in milliseconds of all occurred checkpoints
     * @return accumulated checkpoints duration in milliseconds
     */
    long checkPointAccumulatedTotalTimeMillis();

    /**
     * Last checkpoint duration in milliseconds
     * @return last checkpoint duration in milliseconds
     */
    long lastCheckpointTimeMillis();

    /**
     * Number of pages flushed in the last checkpoint
     * @return number of pages flushed in the last checkpoint
     */
    long lastCheckpointPagesFlushed();

    /**
     * Number of IOs performed by the last checkpoint
     * @return number of IOs performed by the last checkpoint
     */
    long lastCheckpointIOs();

    /**
     * Last observed value of io limited during the last checkpoint
     * @return Last observed value of io limited during the last checkpoint
     */
    long lastCheckpointIOLimit();

    /**
     * Number of times last checkpoint IOs was limited by io controller
     */
    long lastCheckpointIOLimitedTimes();

    /**
     * Number of millis last checkpoint IOs was limited by io controller
     */
    long lastCheckpointIOLimitedMillis();

    /**
     * Total number of bytes flushed as result of doing checkpoints.
     * Please note that this is a metric for all checkpoints, including ongoing and not for the last one only.
     * @return total number of flushed bytes
     */
    long flushedBytes();
}
