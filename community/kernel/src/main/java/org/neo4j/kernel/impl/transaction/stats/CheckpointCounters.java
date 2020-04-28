/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.stats;

public interface CheckpointCounters
{
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
}
