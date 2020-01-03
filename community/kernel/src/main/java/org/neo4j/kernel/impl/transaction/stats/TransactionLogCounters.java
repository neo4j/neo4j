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

public interface TransactionLogCounters
{
    /**
     * Total number of bytes appended to transaction logs as result of applying transactions
     * @return total number of appended bytes
     */
    long appendedBytes();

    /**
     * Total number of transaction log file rotations
     * @return number of rotations
     */
    long numberOfLogRotations();

    /**
     * Accumulated log rotation time in milliseconds
     * @return accumulated log rotations time in milliseconds
     */
    long logRotationAccumulatedTotalTimeMillis();

    /**
     * Last log rotation time in milliseconds
     * @return last log rotation time in milliseconds
     */
    long lastLogRotationTimeMillis();
}
