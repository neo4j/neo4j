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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.nio.file.Path;

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

/**
 * Determines transaction log pruning point below which it should be safe to prune log files.
 */
public interface Threshold
{
    void init();

    /**
     * Check if threshold is reached for provided version of transaction log file.
     * Even if file can't be read or some condition can't be evaluated threshold should not throw exception and make any assumptions about presence or
     * absence of file, correctness of information, etc. Instead threshold should not be reached as result.
     * @param path transaction log file
     * @param version version of log file
     * @param source meta information about particular transaction file
     * @return true if reached, false otherwise
     */
    boolean reached( Path path, long version, LogFileInformation source );
}
