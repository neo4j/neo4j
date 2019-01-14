/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.stream.LongStream;

/**
 * The LogPruneStrategy examines the current population of transaction logs, and decides which ones can be deleted,
 * up to some version.
 * <p>
 * Implementations of this class must be thread-safe, since they might experience multiple concurrent calls to
 * {@link #findLogVersionsToDelete(long)} from different threads.
 */
@FunctionalInterface
public interface LogPruneStrategy
{
    /**
     * Produce a stream of log versions which can be deleted, up to and <em>excluding</em> the given
     * {@code upToLogVersion}.
     * <p>
     * <strong>Note:</strong> It is important to delete the log files in the order specified by the stream,
     * which must be from the oldest version towards the newest. This way, no gaps are left behind if there is a crash
     * in the middle of log pruning.
     *
     * @param upToLogVersion Never suggest deleting log files at or greater than this version.
     * @return The, possibly empty, stream of log versions whose files can be deleted, according to this log pruning
     * strategy.
     */
    LongStream findLogVersionsToDelete( long upToLogVersion );
}
