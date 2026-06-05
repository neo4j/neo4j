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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

/**
 * Per-cycle decision: callers iterate log files newest → oldest and ask this predicate at each version. The first
 * file for which the predicate returns true is kept; everything strictly older becomes deletable.
 * <p>
 * The predicate instance lives for the whole cycle, so implementations that care about the previous iteration
 * (e.g. time-based pruning that looks at the next-newer file's timestamp) keep that reference internally.
 * <p>
 * Implementations should not throw to signal "couldn't decide" — return false instead.
 */
@FunctionalInterface
public interface PrunePredicate {
    boolean isLowestVersionToKeep(LogFileInformation current);
}
