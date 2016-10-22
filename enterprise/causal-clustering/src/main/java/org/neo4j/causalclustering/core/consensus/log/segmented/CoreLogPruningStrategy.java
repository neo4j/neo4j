/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

public interface CoreLogPruningStrategy
{
    /**
     * Returns the index to keep depending on the configuration strategy.
     * This does not factor in the value of the safe index to prune to.
     *
     * It is worth noting that the returned value may be the first available value,
     * rather than the first possible value. This signifies that no pruning is needed.
     *
     * @param segments The segments to inspect.
     * @return The lowest index the pruning configuration allows to keep. It is a value in the same range as
     * append indexes, starting from -1 all the way to {@link Long#MAX_VALUE}.
     */
    long getIndexToKeep( Segments segments );
}
