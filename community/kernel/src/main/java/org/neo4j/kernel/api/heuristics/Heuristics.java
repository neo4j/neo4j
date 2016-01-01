/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.heuristics;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;

/**
 * A service for accessing approximations and general statistics about the data in the database.
 */
public interface Heuristics
{
    /** Label id -> relative occurrence, value between 0 and 1. The total may be > 1, since labels may co-occur. */
    LabelledDistribution<Integer> labelDistribution();

    /** Relationship type id -> relative occurrence, value between 0 and 1. The total adds up to 1 */
    LabelledDistribution<Integer> relationshipTypeDistribution();

    /** Relationship degree distribution for a label/rel type/direction triplet. */
    double degree( int labelId, int relType, Direction direction );

    /** value between 0 and 1 representing fraction of nodes that could be read (weren't deleted or couldn't be read) */
    double liveNodesRatio();

    /** maximum number of nodes currently addressable in the database */
    long maxAddressableNodes();
}
