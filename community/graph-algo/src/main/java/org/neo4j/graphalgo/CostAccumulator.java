/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphalgo;

/**
 * In order to make the solving of shortest path problems as general as
 * possible, the algorithms accept objects handling all relevant tasks regarding
 * costs of paths. This allows the user to represent the costs in any possible
 * way, and to calculate them in any way. The usual case is numbers that we just
 * add together, but what if we have for example factors we would like to
 * multiply instead? This is handled by this system, which works as follows. A
 * CostEvaluator is used to get the cost for a single relationship. These costs
 * are then added through a CostAccumulator. Costs for alternative paths are
 * compared with a common java.util.Comparator.
 * 
 * @author Patrik Larsson
 * @param <T> The data type the edge weights are represented by.
 */
public interface CostAccumulator<T>
{
    /**
     * This is the accumulating method. This should return the results of
     * "adding" two path costs with each other.
     *
     * @param c1 One of the costs.
     * @param c2 The other cost.
     * @return The resulting cost.
     */
    T addCosts( T c1, T c2 );
}
