/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.shortestPath;

import org.neo4j.api.core.Relationship;

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
 * @author Patrik Larsson
 * @param <CostType>
 *            The datatype the edge weigths are represented by.
 */
public interface CostEvaluator<CostType>
{
    /**
     * This is the general method for looking up costs for relationships. This
     * can do anything, like looking up a property or running some small
     * calculation.
     * @param relationship
     * @param backwards
     *            True if an eventual path would be going in the opposite
     *            direction through this edge, ie from the end node to the start
     *            node. This will always be false when using Direction.OUTGOING
     *            and always true when using Direction.INCOMMING
     * @return The cost for this edge/relationship
     */
    CostType getCost( Relationship relationship, boolean backwards );
}
