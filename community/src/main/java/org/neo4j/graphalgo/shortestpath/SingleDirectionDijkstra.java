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
package org.neo4j.graphalgo.shortestpath;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;

/**
 * Dijkstra class identical to {@link Dijkstra} except that it searches only
 * from the start node. Theoretical complexity is identical, but in practice
 * this has to traverse more of the network, i.e. runs slower.
 * @author Patrik Larsson
 * @param <CostType>
 *            The datatype the edge weights will be represented by.
 */
public class SingleDirectionDijkstra<CostType> extends Dijkstra<CostType>
{
    public SingleDirectionDijkstra( CostType startCost, Node startNode,
        Node endNode, CostEvaluator<CostType> costEvaluator,
        CostAccumulator<CostType> costAccumulator,
        Comparator<CostType> costComparator, Direction relationDirection,
        RelationshipType... costRelationTypes )
    {
        super( startCost, startNode, endNode, costEvaluator, costAccumulator,
            costComparator, relationDirection, costRelationTypes );
    }

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
    @Override
    public boolean calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return true;
        }
        doneCalculation = true;
        // Special case when path length is zero
        if ( startNode.equals( endNode ) )
        {
            foundPathsMiddleNodes = new HashSet<Node>();
            foundPathsMiddleNodes.add( startNode );
            foundPathsCost = costAccumulator.addCosts( startCost, startCost );
            return true;
        }
        HashMap<Node,CostType> seen1 = new HashMap<Node,CostType>();
        HashMap<Node,CostType> seen2 = new HashMap<Node,CostType>();
        HashMap<Node,CostType> dists1 = new HashMap<Node,CostType>();
        HashMap<Node,CostType> dists2 = new HashMap<Node,CostType>();
        DijstraIterator iter1 = new DijstraIterator( startNode, predecessors1,
            seen1, seen2, dists1, dists2, false );
        dists2.put( endNode, startCost );
        seen2.put( endNode, startCost );
        while ( iter1.hasNext() && !limitReached() )
        {
            iter1.next();
            if ( iter1.isDone() ) // A path was found
            {
                return true;
            }
        }
        return false;
    }
}
