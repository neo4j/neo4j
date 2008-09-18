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

import org.neo4j.api.core.Direction;
import org.neo4j.graphalgo.shortestPath.Dijkstra;
import org.neo4j.graphalgo.shortestPath.SingleDirectionDijkstra;
import org.neo4j.graphalgo.testUtil.SimpleGraphBuilder;

/**
 * Test cases for the singly directed dijkstra. This inherits all test cases
 * from DijkstraTest.
 */
public class SingleDirectionDijstraTest extends DijkstraTest
{
    public SingleDirectionDijstraTest( String arg0 )
    {
        super( arg0 );
    }

    // All we need to do is override this method
    protected Dijkstra<Double> getDijkstra( SimpleGraphBuilder graph,
        Double startCost, String startNode, String endNode )
    {
        return new SingleDirectionDijkstra<Double>( startCost, graph
            .getNode( startNode ), graph.getNode( endNode ),
            new org.neo4j.graphalgo.shortestPath.std.DoubleEvaluator( "cost" ),
            new org.neo4j.graphalgo.shortestPath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestPath.std.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
    }
}
