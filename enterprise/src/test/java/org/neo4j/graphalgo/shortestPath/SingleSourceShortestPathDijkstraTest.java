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

import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class SingleSourceShortestPathDijkstraTest extends
    SingleSourceShortestPathTest
{
    public SingleSourceShortestPathDijkstraTest( String name )
    {
        super( name );
    }

    protected SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode )
    {
        return new SingleSourceShortestPathDijkstra<Integer>( 0, startNode,
            new CostEvaluator<Integer>()
            {
                public Integer getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.IntegerAdder(),
            new org.neo4j.graphalgo.shortestpath.std.IntegerComparator(),
            Direction.BOTH, MyRelTypes.R1 );
    }

    protected SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode, Direction direction, RelationshipType... relTypes )
    {
        return new SingleSourceShortestPathDijkstra<Integer>( 0, startNode,
            new CostEvaluator<Integer>()
            {
                public Integer getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.IntegerAdder(),
            new org.neo4j.graphalgo.shortestpath.std.IntegerComparator(), direction,
            relTypes );
    }
}
