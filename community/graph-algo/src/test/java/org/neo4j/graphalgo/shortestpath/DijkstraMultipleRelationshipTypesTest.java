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
package org.neo4j.graphalgo.shortestpath;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import common.Neo4jAlgoTestCase;

public class DijkstraMultipleRelationshipTypesTest extends Neo4jAlgoTestCase
{
    protected Dijkstra<Double> getDijkstra( String startNode, String endNode,
            RelationshipType... relTypes )
    {
        return new Dijkstra<Double>( 0.0, graph.getNode( startNode ),
                graph.getNode( endNode ), new CostEvaluator<Double>()
                {
                    public Double getCost( Relationship relationship,
                            Direction direction )
                    {
                        return 1.0;
                    }
                }, new DoubleAdder(), new DoubleComparator(), Direction.BOTH,
                relTypes );
    }

    @Test
    public void testRun()
    {
        graph.setCurrentRelType( MyRelTypes.R1 );
        graph.makeEdgeChain( "a,b,c,d,e" );
        graph.setCurrentRelType( MyRelTypes.R2 );
        graph.makeEdges( "a,c" ); // first shortcut
        graph.setCurrentRelType( MyRelTypes.R3 );
        graph.makeEdges( "c,e" ); // second shortcut
        Dijkstra<Double> dijkstra;
        dijkstra = getDijkstra( "a", "e", MyRelTypes.R1 );
        assertTrue( dijkstra.getCost() == 4.0 );
        dijkstra = getDijkstra( "a", "e", MyRelTypes.R1, MyRelTypes.R2 );
        assertTrue( dijkstra.getCost() == 3.0 );
        dijkstra = getDijkstra( "a", "e", MyRelTypes.R1, MyRelTypes.R3 );
        assertTrue( dijkstra.getCost() == 3.0 );
        dijkstra = getDijkstra( "a", "e", MyRelTypes.R1, MyRelTypes.R2,
                MyRelTypes.R3 );
        assertTrue( dijkstra.getCost() == 2.0 );
    }
}
