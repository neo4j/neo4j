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
package org.neo4j.graphalgo.benchmark;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.centrality.Eccentricity;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.shortestPath.std.DoubleAdder;
import org.neo4j.graphalgo.shortestPath.std.DoubleComparator;
import org.neo4j.graphalgo.shortestPath.std.DoubleEvaluator;
import org.neo4j.graphalgo.shortestPath.std.IntegerComparator;

public class EccentricityLazySpBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new EccentricityLazySpBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    @Override
    public String getTestId()
    {
        return "EccentricityLazySpBenchmark";
    }

    RandomGraph graph;
    Set<Node> nodeSet;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
            250, 10000 );
        Transaction tx = neo.beginTx();
        numberOfRuns = 50;
        tx.finish();
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        nodeSet = new HashSet<Node>();
        for ( int i = 0; i < 2; ++i )
        {
            nodeSet.add( graph.getRandomNode( null ) );
        }
        // First test bfs
        SingleSourceShortestPath<Integer> singleSourceShortestPath = new SingleSourceShortestPathBFS(
            graph.getRandomNode( null ), Direction.BOTH, MyRelTypes.R1 );
        new Eccentricity<Integer>( singleSourceShortestPath, 0, nodeSet,
            new IntegerComparator() ).calculate();
        // Then test dijkstra
        SingleSourceShortestPath<Double> singleSourceShortestPath2 = new SingleSourceShortestPathDijkstra<Double>(
            0.0, null, new DoubleEvaluator( "cost" ), new DoubleAdder(),
            new DoubleComparator(), Direction.BOTH, MyRelTypes.R1 );
        new Eccentricity<Double>( singleSourceShortestPath2, 0.0, nodeSet,
            new DoubleComparator() ).calculate();
        tx.finish();
    }

    @Override
    protected void setUp()
    {
    }

    @Override
    protected void tearDown()
    {
    }
}
