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

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.SubGraph;
import org.neo4j.graphalgo.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.centrality.CostDivider;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.shortestPath.std.IntegerAdder;

public class ClosenessCentralityBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new ClosenessCentralityBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    @Override
    public String getTestId()
    {
        return "ClosenessCentralityBenchmark";
    }

    RandomGraph graph;
    SubGraph graph2;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
            250, 10000 );
        Transaction tx = neo.beginTx();
        graph2 = new SubGraph();
        graph2.addSubGraphFromCentralNode( graph.getRandomNode( null ),
            1000000, MyRelTypes.R1, Direction.BOTH, false );
        tx.finish();
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        SingleSourceShortestPath<Integer> singleSourceShortestPath = new SingleSourceShortestPathBFS(
            graph.getRandomNode( null ), Direction.BOTH, MyRelTypes.R1 );
        new ClosenessCentrality<Integer>( singleSourceShortestPath,
            new IntegerAdder(), 0, graph2.getNodes(),
            new CostDivider<Integer>()
            {
                public Integer divideByCost( Double d, Integer c )
                {
                    return (int) (d / c);
                }

                public Integer divideCost( Integer c, Double d )
                {
                    return (int) (c / d);
                }
            } ).calculate();
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
