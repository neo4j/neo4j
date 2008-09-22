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

import java.util.LinkedList;
import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GeneratedGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.PreferentialAttachment;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.SmallWorldGraph;
import org.neo4j.graphalgo.centrality.Eccentricity;
import org.neo4j.graphalgo.centrality.ParallellCentralityCalculation;
import org.neo4j.graphalgo.centrality.StressCentrality;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestPath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.shortestPath.std.IntegerComparator;

public class CentralityBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new CentralityBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    @Override
    public String getTestId()
    {
        return "CentralityBenchmark";
    }

    GeneratedGraph graph;
    Set<Node> nodeSet;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        if ( "RandomGraph(100,1000)".equals( currentInternalId ) )
        {
            graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
                100, 1000 );
        }
        if ( "SmallWorldGraph(100,10,0.25,true,true)"
            .equals( currentInternalId ) )
        {
            graph = new SmallWorldGraph( neo, new GraphStore( neo ),
                MyRelTypes.R1, 100, 10, 0.25, true, true );
        }
        if ( "PreferentialAttachment(100,10)".equals( currentInternalId ) )
        {
            graph = new PreferentialAttachment( neo, new GraphStore( neo ),
                MyRelTypes.R1, 100, 10 );
        }
        if ( "RandomGraph(1000,5000)".equals( currentInternalId ) )
        {
            graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
                1000, 5000 );
        }
        if ( "SmallWorldGraph(1000,5,0.25,true,true)"
            .equals( currentInternalId ) )
        {
            graph = new SmallWorldGraph( neo, new GraphStore( neo ),
                MyRelTypes.R1, 1000, 5, 0.25, true, true );
        }
        if ( "PreferentialAttachment(1000,5)".equals( currentInternalId ) )
        {
            graph = new PreferentialAttachment( neo, new GraphStore( neo ),
                MyRelTypes.R1, 1000, 5 );
        }
        if ( "RandomGraph(1000,10000)".equals( currentInternalId ) )
        {
            graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
                1000, 10000 );
        }
        if ( "SmallWorldGraph(1000,10,0.25,true,true)"
            .equals( currentInternalId ) )
        {
            graph = new SmallWorldGraph( neo, new GraphStore( neo ),
                MyRelTypes.R1, 1000, 10, 0.25, true, true );
        }
        if ( "PreferentialAttachment(1000,10)".equals( currentInternalId ) )
        {
            graph = new PreferentialAttachment( neo, new GraphStore( neo ),
                MyRelTypes.R1, 1000, 10 );
        }
        Transaction tx = neo.beginTx();
        nodeSet = graph.getNodes();
        tx.finish();
    }

    public CentralityBenchmark()
    {
        internalIds = new LinkedList<Object>();
        internalIds.add( "RandomGraph(100,1000)" );
        internalIds.add( "SmallWorldGraph(100,10,0.25,true,true)" );
        internalIds.add( "PreferentialAttachment(100,10)" );
        internalIds.add( "RandomGraph(1000,5000)" );
        internalIds.add( "SmallWorldGraph(1000,5,0.25,true,true)" );
        internalIds.add( "PreferentialAttachment(1000,5)" );
        internalIds.add( "RandomGraph(1000,10000)" );
        internalIds.add( "SmallWorldGraph(1000,10,0.25,true,true)" );
        internalIds.add( "PreferentialAttachment(1000,10)" );
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        // Use a BFS
        SingleSourceShortestPath<Integer> singleSourceShortestPath = new SingleSourceShortestPathBFS(
            graph.getRandomNode( null ), Direction.BOTH, MyRelTypes.R1 );
        // new BetweennessCentrality<Integer>( singleSourceShortestPath, nodeSet
        // )
        // .calculate();
        // new ClosenessCentrality<Integer>( singleSourceShortestPath,
        // new IntegerAdder(), 0, nodeSet, new CostDivider<Integer>()
        // {
        // public Integer divideByCost( Double d, Integer c )
        // {
        // return (int) (d / c);
        // }
        //
        // public Integer divideCost( Integer c, Double d )
        // {
        // return (int) (c / d);
        // }
        // } ).calculate();
        ParallellCentralityCalculation<Integer> parallellCentralityCalculation = new ParallellCentralityCalculation<Integer>(
            singleSourceShortestPath, nodeSet );
        parallellCentralityCalculation
            .addCalculation( new StressCentrality<Integer>(
                singleSourceShortestPath, nodeSet ) );
        parallellCentralityCalculation
            .addCalculation( new Eccentricity<Integer>(
                singleSourceShortestPath, 0, nodeSet, new IntegerComparator() ) );
        parallellCentralityCalculation.calculate();
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
