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
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.AllSimplePaths;
import org.neo4j.graphalgo.benchmark.graphgeneration.GeneratedGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.PreferentialAttachment;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.SmallWorldGraph;

public class AllSimplePathsBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new AllSimplePathsBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    Node startNode;
    Node endNode;
    GeneratedGraph graph;

    @Override
    public String getTestId()
    {
        return "AllSimplePathsBenchmark";
    }

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        RandomGraph graph1;
        SmallWorldGraph graph2;
        PreferentialAttachment graph3;
        // graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
        // 10,
        // 30 );
        // graph = new SmallWorldGraph( neo, new GraphStore( neo ),
        // MyRelTypes.R1,
        // 10, 2, 0.25, true, true );
        graph = new PreferentialAttachment( neo, new GraphStore( neo ),
            MyRelTypes.R1, 10, 3 );
        numberOfRuns = 100;
    }

    @Override
    protected void setUp()
    {
    }

    @Override
    protected void tearDown()
    {
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        startNode = graph.getRandomNode( null );
        endNode = graph.getRandomNode( startNode );
        new AllSimplePaths( startNode, endNode, Integer.MAX_VALUE,
            Direction.BOTH, MyRelTypes.R1 ).calculate();
        tx.finish();
    }
}
