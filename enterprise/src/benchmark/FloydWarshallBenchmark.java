/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.graphalgo.benchmark;

import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.PreferentialAttachment;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.SmallWorldGraph;
import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.FloydWarshall;
import org.neo4j.graphalgo.shortestpath.std.DoubleAdder;
import org.neo4j.graphalgo.shortestpath.std.DoubleComparator;

public class FloydWarshallBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new FloydWarshallBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    @Override
    public String getTestId()
    {
        return "FloydWarshallBenchmark";
    }

    Set<Node> graphNodes;
    Set<Relationship> graphRelationships;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        SmallWorldGraph graph3 = new SmallWorldGraph( neo,
            new GraphStore( neo ), MyRelTypes.R1, 1000, 10, 0.15, true, true );
        RandomGraph graph2 = new RandomGraph( neo, new GraphStore( neo ),
            MyRelTypes.R1, 1000, 10000 );
        PreferentialAttachment graph = new PreferentialAttachment( neo,
            new GraphStore( neo ), MyRelTypes.R1, 1000, 10 );
        Transaction tx = neo.beginTx();
        graphNodes = graph.getNodes();
        graphRelationships = graph.getRelationships();
        numberOfRuns = 10;
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

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        new FloydWarshall<Double>( 0.0, Double.POSITIVE_INFINITY,
            Direction.BOTH, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1.0;
                }
            }, new DoubleAdder(), new DoubleComparator(), graphNodes,
            graphRelationships ).calculate();
        tx.finish();
    }
}
