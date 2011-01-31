/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.LinkedList;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GeneratedGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphgeneration.PreferentialAttachment;
import org.neo4j.graphalgo.benchmark.graphgeneration.RandomGraph;
import org.neo4j.graphalgo.benchmark.graphgeneration.SmallWorldGraph;
import org.neo4j.graphalgo.centrality.EigenvectorCentralityPower;
import org.neo4j.graphalgo.shortestpath.CostEvaluator;

public class EigenvectorCentralityPowerBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new EigenvectorCentralityPowerBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    public EigenvectorCentralityPowerBenchmark()
    {
        internalIds = new LinkedList<Object>();
        internalIds.add( "RandomGraph(1000,10000)" );
        internalIds.add( "SmallWorldGraph(1000,10,0.25,true,true)" );
        internalIds.add( "PreferentialAttachment(1000,10)" );
    }

    @Override
    public String getTestId()
    {
        return "EigenvectorCentralityPowerBenchmark";
    }

    GeneratedGraph graph;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
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
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        EigenvectorCentralityPower eigenvectorCentralityPower = new EigenvectorCentralityPower(
            Direction.OUTGOING, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1.0;
                }
            }, graph.getNodes(), graph.getRelationships(), 0.001 );
        eigenvectorCentralityPower.calculate();
        // System.out.println( "Iterations: "
        // + eigenvectorCentralityPower.getTotalIterations() );
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
