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
package org.neo4j.experimental;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.NeoAlgoBenchmark;
import org.neo4j.graphalgo.benchmark.graphGeneration.GraphStore;
import org.neo4j.graphalgo.benchmark.graphGeneration.RandomGraph;
import org.neo4j.graphalgo.shortestPath.CostEvaluator;

public class EigenvectorCentralityRayleighBenchmark extends NeoAlgoBenchmark
{
    public static void main( String args[] )
    {
        // hack to avoid static
        new EigenvectorCentralityRayleighBenchmark().neoAlgoBenchMarkRun();
    }

    protected static enum MyRelTypes implements RelationshipType
    {
        R1
    }

    @Override
    public String getTestId()
    {
        return "EigenvectorCentralityRayleighBenchmark";
    }

    RandomGraph graph;

    @Override
    protected void setUpGlobal()
    {
        super.setUpGlobal();
        graph = new RandomGraph( neo, new GraphStore( neo ), MyRelTypes.R1,
            250, 2000 );
        Transaction tx = neo.beginTx();
        tx.finish();
    }

    @Override
    protected void runBenchMark()
    {
        Transaction tx = neo.beginTx();
        EigenvectorCentralityRayleigh eigenvectorCentralityRayleigh = new EigenvectorCentralityRayleigh(
            Direction.OUTGOING, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1.0;
                }
            }, graph.getNodes(), graph.getRelationships(), 0.0000001 );
        eigenvectorCentralityRayleigh.setMaxIterations( 100 );
        eigenvectorCentralityRayleigh.calculate();
        System.out.println( "Iterations: "
            + eigenvectorCentralityRayleigh.getTotalIterations() );
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
