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
package org.neo4j.graphalgo.path;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.graphalgo.CommonEvaluators.doubleCostEvaluator;
import static org.neo4j.graphalgo.GraphAlgoFactory.aStar;
import static org.neo4j.graphdb.PathExpanders.allTypesAndDirections;

@Ignore( "Not a test, just nice to have" )
public class AStarPerformanceIT
{
    private File directory;

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void setup()
    {
        directory = testDirectory.directory( "graph-db" );
    }

    @Test
    public void somePerformanceTesting() throws Exception
    {
        // GIVEN
        int numberOfNodes = 200000;
        GeoDataGenerator generator = new GeoDataGenerator( numberOfNodes, 5d, 1000, 1000 );
        generator.generate( directory );

        // WHEN
        long[][] points = new long[][]{
                new long[]{9415, 158154},
                new long[]{89237, 192863},
                new long[]{68072, 150484},
                new long[]{186309, 194495},
                new long[]{152097, 99289},
                new long[]{92150, 161182},
                new long[]{188446, 115873},
                new long[]{85033, 7772},
                new long[]{291, 86707},
                new long[]{188345, 158468}
        };
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.getAbsolutePath() );
        PathFinder<WeightedPath> algo = aStar( allTypesAndDirections(),
                doubleCostEvaluator( "weight", 0 ), GeoDataGenerator.estimateEvaluator() );
        for ( int i = 0; i < 10; i++ )
        {
            System.out.println( "----- " + i );
            for ( long[] p : points )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node start = db.getNodeById( p[0] );
                    Node end = db.getNodeById( p[1] );
                    long time = currentTimeMillis();
                    WeightedPath path = algo.findSinglePath( start, end );
                    time = currentTimeMillis() - time;
                    System.out.println( "time: " + time + ", len:" + path.length() + ", weight:" + path.weight() );
                    tx.success();
                }
            }
        }

        // THEN
        db.shutdown();
    }
}
