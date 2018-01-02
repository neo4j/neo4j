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

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.toList;

@Ignore( "Not a test, merely a performance measurement. Convert into a proper performance benchmark at some point" )
public class PathExplosionIT
{
    private static final boolean FIND_MULTIPLE_PATHS = false;
    private static final int MIN_PATH_LENGTH = 2;
    private static final int MAX_PATH_LENGTH = 10;
    private static final int MIN_PATH_WIDTH = 2;
    private static final int MAX_PATH_WIDTH = 6;
    private static final int WARMUP_RUNS = 2;
    private static final long TOLERATED_DURATION = 10000;
    
    @Test
    public void aStarShouldFinishWithinToleratedDuration() throws IOException
    {
        assertPathFinderCompletesWithinToleratedDuration( TOLERATED_DURATION,
                GraphAlgoFactory.aStar( expander, constantEvaluator, estimateEvaluator ) );
    }

    @Test
    public void dijkstraShouldFinishWithinToleratedDuration() throws IOException
    {
        assertPathFinderCompletesWithinToleratedDuration( TOLERATED_DURATION,
                GraphAlgoFactory.dijkstra( expander, constantEvaluator ) );
    }

    @Test
    public void shortestPathShouldFinishWithinToleratedDuration() throws IOException
    {
        assertPathFinderCompletesWithinToleratedDuration( TOLERATED_DURATION,
                GraphAlgoFactory.shortestPath( expander, Integer.MAX_VALUE ) );
    }

    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    void assertPathFinderCompletesWithinToleratedDuration( long toleratedRuntime, PathFinder<? extends Path> pathFinder )
            throws IOException
    {
        for ( int pathWidth = MIN_PATH_WIDTH; pathWidth <= MAX_PATH_WIDTH; pathWidth++ )
        {
            for ( int pathLength = MIN_PATH_LENGTH; pathLength <= MAX_PATH_LENGTH; pathLength++ )
            {
                FileUtils.deleteRecursively( testDir.directory() );
                GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
                try
                {
                    long[] startEndNodeIds = createPathGraphAndReturnStartAndEnd( pathLength, pathWidth, db );
                    long runTime = findPath( startEndNodeIds[0], startEndNodeIds[1], db, pathFinder, WARMUP_RUNS );
                    System.out.println( String.format( "Runtime[pathWidth:%s, pathLength:%s] = %s", pathWidth, pathLength,
                            runTime ) );
                    assertTrue( runTime < toleratedRuntime );
                }
                finally
                {
                    db.shutdown();
                }
            }
        }
    }

    long findPath( long startId, long endId, GraphDatabaseService db, PathFinder<? extends Path> pathFinder,
                   int warmUpRuns )
    {
        long runTime = -1;
        try ( Transaction tx = db.beginTx() )
        {
            Node startNode = db.getNodeById( startId );
            Node endNode = db.getNodeById( endId );
            for ( int run = 0; run < warmUpRuns; run++ )
            {
                runPathOnce( startNode, endNode, db, pathFinder );
            }
            runTime = runPathOnce( startNode, endNode, db, pathFinder );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
        return runTime;
    }

    long runPathOnce( Node startNode, Node endNode, GraphDatabaseService db, PathFinder<? extends Path> pathFinder )
    {
        long startTime = currentTimeMillis();
        
        Path path = FIND_MULTIPLE_PATHS ?
                // call findAllPaths, but just grab the first one
                toList( pathFinder.findAllPaths( startNode, endNode ) ).get( 0 ) :
                // call findinglePath
                pathFinder.findSinglePath( startNode, endNode );

        // iterate through the path
        count( path );
        
        return currentTimeMillis() - startTime;
    }

    long[] createPathGraphAndReturnStartAndEnd( int length, int width, GraphDatabaseService db )
    {
        long startId = -1;
        long endId = -1;
        try ( Transaction tx = db.beginTx() )
        {
            Node startNode = null;
            Node endNode = db.createNode();
            startId = endNode.getId();
            for ( int l = 0; l < length; l++ )
            {
                startNode = endNode;
                endNode = db.createNode();
                for ( int w = 0; w < width; w++ )
                {
                    startNode.createRelationshipTo( endNode, RelTypes.SomeRelType );
                }
            }
            endId = endNode.getId();
            tx.success();
        }
        return new long[] { startId, endId };
    }

    private enum RelTypes implements RelationshipType
    {
        SomeRelType
    }
    
    private final PathExpander<?> expander = PathExpanders.forDirection(Direction.BOTH );
    private final CostEvaluator<Double> constantEvaluator = new CostEvaluator<Double>()
    {
        @Override
        public Double getCost( Relationship relationship, Direction direction )
        {
            return 1D;
        }
    };
    private final EstimateEvaluator<Double> estimateEvaluator = new EstimateEvaluator<Double>()
    {
        @Override
        public Double getCost( Node node, Node goal )
        {
            return 1D;
        }
    };
}
