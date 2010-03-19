package org.neo4j.graphalgo.shortestPath;

import org.neo4j.graphalgo.shortestpath.LevelShortestPathsFinder;
import org.neo4j.graphalgo.shortestpath.PathFinder;
import org.neo4j.graphdb.Direction;

public class TestLevelShortestPathsFinder extends TestSingleStepShortestPath
{
    @Override
    protected PathFinder instantiatePathFinder( int maxDepth )
    {
        return new LevelShortestPathsFinder( maxDepth, MyRelTypes.R1, Direction.BOTH );
    }
}
