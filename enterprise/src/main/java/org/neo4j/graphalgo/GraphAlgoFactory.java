package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.path.AStar;
import org.neo4j.graphalgo.impl.path.AllPaths;
import org.neo4j.graphalgo.impl.path.AllSimplePaths;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Static factory methods for the recommended implementations of the common
 * graph algorithms.
 * 
 * @author Mattias Persson
 */
public abstract class GraphAlgoFactory
{
    public static PathFinder<Path> allPaths( RelationshipExpander expander, int maxDepth )
    {
        return new AllPaths( maxDepth, expander );
    }

    public static PathFinder<Path> allSimplePaths( RelationshipExpander expander,
            int maxDepth )
    {
        return new AllSimplePaths( maxDepth, expander );
    }

    public static PathFinder<Path> shortestPath( RelationshipExpander expander, int maxDepth )
    {
        return new ShortestPath( maxDepth, expander );
    }

    public static PathFinder<WeightedPath> aStar( RelationshipExpander expander,
            CostEvaluator<Double> lengthEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        return new AStar( expander, lengthEvaluator, estimateEvaluator );
    }

    public static PathFinder<WeightedPath> dijkstra( RelationshipExpander expander,
            CostEvaluator<Double> costEvaluator )
    {
        return new Dijkstra( expander, costEvaluator );
    }
}
