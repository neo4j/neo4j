package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.path.AStar;
import org.neo4j.graphalgo.impl.path.AllPaths;
import org.neo4j.graphalgo.impl.path.AllSimplePaths;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Static factory methods for the recommended implementations of common
 * graph algorithms for Neo4j.
 * 
 * @author Mattias Persson
 */
public abstract class GraphAlgoFactory
{
    /**
     * Returns an algorithm which can find all available paths between two
     * nodes. These returned paths can contain loops (i.e. a node can occur
     * more than once in any returned path).
     * @see AllPaths
     * @param expander the {@link RelationshipExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param maxDepth the max {@link Path#length()} returned paths are
     * allowed to have.
     * @return an algorithm which finds all paths between two nodes.
     */
    public static PathFinder<Path> allPaths( RelationshipExpander expander, int maxDepth )
    {
        return new AllPaths( maxDepth, expander );
    }

    /**
     * Returns an algorithm which can find all simple paths between two
     * nodes. These returned paths cannot contain loops (i.e. a node cannot
     * occur more than once in any returned path).
     * @see AllSimplePaths
     * @param expander the {@link RelationshipExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param maxDepth the max {@link Path#length()} returned paths are
     * allowed to have.
     * @return an algorithm which finds simple paths between two nodes.
     */
    public static PathFinder<Path> allSimplePaths( RelationshipExpander expander,
            int maxDepth )
    {
        return new AllSimplePaths( maxDepth, expander );
    }

    /**
     * Returns an algorithm which can find all shortest paths (i.e. paths with
     * as short {@link Path#length()} as possible) between two nodes. These
     * returned paths cannot contain loops (i.e. a node cannot occur more than 
     * once in any returned path).
     * @see ShortestPath
     * @param expander the {@link RelationshipExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param maxDepth the max {@link Path#length()} returned paths are
     * allowed to have.
     * @return an algorithm which finds shortest paths between two nodes.
     */
    public static PathFinder<Path> shortestPath( RelationshipExpander expander, int maxDepth )
    {
        return new ShortestPath( maxDepth, expander );
    }

    /**
     * Returns an {@link PathFinder} which uses the A* algorithm to find the
     * cheapest path between two nodes. The definition of "cheap" is the lowest
     * possible cost to get from the start node to the end node, where the cost
     * is returned from {@code lengthEvaluator} and {@code estimateEvaluator}.
     * These returned paths cannot contain loops (i.e. a node cannot occur more
     * than once in any returned path).
     * 
     * See http://en.wikipedia.org/wiki/A*_search_algorithm for more
     * information.
     * 
     * @see AStar
     * @param expander the {@link RelationshipExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param lengthEvaluator evaluator that can return the cost represented
     * by each relationship the algorithm traverses.
     * @param estimateEvaluator evaluator that returns an (optimistic)
     * estimation of the cost to get from the current node (in the traversal)
     * to the end node.
     * @return an algorithm which finds the cheapest path between two nodes
     * using the A* algorithm.
     */
    public static PathFinder<WeightedPath> aStar( RelationshipExpander expander,
            CostEvaluator<Double> lengthEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        return new AStar( expander, lengthEvaluator, estimateEvaluator );
    }

    /**
     * Returns an {@link PathFinder} which uses the Dijkstra algorithm to find
     * the cheapest path between two nodes. The definition of "cheap" is the
     * lowest possible cost to get from the start node to the end node, where
     * the cost is returned from {@code costEvaluator}. These returned paths
     * cannot contain loops (i.e. a node cannot occur more than once in any
     * returned path).
     * 
     * See http://en.wikipedia.org/wiki/Dijkstra%27s_algorithm for more
     * information.
     * 
     * @see Dijkstra
     * @param expander the {@link RelationshipExpander} to use for expanding
     * {@link Relationship}s for each {@link Node}.
     * @param costEvaluator evaluator that can return the cost represented
     * by each relationship the algorithm traverses.
     * @return an algorithm which finds the cheapest path between two nodes
     * using the Dijkstra algorithm.
     */
    public static PathFinder<WeightedPath> dijkstra( RelationshipExpander expander,
            CostEvaluator<Double> costEvaluator )
    {
        return new Dijkstra( expander, costEvaluator );
    }
}
