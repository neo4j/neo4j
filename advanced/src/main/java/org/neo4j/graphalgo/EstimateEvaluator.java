package org.neo4j.graphalgo;

import org.neo4j.graphdb.Node;

/**
 * Evaluator used to estimate the weight of the remaining path from one node to
 * another.
 *
 * @author Mattias Persson
 * @param <T> The data type of the estimated weight.
 */
public interface EstimateEvaluator<T>
{
    /**
     * Estimate the weight of the remaining path from one node to another.
     *
     * @param node the node to estimate the weight from.
     * @param goal the node to estimate the weight to.
     * @return an estimation of the weight of the path from the first node to
     *         the second.
     */
    T getCost( Node node, Node goal );
}
