package org.neo4j.graphalgo;

/**
 * Evaluator for determining if the maximum path cost has been exceeded.
 *
 * @author Peter Neubauer
 * @param <T> The cost value type
 */
public interface MaxCostEvaluator<T>
{
    /**
     * Evaluates whether the maximum cost has been exceeded.
     * 
     * @param currentCost the cost to be checked
     * @return true if the maximum Cost is less that currentCost
     */
    public boolean maxCostExceeded( T currentCost );
}
