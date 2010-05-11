package org.neo4j.graphalgo;

/**
 * Evaluator for determining if the maximum path cost has been exceeded.
 * 
 * @author Peter Neubauer
 * @param <COST_TYPE> The cost value type
 */
public interface MaxCostEvaluator<COST_TYPE>
{
    /**
     * 
     * @param currentCost
     *            the cost to be checked
     * @return true if the maximum Cost is less that currentCost
     */
    public boolean maxCostExceeded( COST_TYPE currentCost );
}
