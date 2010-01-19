package org.neo4j.graphalgo.shortestpath;
/*
 * 
 */
public interface MaxCostComparator<CostType> {
	/**
	 * 
	 * @param currentCost the cost to be checked
	 * @return true if the maximum Cost is less that currentCost
	 */
	public boolean maxCostExceeded(CostType currentCost);

}
