package org.neo4j.graphalgo.shortestpath;

import org.neo4j.graphdb.Node;

public interface EstimateEvaluator<CostType>
{
    CostType getCost( Node node, Node goal );
}
