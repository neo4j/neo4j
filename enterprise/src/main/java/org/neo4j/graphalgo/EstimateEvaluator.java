package org.neo4j.graphalgo;

import org.neo4j.graphdb.Node;

public interface EstimateEvaluator<CostType>
{
    CostType getCost( Node node, Node goal );
}
