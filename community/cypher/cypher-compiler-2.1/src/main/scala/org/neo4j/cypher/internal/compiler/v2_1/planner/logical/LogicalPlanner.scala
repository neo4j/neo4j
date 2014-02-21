package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, CostModel, CardinalityEstimator}

class LogicalPlanner(estimator: CardinalityEstimator, costModel: CostModel) {
  def plan(in: QueryGraph): LogicalPlan = ???
}
