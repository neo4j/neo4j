package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

object Metrics {
  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type costModel = LogicalPlan => Int

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for heuristics)
  type cardinalityEstimator = LogicalPlan => Int

  // This metric estimates the selectivity of an expression
  // (e.g. by algebraic analysis or using heuristics)
  type selectivityEstimator = Expression => Double

  def newCostModel[A](pf: PartialFunction[A, Int]) = pf.lift.andThen(_.getOrElse(Int.MaxValue))
  def newCardinalityEstimator[A](pf: PartialFunction[A, Int]) = pf.lift.andThen(_.getOrElse(Int.MaxValue))
  def newSelectivityEstimator[A](pf: PartialFunction[A, Double]) = pf.lift.andThen(_.getOrElse(1.0d))
}

