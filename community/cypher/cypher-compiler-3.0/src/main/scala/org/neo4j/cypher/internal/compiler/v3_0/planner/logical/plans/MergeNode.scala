package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v3_0.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{PropertyKeyName, Expression}

case class MergeNode(source: LogicalPlan, idName: IdName, labels: Seq[LazyLabel], properties: Map[PropertyKeyName, Expression])
                    (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LogicalPlanWithoutExpressions {
  override def lhs = Some(source)

  override def availableSymbols = source.availableSymbols + idName

  override def rhs = None

  override def strictness = source.strictness
}
