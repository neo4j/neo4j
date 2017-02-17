package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.SortDescription
import org.neo4j.cypher.internal.compiler.v3_2.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.neo4j.cypher.internal.ir.v3_2.IdName

case class Top(left: LogicalPlan, sortItems: Seq[SortDescription], limit: Expression)
              (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with EagerLogicalPlan {
  override def lhs: Option[LogicalPlan] = Some(left)

  override def rhs: Option[LogicalPlan] = None

  override def availableSymbols: Set[IdName] = left.availableSymbols
}