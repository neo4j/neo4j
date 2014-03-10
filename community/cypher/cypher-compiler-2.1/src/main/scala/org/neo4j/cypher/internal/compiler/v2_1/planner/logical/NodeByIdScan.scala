package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

case class NodeByIdScan(idName: IdName, nodeId: Expression, cardinality: Int) extends LogicalPlan {
  def coveredIds: Set[IdName] = Set(idName)

  def cost: Int = ???

  def rhs: Option[LogicalPlan] = None
  def lhs: Option[LogicalPlan] = None
}
