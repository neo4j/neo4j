package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier

case class LogicalPlanContext(planContext: PlanContext, estimator: CardinalityEstimator, costs: CostModel, semanticTable: SemanticTable)

object NodeIdName {
  def unapply(v: Any)(implicit context: LogicalPlanContext): Option[IdName] = v match {
    case identifier @ Identifier(name) if context.semanticTable.isNode(identifier) => Some(IdName(identifier.name))
    case _                                                                         => None
  }
}

object RelationshipIdName {
  def unapply(v: Any)(implicit context: LogicalPlanContext): Option[IdName] = v match {
    case identifier @ Identifier(name) if context.semanticTable.isRelationship(identifier) => Some(IdName(identifier.name))
    case _                                                                                 => None
  }
}


