package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans


case class OuterHashJoin(node: IdName, left: LogicalPlan, right: LogicalPlan) extends LogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  val coveredIds = left.coveredIds ++ right.coveredIds
  val solvedPredicates = left.solvedPredicates ++ right.solvedPredicates
  val solvedPatterns = left.solvedPatterns ++ right.solvedPatterns
}
