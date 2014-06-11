package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

case class FindShortestPaths(left: LogicalPlan, shortestPath: ShortestPathPattern) extends LogicalPlan {
  val lhs = Some(left)
  def rhs = None

  def availableSymbols = left.availableSymbols ++ shortestPath.availableSymbols
}
