package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.ExecutionPlan

class Planner {
  def producePlan(ast: Query): ExecutionPlan = ???
}
