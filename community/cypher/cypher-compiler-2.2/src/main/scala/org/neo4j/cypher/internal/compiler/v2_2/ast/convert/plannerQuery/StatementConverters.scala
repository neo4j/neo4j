package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._

object StatementConverters {
  object SingleQueryPlanInput {
    val empty = new PlannerQueryBuilder(PlannerQuery.empty, Map.empty)
  }

  implicit class QueryConverter(val query: Query) {
    def asQueryPlanInput: QueryPlanInput = query match {
      case Query(None, SingleQuery(clauses)) =>
        val input = clauses.foldLeft(SingleQueryPlanInput.empty) {
          case (acc, clause) => clause.addToQueryPlanInput(acc)
        }

        val singeQueryPlanInput = input.build()

        QueryPlanInput(
          query = UnionQuery(Seq(singeQueryPlanInput), distinct = false),
          patternInExpression = input.patternExprTable
        )

      case _ =>
        throw new CantHandleQueryException
    }

  }
}
