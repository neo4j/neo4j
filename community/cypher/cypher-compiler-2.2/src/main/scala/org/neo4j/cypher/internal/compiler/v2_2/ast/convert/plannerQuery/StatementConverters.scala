package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._

object StatementConverters {
  object SingleQueryPlanInput {
    val empty = new SingleQueryPlanInput(PlannerQuery.empty, Map.empty)
  }

  case class SingleQueryPlanInput(q: PlannerQuery, patternExprTable: Map[PatternExpression, QueryGraph])

  implicit class QueryConverter(val query: Query) {
    def asQueryPlanInput: QueryPlanInput = query match {
      case Query(None, SingleQuery(clauses)) =>
        val singleQueryPlanInput = clauses.foldLeft(SingleQueryPlanInput.empty) {
          case (acc, clause) => clause.addToQueryPlanInput(acc)
        }

        QueryPlanInput(
          query = UnionQuery(Seq(singleQueryPlanInput.q), distinct = false),
          patternInExpression = singleQueryPlanInput.patternExprTable
        )

      //      case Query(None, u: ast.Union) =>
      //        val queries = u.unionedQueries
      //        val distinct = u match {
      //          case _: UnionAll      => false
      //          case _: UnionDistinct => true
      //        }
      //        val plannedQueries: Seq[SingleQueryPlanInput] = queries.reverseMap(x => produceQueryGraphFromClauses(SingleQueryPlanInput.empty, x.clauses))
      //        val table = plannedQueries.map(_.patternExprTable).reduce(_ ++ _)
      //        QueryPlanInput(
      //          query = UnionQuery(plannedQueries.map(_.q), distinct),
      //          patternInExpression = table
      //        )

      case _ =>
        throw new CantHandleQueryException
    }

  }
}
