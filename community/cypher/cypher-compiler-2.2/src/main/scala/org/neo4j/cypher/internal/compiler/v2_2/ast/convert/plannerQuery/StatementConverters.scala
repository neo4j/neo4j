package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.helpers.CollectionSupport

object StatementConverters {
  object SingleQueryPlanInput {
    val empty = new SingleQueryPlanInput(PlannerQuery.empty, Map.empty)
  }

  case class SingleQueryPlanInput(private val q: PlannerQuery, patternExprTable: Map[PatternExpression, QueryGraph])
    extends CollectionSupport {
    def addPatternExpressions(expressions: PatternExpression*) = {
      copy(patternExprTable = patternExprTable ++ expressions.map(x => x -> x.asQueryGraph))
    }

    def updateGraph(f: QueryGraph => QueryGraph): SingleQueryPlanInput = copy(q = q.updateGraph(f))

    def build() = fixArgumentIds(q)

    private def fixArgumentIds(plannerQuery: PlannerQuery): PlannerQuery = {
      val optionalMatches = plannerQuery.graph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.graph.coveredIds) { case (args, qg) =>
        (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
      }
      plannerQuery
        .updateGraph(_.withOptionalMatches(newOptionalMatches))
        .updateTail(fixArgumentIds)
    }
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
