/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner._

object ClauseConverters {

  import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters.SingleQueryPlanInput

  implicit class OptionalWhereConverter(val optWhere: Option[Where]) extends AnyVal {
    def asSelections = Selections(optWhere.
      map(_.expression.asPredicates).
      getOrElse(Set.empty))
  }

  implicit class SelectionsSubQueryExtraction(val selections: Selections) extends AnyVal {
    def getContainedPatternExpressions: Set[PatternExpression] = {
      val predicates = selections.predicates
      val predicatesWithCorrectDeps = predicates.map {
        case Predicate(deps, e: PatternExpression) =>
          Predicate(deps.filter(x => isNamed(x.name)), e)
        case Predicate(deps, ors@Ors(exprs)) =>
          val newDeps = exprs.foldLeft(Set.empty[IdName]) { (acc, exp) =>
            exp match {
              case exp: PatternExpression => acc ++ exp.idNames.filter(x => isNamed(x.name))
              case _                      => acc ++ exp.idNames
            }
          }
          Predicate(newDeps, ors)
        case p                               => p
      }

      val subQueries: Set[PatternExpression] = predicatesWithCorrectDeps.flatMap {
        case Predicate(_, Ors(orOperands)) =>
          orOperands.collect {
            case expr: PatternExpression      => expr
            case Not(expr: PatternExpression) => expr
          }

        case Predicate(_, Not(expr: PatternExpression)) =>
          Some(expr)

        case Predicate(_, expr: PatternExpression) =>
          Some(expr)

        case _ =>
          None
      }

      subQueries
    }
  }

  implicit class SortItems(val optOrderBy: Option[OrderBy]) extends AnyVal {
    def asQueryShuffle = {
      val sortItems: Seq[SortItem] = optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)
      QueryShuffle(sortItems, None, None)
    }
  }

  implicit class ReturnItemConverter(val items: Seq[ReturnItem]) extends AnyVal {
    def asQueryProjection(distinct: Boolean): QueryProjection = {
      val (aggregatingItems: Seq[ReturnItem], groupingKeys: Seq[ReturnItem]) =
        items.partition(item => IsAggregate(item.expression))

      def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

      val projectionMap = turnIntoMap(groupingKeys)
      val aggregationsMap = turnIntoMap(aggregatingItems)

      if (projectionMap.values.exists(containsAggregate))
        throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

      if (aggregationsMap.nonEmpty || distinct)
        AggregatingQueryProjection(groupingKeys = projectionMap, aggregationExpressions = aggregationsMap)
      else
        RegularQueryProjection(projections = projectionMap)
    }
  }

  implicit class ClauseConverter(val clause: Clause) extends AnyVal {
    def addToQueryPlanInput(acc: SingleQueryPlanInput): SingleQueryPlanInput = clause match {
      case c: Return => c.addReturnToQueryPlanInput(acc)
      case c: Match => c.addMatchToQueryPlanInput(acc)
      case x         => throw new CantHandleQueryException(x.toString)
    }
  }

  implicit class ReturnConverter(val clause: Return) extends AnyVal {
    def addReturnToQueryPlanInput(acc: SingleQueryPlanInput): SingleQueryPlanInput = clause match {
      case Return(distinct, ListedReturnItems(items), optOrderBy, skip, limit) =>

        val newPatternInExpressionTable =
          acc.patternExprTable ++
          items.flatMap(_.expression.extractPatternExpressions).
            map(x => x -> x.asQueryGraph)

        val shuffle = optOrderBy.asQueryShuffle.
          withSkip(skip).
          withLimit(limit)

        val projection = items.
          asQueryProjection(distinct).
          withShuffle(shuffle)

        val plannerQuery = acc.build().withHorizon(projection)

        acc.copy(
          q = plannerQuery,
          patternExprTable = newPatternInExpressionTable
        )
      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
    }
  }

  implicit class MatchConverter(val clause: Match) extends AnyVal {
    def addMatchToQueryPlanInput(acc: SingleQueryPlanInput): SingleQueryPlanInput = {
      val patternContent = clause.pattern.destructed

      val selections = clause.where.asSelections
      val subQueries = selections.getContainedPatternExpressions

      if (clause.optional) {
        acc.
          updateGraph { qg => qg.withAddedOptionalMatch(
          QueryGraph(
            selections = selections,
            patternNodes = patternContent.nodeIds.toSet,
            patternRelationships = patternContent.rels.toSet,
            hints = clause.hints.toSet,
            shortestPathPatterns = patternContent.shortestPaths.toSet
          ))
        }.
          addPatternExpressions(subQueries.toSeq: _*)
      } else {
        acc.updateGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(patternContent.nodeIds: _*).
            addPatternRels(patternContent.rels).
            addHints(clause.hints).
            addShortestPaths(patternContent.shortestPaths: _*)
        }.
          addPatternExpressions(subQueries.toSeq: _*)
      }
    }
  }

}
