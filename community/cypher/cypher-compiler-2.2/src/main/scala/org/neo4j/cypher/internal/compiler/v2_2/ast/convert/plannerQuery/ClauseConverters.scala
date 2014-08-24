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
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName

object ClauseConverters {

  implicit class OptionalWhereConverter(val optWhere: Option[Where]) extends AnyVal {
    def asSelections = Selections(optWhere.
      map(_.expression.asPredicates).
      getOrElse(Set.empty))
  }

  implicit class SelectionsSubQueryExtraction(val selections: Selections) extends AnyVal {
    def getContainedPatternExpressions: Set[PatternExpression] =
      selections.flatPredicates.flatMap(_.extractPatternExpressions).toSet
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
    def addToQueryPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case c: Return => c.addReturnToQueryPlanInput(acc)
      case c: Match => c.addMatchToQueryPlanInput(acc)
      case c: With => c.addWithToQueryPlanInput(acc)
      case c: Unwind => c.addUnwindToQueryPlanInput(acc)
      case x         => throw new CantHandleQueryException(x.toString)
    }
  }

  implicit class ReturnConverter(val clause: Return) extends AnyVal {
    def addReturnToQueryPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case Return(distinct, returnItems@ListedReturnItems(items), optOrderBy, skip, limit) =>

        val shuffle = optOrderBy.asQueryShuffle.
          withSkip(skip).
          withLimit(limit)

        val projection = items.
          asQueryProjection(distinct).
          withShuffle(shuffle)

        acc.
          withHorizon(projection)

      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
    }
  }

  implicit class ReturnItemsConverter(val clause: ReturnItems) extends AnyVal {
    def asReturnItems(current: QueryGraph): Seq[ReturnItem] = clause match {
      case _: ReturnAll => QueryProjection.forIds(current.allCoveredIds)
      case ListedReturnItems(items) => items
    }

    def getContainedPatternExpressions:Set[PatternExpression] = clause match {
      case _: ReturnAll => Set.empty
      case ListedReturnItems(items) => items.flatMap(_.expression.extractPatternExpressions).toSet
    }
  }

  implicit class MatchConverter(val clause: Match) extends AnyVal {
    def addMatchToQueryPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = {
      val patternContent = clause.pattern.destructed

      val selections = clause.where.asSelections
      val subQueries = selections.getContainedPatternExpressions

      if (clause.optional) {
        acc.
          updateGraph { qg => qg.withAddedOptionalMatch(
          // When adding QueryGraphs for optional matches, we always start with a new one.
          // It's either all or nothing per match clause.
          QueryGraph(
            selections = selections,
            patternNodes = patternContent.nodeIds.toSet,
            patternRelationships = patternContent.rels.toSet,
            hints = clause.hints.toSet,
            shortestPathPatterns = patternContent.shortestPaths.toSet
          ))
        }
      } else {
        acc.updateGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(patternContent.nodeIds: _*).
            addPatternRels(patternContent.rels).
            addHints(clause.hints).
            addShortestPaths(patternContent.shortestPaths: _*)
        }
      }
    }
  }

  implicit class WithConverter(val clause: With) extends AnyVal {

    private implicit def returnItemsToIdName(s: Seq[ReturnItem]):Set[IdName] =
      s.map(item => IdName(item.name)).toSet

    def addWithToQueryPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder = clause match {

      /*
      When encountering a WITH that is not an event horizon, and we have no optional matches in the current QueryGraph,
      we simply continue building on the current PlannerQuery. Our ASTRewriters rewrite queries in such a way that
      a lot of queries have these WITH clauses.

      Handles: ... WITH * [WHERE <predicate>] ...
       */
      case With(false, _: ReturnAll, None, None, None, where) if !builder.currentQueryGraph.hasOptionalPatterns =>
        val selections = where.asSelections

        builder.
          updateGraph(_.addSelections(selections))

      /*
      When encountering a WITH that is an event horizon, we introduce the horizon and start a new empty QueryGraph.

      Handles all other WITH clauses
       */
      case With(distinct, projection: ReturnItems, orderBy, skip, limit, where) =>
        val selections = where.asSelections
        val returnItems = projection.asReturnItems(builder.currentQueryGraph)

        val shuffle =
          orderBy.
            asQueryShuffle.
            withLimit(limit).
            withSkip(skip)

        val queryProjection =
          returnItems.
            asQueryProjection(distinct).
            withShuffle(shuffle)

        builder.
          withHorizon(queryProjection).
          withTail(PlannerQuery(QueryGraph(selections = selections)))


      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)    }
  }

  implicit class UnwindConverter(val clause: Unwind) extends AnyVal {

    def addUnwindToQueryPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder =
      builder.
        withHorizon(
          UnwindProjection(
            identifier = IdName(clause.identifier.name),
            exp = clause.expression)
        ).
        withTail(PlannerQuery.empty)
  }
}
