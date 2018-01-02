/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v2_3.InternalException
import org.neo4j.cypher.internal.frontend.v2_3.ast._

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
    def addToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case c: Return => c.addReturnToLogicalPlanInput(acc)
      case c: Match => c.addMatchToLogicalPlanInput(acc)
      case c: With => c.addWithToLogicalPlanInput(acc)
      case c: Unwind => c.addUnwindToLogicalPlanInput(acc)
      case c: Start => c.addStartToLogicalPlanInput(acc)
      case x         => throw new CantHandleQueryException//(x.toString)
    }
  }

  implicit class ReturnConverter(val clause: Return) extends AnyVal {
    def addReturnToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case Return(distinct, ri, optOrderBy, skip, limit, _) if !ri.includeExisting =>

        val shuffle = optOrderBy.asQueryShuffle.
          withSkip(skip).
          withLimit(limit)

        val projection = ri.items.
          asQueryProjection(distinct).
          withShuffle(shuffle)
        val returns = ri.items.collect {
          case AliasedReturnItem(_, identifier) => IdName.fromIdentifier(identifier)
        }
        acc.
          withHorizon(projection).
          withReturns(returns)
      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
    }
  }

  implicit class ReturnItemsConverter(val clause: ReturnItems) extends AnyVal {
    def asReturnItems(current: QueryGraph): Seq[ReturnItem] =
      if (clause.includeExisting)
        QueryProjection.forIds(current.allCoveredIds) ++ clause.items
      else
        clause.items

    def getContainedPatternExpressions: Set[PatternExpression] =
      clause.items.flatMap(_.expression.extractPatternExpressions).toSet
  }

  implicit class MatchConverter(val clause: Match) extends AnyVal {
    def addMatchToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = {
      val patternContent = clause.pattern.destructed

      val selections = clause.where.asSelections

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
            addPatternRelationships(patternContent.rels).
            addHints(clause.hints).
            addShortestPaths(patternContent.shortestPaths: _*)
        }
      }
    }
  }

  implicit class WithConverter(val clause: With) extends AnyVal {

    private implicit def returnItemsToIdName(s: Seq[ReturnItem]):Set[IdName] =
      s.map(item => IdName(item.name)).toSet

    def addWithToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder = clause match {

      /*
      When encountering a WITH that is not an event horizon, and we have no optional matches in the current QueryGraph,
      we simply continue building on the current PlannerQuery. Our ASTRewriters rewrite queries in such a way that
      a lot of queries have these WITH clauses.

      Handles: ... WITH * [WHERE <predicate>] ...
       */
      case With(false, ri, None, None, None, where)
        if !builder.currentQueryGraph.hasOptionalPatterns
          && ri.items.forall(item => !containsAggregate(item.expression))
          && ri.items.forall {
          case item: AliasedReturnItem => item.expression == item.identifier
          case x => throw new InternalException("This should have been rewritten to an AliasedReturnItem.")
        } =>
        val selections = where.asSelections
        builder.
          updateGraph(_.addSelections(selections))

      /*
      When encountering a WITH that is an event horizon, we introduce the horizon and start a new empty QueryGraph.

      Handles all other WITH clauses
       */
      case With(distinct, projection, orderBy, skip, limit, where) =>
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

    def addUnwindToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder =
      builder.
        withHorizon(
          UnwindProjection(
            identifier = IdName(clause.identifier.name),
            exp = clause.expression)
        ).
        withTail(PlannerQuery.empty)
  }

  implicit class StartConverter(val clause: Start) extends AnyVal {
    def addStartToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder = {
        builder.updateGraph { qg =>
          val items = clause.items.map {
            case hints: LegacyIndexHint => Right(hints)
            case item              => Left(item)
          }

          val hints = items.collect { case Right(hint) => hint }
          val nonHints = items.collect { case Left(item) => item }

          if (nonHints.nonEmpty) {
            // all other start queries is delegated to legacy planner
            throw new CantHandleQueryException()
          }

          val nodeIds = hints.collect { case n: NodeHint => IdName(n.identifier.name)}

          val selections = clause.where.asSelections

          qg.addPatternNodes(nodeIds: _*)
            .addSelections(selections)
            .addHints(hints)
        }
    }
  }
}
