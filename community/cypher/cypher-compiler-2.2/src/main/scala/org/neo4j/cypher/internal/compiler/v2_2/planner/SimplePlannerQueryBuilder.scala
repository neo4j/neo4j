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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ast
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.helpers.CollectionSupport


class SimplePlannerQueryBuilder extends PlannerQueryBuilder with CollectionSupport {

  private def getSelectionsAndSubQueries(optWhere: Option[Where]): (Selections, Seq[(PatternExpression, QueryGraph)]) = {
    val selections = optWhere.asSelections
    val subQueries = selections.getContainedPatternExpressions.toSeq.map(x => x -> x.asQueryGraph)

    (selections, subQueries)
  }

  private def extractPatternInExpressionFromWhere(optWhere: Option[Where]): Seq[(PatternExpression, QueryGraph)] = {
    val expressions = optWhere.asSelections.flatPredicates

    def containsNestedPatternExpressions(expr: Expression): Boolean = expr match {
      case _: PatternExpression      => false
      case Not(_: PatternExpression) => false
      case Ors(exprs)                => exprs.exists(containsNestedPatternExpressions)
      case expr                      => expr.exists { case _: PatternExpression => true}
    }

    val expressionsWithNestedPatternExpr = expressions.filter(containsNestedPatternExpressions)
    getPatternInExpressionQueryGraphs(expressionsWithNestedPatternExpr)
  }

  private def getPatternInExpressionQueryGraphs(expressions: Seq[Expression]): Seq[(PatternExpression, QueryGraph)] = {

    val patternExpressions = expressions.treeFold(Seq.empty[PatternExpression]) {
      case p: PatternExpression =>
        (acc, _) => acc :+ p
    }

    patternExpressions.map { e => e -> e.asQueryGraph }
  }

  override def produce(parsedQuery: Query): QueryPlanInput = parsedQuery match {
    case Query(None, SingleQuery(clauses)) =>
      val input @ SingleQueryPlanInput(query, table) = produceQueryGraphFromClauses(SingleQueryPlanInput.empty, clauses)
      val singleQueryPlanInput = input.copy(q = fixArgumentIds(input.q))

      QueryPlanInput(
        query = UnionQuery(Seq(singleQueryPlanInput.q), distinct = false),
        patternInExpression = singleQueryPlanInput.patternExprTable
      )

    case Query(None, u: ast.Union) =>
      val queries = u.unionedQueries
      val distinct = u match {
        case _: UnionAll      => false
        case _: UnionDistinct => true
      }
      val plannedQueries: Seq[SingleQueryPlanInput] = queries.reverseMap(x => {
        val input @ SingleQueryPlanInput(query, table) = produceQueryGraphFromClauses(SingleQueryPlanInput.empty, x.clauses)
        input.copy(q = fixArgumentIds(input.q))
      })
      val table = plannedQueries.map(_.patternExprTable).reduce(_ ++ _)
      QueryPlanInput(
        query = UnionQuery(plannedQueries.map(_.q), distinct),
        patternInExpression = table
      )

    case _ =>
      throw new CantHandleQueryException
  }

  object SingleQueryPlanInput {
    val empty = new SingleQueryPlanInput(PlannerQuery.empty, Map.empty)
  }

  case class SingleQueryPlanInput(q: PlannerQuery, patternExprTable: Map[PatternExpression, QueryGraph])

  private def produceQueryGraphFromClauses(input: SingleQueryPlanInput, clauses: Seq[Clause]): SingleQueryPlanInput = {
    clauses match {
      case Return(distinct, ListedReturnItems(items), optOrderBy, skip, limit) :: tl =>
        val newPatternInExpressionTable = input.patternExprTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression))
        val sortItems = produceSortItems(optOrderBy)
        val shuffle = QueryShuffle(sortItems, skip.map(_.expression), limit.map(_.expression))
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)
        val newQG = input.q.withHorizon(projection)
        val nextStep = input.copy(
          q = newQG,
          patternExprTable = newPatternInExpressionTable
        )
        produceQueryGraphFromClauses(nextStep, tl)

      case Match(optional@false, pattern: Pattern, hints, optWhere) :: tl =>
        val patternContent = pattern.destructed

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries ++ nestedPatternPredicates

        val plannerQuery = input.q.updateGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(patternContent.nodeIds: _*).
            addPatternRels(patternContent.rels).
            addHints(hints).
            addShortestPaths(patternContent.shortestPaths: _*)
        }

        val nextStep = input.copy(q = plannerQuery, patternExprTable = newPatternInExpressionTable)
        produceQueryGraphFromClauses(nextStep, tl)

      case Match(optional@true, pattern: Pattern, hints, optWhere) :: tl =>
        val patternContent = pattern.destructed

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries ++ nestedPatternPredicates

        val optionalMatch = QueryGraph(
          selections = selections,
          patternNodes = patternContent.nodeIds.toSet,
          patternRelationships = patternContent.rels.toSet,
          hints = hints.toSet,
          shortestPathPatterns = patternContent.shortestPaths.toSet
        )

        val newQuery = input.q.updateGraph {
          qg => qg.withAddedOptionalMatch(optionalMatch)
        }

        val nextStep = input.copy(q = newQuery, patternExprTable = newPatternInExpressionTable)

        produceQueryGraphFromClauses(nextStep, tl)

      case With(false, _: ReturnAll, None, None, None, optWhere) :: tl if !input.q.graph.hasOptionalPatterns =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries
        val newQuery = input.q
          .updateGraph(_.addSelections(selections))

        val nextStep = input.copy(q = newQuery, patternExprTable = newPatternInExpressionTable)
        produceQueryGraphFromClauses(nextStep, tl)

      case With(distinct, _: ReturnAll, optOrderBy, skip, limit, optWhere) :: tl =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries

        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery(QueryGraph(selections = selections)), newPatternInExpressionTable)
        val tailPlannedOutput = produceQueryGraphFromClauses(tailInput, tl)

        val inputIds = input.q.graph.allCoveredIds
        val tailQuery = tailPlannedOutput.q
        val argumentIds = inputIds intersect tailQuery.graph.allCoveredIds
        val items = QueryProjection.forIds(inputIds)
        val shuffle = QueryShuffle(
          produceSortItems(optOrderBy),
          skip.map(_.expression),
          limit.map(_.expression)
        )
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)

        val newQuery =
          input.q
            .withHorizon(projection)
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, tailPlannedOutput.patternExprTable)

      case With(distinct, ListedReturnItems(items), optOrderBy, skip, limit, optWhere) :: tl =>
        val orderBy = produceSortItems(optOrderBy)
        val shuffle = QueryShuffle(orderBy, skip.map(_.expression), limit.map(_.expression))
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

        val newPatternInExpressionTable = input.patternExprTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression)) ++ subQueries
        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery(QueryGraph(selections = selections)), newPatternInExpressionTable)
        val tail = produceQueryGraphFromClauses(tailInput, tl)

        val inputIds = projection.keySet.map(IdName)
        val argumentIds = inputIds intersect tail.q.graph.allCoveredIds

        val newQuery =
          input.q
            .withHorizon(projection)
            .withTail(tail.q.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, patternExprTable = tail.patternExprTable)

      case Unwind(expression, identifier) :: tl =>

        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery.empty, input.patternExprTable)
        val tailPlannedOutput: SingleQueryPlanInput = produceQueryGraphFromClauses(tailInput, tl)
        val tailQuery: PlannerQuery = tailPlannedOutput.q
        val inputIds = input.q.graph.allCoveredIds
        val argumentIds = inputIds intersect tailQuery.graph.allCoveredIds
        val unwind = UnwindProjection(IdName(identifier.name), expression)

        val newQuery =
          input.q
            .withHorizon(unwind)
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, tailPlannedOutput.patternExprTable)

      case Seq() =>
        input

      case x =>
        throw new CantHandleQueryException(x.toString())
    }
  }

  private def fixArgumentIds(plannerQuery: PlannerQuery): PlannerQuery = {
    val optionalMatches = plannerQuery.graph.optionalMatches
    val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.graph.coveredIds) { case (args, qg) =>
      (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
    }
    plannerQuery
      .updateGraph(_.withOptionalMatches(newOptionalMatches))
      .updateTail(fixArgumentIds)
  }

  private def produceSortItems(optOrderBy: Option[OrderBy]) =
    optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)

  private def produceProjectionsMaps(items: Seq[ReturnItem], distinct: Boolean): QueryProjection = {
    val (aggregatingItems: Seq[ReturnItem], nonAggrItems: Seq[ReturnItem]) =
      items.partition(item => IsAggregate(item.expression))

    def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

    val projectionMap = turnIntoMap(nonAggrItems)
    val aggregationsMap = turnIntoMap(aggregatingItems)

    if(projectionMap.values.exists(containsAggregate))
      throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

    if (aggregationsMap.nonEmpty || distinct)
      AggregatingQueryProjection(groupingKeys = projectionMap, aggregationExpressions = aggregationsMap)
    else
      RegularQueryProjection(projections = projectionMap)
  }
}
