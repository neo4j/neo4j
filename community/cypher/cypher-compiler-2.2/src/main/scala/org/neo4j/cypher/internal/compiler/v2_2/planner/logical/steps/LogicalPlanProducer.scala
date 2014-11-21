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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v2_2.pipes.SortDescription
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{Limit => LimitPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, ast}
import org.neo4j.graphdb.Direction

object LogicalPlanProducer {
  def solvePredicate(plan: LogicalPlan, solved: Expression) =
     plan.updateSolved(_.updateGraph(_.addPredicates(solved)))

  def planAggregation(left: LogicalPlan, grouping: Map[String, Expression], aggregation: Map[String, Expression]) =
    Aggregation(left, grouping, aggregation)(
      left.solved.updateTailOrSelf(_.withHorizon(
        AggregatingQueryProjection(groupingKeys = grouping, aggregationExpressions = aggregation)
      )))

  def planAllNodesScan(idName: IdName, argumentIds: Set[IdName]) =
    AllNodesScan(idName, argumentIds)(
      PlannerQuery(graph = QueryGraph(
        argumentIds = argumentIds,
        patternNodes = Set(idName))))

  def planApply(left: LogicalPlan, right: LogicalPlan) = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved = right.solved.updateTailOrSelf(_.updateGraph(_.withArgumentIds(Set.empty)))

    Apply(left, right)(solved = left.solved ++ rhsSolved)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan) =
    Apply(left, right)(
      solved = left.solved.updateTailOrSelf(_.withTail(right.solved)))

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan) = {
    assert((left.solved.graph.allCoveredIds intersect right.solved.graph.allCoveredIds).isEmpty)
    CartesianProduct(left, right)(
      left.solved ++ right.solved)
  }

  def planDirectedRelationshipByIdSeek(idName: IdName,
                                       relIds: EntityByIdRhs,
                                       startNode: IdName,
                                       endNode: IdName,
                                       pattern: PatternRelationship,
                                       argumentIds: Set[IdName],
                                       solvedPredicates: Seq[Expression] = Seq.empty) =
    DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternRel(pattern)
        .addPredicates(solvedPredicates: _*)
        .addArgumentIds(argumentIds.toSeq)
      )
    )

  def planUndirectedRelationshipByIdSeek(idName: IdName,
                                         relIds: EntityByIdRhs,
                                         leftNode: IdName,
                                         rightNode: IdName,
                                         pattern: PatternRelationship,
                                         argumentIds: Set[IdName],
                                         solvedPredicates: Seq[Expression] = Seq.empty) =
    UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternRel(pattern)
        .addPredicates(solvedPredicates: _*)
        .addArgumentIds(argumentIds.toSeq)
      )
    )

  def planExpand(left: LogicalPlan,
                 from: IdName,
                 dir: Direction,
                 projectedDir: Direction,
                 types: Seq[ast.RelTypeName],
                 to: IdName,
                 relName: IdName,
                 length: PatternLength,
                 pattern: PatternRelationship,
                 predicates: Seq[(Identifier, Expression)] = Seq.empty,
                 allPredicates: Seq[Expression] = Seq.empty) =
    Expand(left, from, dir, projectedDir, types, to, relName, length, predicates)(
      left.solved.updateGraph(_
        .addPatternRel(pattern)
        .addPredicates(allPredicates: _*)
      ))

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan) =
    Selection(predicates, left)(left.solved)

  def planNodeByIdSeek(idName: IdName, nodeIds: EntityByIdRhs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[IdName]) =
    NodeByIdSeek(idName, nodeIds, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addArgumentIds(argumentIds.toSeq)
      )
    )

  def planNodeByLabelScan(idName: IdName, label: Either[String, LabelId], solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None, argumentIds: Set[IdName]) =
    NodeByLabelScan(idName, label, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toSeq)
      )
    )

  def planNodeIndexSeek(idName: IdName,
                        label: ast.LabelToken,
                        propertyKey: ast.PropertyKeyToken,
                        valueExpr: QueryExpression[Expression], solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[IdName]) = {
    NodeIndexSeek(idName, label, propertyKey, valueExpr, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toSeq)
      )
    )
  }

  def planLegacyHintSeek(idName: IdName, hint: LegacyIndexHint, argumentIds: Set[IdName]) = {
    LegacyIndexSeek(idName, hint, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addHints(Some(hint))
        .addArgumentIds(argumentIds.toSeq)
      )
    )
  }

  def planNodeHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan) =
    NodeHashJoin(nodes, left, right)(
      left.solved ++ right.solved
    )

  def planNodeIndexUniqueSeek(idName: IdName,
                              label: ast.LabelToken,
                              propertyKey: ast.PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[IdName]) =
    NodeIndexUniqueSeek(idName, label, propertyKey, valueExpr, argumentIds)(
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toSeq)
      )
    )

  def planOptionalExpand(left: LogicalPlan,
                         from: IdName,
                         dir: Direction,
                         types: Seq[ast.RelTypeName],
                         to: IdName,
                         relName: IdName,
                         length: PatternLength,
                         predicates: Seq[Expression],
                         solvedQueryGraph: QueryGraph) =
    OptionalExpand(left, from, dir, types, to, relName, length, predicates)(
      left.solved.updateGraph(_.withAddedOptionalMatch(solvedQueryGraph))
    )

  def planOptional(inputPlan: LogicalPlan, ids: Set[IdName]) =
    Optional(inputPlan)(
      PlannerQuery(graph = QueryGraph.empty
        .withAddedOptionalMatch(inputPlan.solved.graph)
        .withArgumentIds(ids)
      )
    )

  def planOuterHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan) =
    OuterHashJoin(nodes, left, right)(
      left.solved.updateGraph(_.withAddedOptionalMatch(right.solved.graph))
    )

  def planSelection(predicates: Seq[Expression], left: LogicalPlan) =
    Selection(predicates, left)(
      left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicates: _*))))

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression) =
    SelectOrAntiSemiApply(outer, inner, expr)(
      outer.solved
    )

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression) =
    LetSelectOrAntiSemiApply(outer, inner, id, expr)(
      outer.solved
    )

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression) =
    SelectOrSemiApply(outer, inner, expr)(
      outer.solved
    )

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression) =
    LetSelectOrSemiApply(outer, inner, id, expr)(
      outer.solved
    )

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName) =
    LetAntiSemiApply(left, right, id)(
      left.solved
    )

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName) =
    LetSemiApply(left, right, id)(
      left.solved
    )

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: PatternExpression, solved: Expression) =
    AntiSemiApply(left, right)(
      left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(solved)))
    )

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: Expression) =
    SemiApply(left, right)(
      left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicate)))
    )

  def planQueryArgumentRow(queryGraph: QueryGraph): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgumentRow(patternNodes, patternRels, otherIds)
  }

  def planArgumentRow(patternNodes: Set[IdName], patternRels: Set[PatternRelationship] = Set.empty, other: Set[IdName] = Set.empty): LogicalPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other
    val typeInfoSeq =
      patternNodes.toSeq.map((x: IdName) => x.name -> CTNode) ++
        relIds.toSeq.map((x: IdName) => x.name -> CTRelationship) ++
        other.toSeq.map((x: IdName) => x.name -> CTAny)
    val typeInfo = typeInfoSeq.toMap

    val singleRowPlan =
      Argument(coveredIds)(
        PlannerQuery(graph =
        QueryGraph(
          argumentIds = coveredIds,
          patternNodes = patternNodes,
          patternRelationships = Set.empty
        ))
      )(typeInfo)

    singleRowPlan
  }

  def planSingleRow() =
    SingleRow()

  def planStarProjection(inner: LogicalPlan, expressions: Map[String, Expression]) =
    inner.updateSolved(
      _.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions)))
    )

  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression]) =
    Projection(inner, expressions)(
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions)))
    )

  def planSkip(inner: LogicalPlan, count: Expression) =
    SkipPlan(inner, count)(
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkipExpression(count))))
    )

  def planUnwind(inner: LogicalPlan, name: IdName, expression: Expression) =
    UnwindCollection(inner, name, expression)(
      inner.solved.updateTailOrSelf(_.withHorizon(UnwindProjection(name, expression)))
    )

  def planLimit(inner: LogicalPlan, count: Expression) =
    LimitPlan(inner, count)(
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimitExpression(count))))
    )

  def planSort(inner: LogicalPlan, descriptions: Seq[SortDescription], items: Seq[ast.SortItem]) =
    Sort(inner, descriptions)(
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))))
    )

  def planSortedLimit(inner: LogicalPlan, limit: Expression, items: Seq[ast.SortItem]) =
    SortedLimit(inner, limit, items)(
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(
        _.withLimitExpression(limit)
         .withSortItems(items))))
    )

  def planSortedSkipAndLimit(inner: LogicalPlan, skip: Expression, limit: Expression, items: Seq[ast.SortItem]) =
    planSkip(
      SortedLimit(inner, ast.Add(limit, skip)(limit.position), items)(
        inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(
          _.withSkipExpression(skip)
           .withLimitExpression(limit)
           .withSortItems(items))))
      ),
      skip
    )

  def planShortestPaths(inner: LogicalPlan, shortestPaths: ShortestPathPattern) =
    FindShortestPaths(inner, shortestPaths)(
      inner.solved.updateGraph(_.addShortestPath(shortestPaths))
    )

  def planEndpointProjection(inner: LogicalPlan, start: IdName, end: IdName, predicates: Seq[Expression], patternRel: PatternRelationship) = {
    val solved = inner.solved.updateGraph(_.addPatternRel(patternRel))

    val projectedPlan = ProjectEndpoints(inner, patternRel.name, start, end, patternRel.dir != Direction.BOTH, patternRel.length)_
    if (predicates.isEmpty)
      projectedPlan(solved)
    else
      Selection(predicates, projectedPlan(inner.solved))(solved)
  }

  def planUnion(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    Union(left, right)(left.solved)
    /* TODO: This is not correct in any way.
     LogicalPlan.solved contains a PlannerQuery, but to represent a Union, we'd need a UnionQuery instead
     Not very important at the moment, but dirty.
     */
  }

  def planDistinct(left: LogicalPlan): LogicalPlan = {
    val returnAll = QueryProjection.forIds(left.availableSymbols) map {
      case AliasedReturnItem(e, Identifier(key)) => key -> e // This smells awful.
    }

    Aggregation(left, returnAll.toMap, Map.empty)(left.solved)
  }
}
