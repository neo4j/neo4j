/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{Limit => LimitPlan, Skip => SkipPlan}
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.pipes.SortDescription
import org.neo4j.cypher.internal.compiler.v2_1.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.LabelId

object QueryPlanProducer {
  def solvePredicate(plan: QueryPlan, solved: Expression) =
    QueryPlan(
      plan.plan,
      plan.solved.updateGraph(_.addPredicates(solved))
    )

  def planAggregation(left: QueryPlan, grouping: Map[String, Expression], aggregation: Map[String, Expression]) =
    QueryPlan(
      Aggregation(left.plan, grouping, aggregation),
      left.solved.updateTailOrSelf(_.withHorizon(
        AggregatingQueryProjection(groupingKeys = grouping, aggregationExpressions = aggregation)
      ))
    )

  def planAllNodesScan(idName: IdName) =
    QueryPlan(
      AllNodesScan(idName),
      PlannerQuery(graph = QueryGraph(patternNodes = Set(idName))))

  def planApply(left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      plan = Apply(left.plan, right.plan),
      solved = left.solved ++ right.solved)

  def planTailApply(left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      plan = Apply(left.plan, right.plan),
      solved = left.solved.updateTailOrSelf(_.withTail(right.solved)))

  def planCartesianProduct(left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      CartesianProduct(left.plan, right.plan),
      left.solved ++ right.solved)

  def planDirectedRelationshipByIdSeek(idName: IdName,
                                       relIds: Seq[Expression],
                                       startNode: IdName,
                                       endNode: IdName,
                                       pattern: PatternRelationship,
                                       solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternRel(pattern).
        addPredicates(solvedPredicates: _*)
      )
    )

  def planExpand(left: QueryPlan,
                 from: IdName,
                 dir: Direction,
                 types: Seq[ast.RelTypeName],
                 to: IdName,
                 relName: IdName,
                 length: PatternLength,
                 pattern: PatternRelationship) =
    QueryPlan(
      Expand(left.plan, from, dir, types, to, relName, length),
      left.solved.updateGraph(_.addPatternRel(pattern)))

  def planHiddenSelection(predicates: Seq[Expression], left: QueryPlan) =
    QueryPlan(
      Selection(predicates, left.plan),
      left.solved)

  def planNodeByIdSeek(idName: IdName, nodeIds: Seq[Expression], solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeByIdSeek(idName, nodeIds),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
      )
    )

  def planNodeByLabelScan(idName: IdName, label: Either[String, LabelId], solvedPredicates: Seq[Expression], solvedHint: Option[UsingScanHint] = None) =
    QueryPlan(
      NodeByLabelScan(idName, label),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
      )
    )

  def planNodeIndexSeek(idName: IdName,
                        label: ast.LabelToken,
                        propertyKey: ast.PropertyKeyToken,
                        valueExpr: QueryExpression[Expression], solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None) =
    QueryPlan(
      NodeIndexSeek(idName, label, propertyKey, valueExpr),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
      )
    )

  def planNodeHashJoin(node: IdName, left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      NodeHashJoin(node, left.plan, right.plan),
      left.solved ++ right.solved
    )

  def planNodeIndexUniqueSeek(idName: IdName,
                              label: ast.LabelToken,
                              propertyKey: ast.PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None) =
    QueryPlan(
      NodeIndexUniqueSeek(idName, label, propertyKey, valueExpr),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
      )
    )

  def planOptionalExpand(left: QueryPlan,
                         from: IdName,
                         dir: Direction,
                         types: Seq[ast.RelTypeName],
                         to: IdName,
                         relName: IdName,
                         length: PatternLength,
                         predicates: Seq[Expression],
                         solvedQueryGraph: QueryGraph) =
    QueryPlan(
      OptionalExpand(left.plan, from, dir, types, to, relName, length, predicates),
      left.solved.updateGraph(_.withAddedOptionalMatch(solvedQueryGraph))
    )

  def planOptional(inputPlan: QueryPlan) =
    QueryPlan(
      Optional(inputPlan.plan),
      PlannerQuery(graph = QueryGraph.empty
        .withAddedOptionalMatch(inputPlan.solved.graph)
      )
    )

  def planOuterHashJoin(node: IdName, left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      OuterHashJoin(node, left.plan, right.plan),
      left.solved.updateGraph(_.withAddedOptionalMatch(right.solved.graph))
    )

  def planSelection(predicates: Seq[Expression], left: QueryPlan) =
    QueryPlan(Selection(predicates, left.plan), left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicates: _*))))

  def planSelectOrAntiSemiApply(outer: QueryPlan, inner: QueryPlan, expr: Expression) =
    QueryPlan(
      SelectOrAntiSemiApply(outer.plan, inner.plan, expr),
      outer.solved
    )

  def planLetSelectOrAntiSemiApply(outer: QueryPlan, inner: QueryPlan, id: IdName, expr: Expression) =
    QueryPlan(
      LetSelectOrAntiSemiApply(outer.plan, inner.plan, id, expr),
      outer.solved
    )

  def planSelectOrSemiApply(outer: QueryPlan, inner: QueryPlan, expr: Expression) =
    QueryPlan(
      SelectOrSemiApply(outer.plan, inner.plan, expr),
      outer.solved
    )

  def planLetSelectOrSemiApply(outer: QueryPlan, inner: QueryPlan, id: IdName, expr: Expression) =
    QueryPlan(
      LetSelectOrSemiApply(outer.plan, inner.plan, id, expr),
      outer.solved
    )

  def planLetAntiSemiApply(left: QueryPlan, right: QueryPlan, id: IdName) =
    QueryPlan(
      LetAntiSemiApply(left.plan, right.plan, id),
      left.solved
    )

  def planLetSemiApply(left: QueryPlan, right: QueryPlan, id: IdName) =
    QueryPlan(
      LetSemiApply(left.plan, right.plan, id),
      left.solved
    )

  def planAntiSemiApply(left: QueryPlan, right: QueryPlan, predicate: PatternExpression, solved: Expression) =
    QueryPlan(
      AntiSemiApply(left.plan, right.plan),
      left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(solved)))
    )

  def planSemiApply(left: QueryPlan, right: QueryPlan, predicate: Expression) =
    QueryPlan(
      SemiApply(left.plan, right.plan),
      left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicate)))
    )

  //TODO: Clean up
  def planUndirectedRelationshipByIdSeek(idName: IdName,
                                         relIds: Seq[Expression],
                                         leftNode: IdName,
                                         rightNode: IdName,
                                         pattern: PatternRelationship,
                                         solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternRel(pattern)
      )
    )

  def planQueryArgumentRow(queryGraph: QueryGraph): QueryPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter( rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes -- patternRels.map(_.name)
    planArgumentRow(patternNodes, patternRels, otherIds)
  }

  def planArgumentRow(patternNodes: Set[IdName], patternRels: Set[PatternRelationship] = Set.empty, other: Set[IdName] = Set.empty): QueryPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other
    val typeInfoSeq =
      patternNodes.toSeq.map( (x: IdName) => x.name -> CTNode) ++
      relIds.toSeq.map( (x: IdName) => x.name -> CTRelationship) ++
      other.toSeq.map( (x: IdName) => x.name -> CTAny)
    val typeInfo = typeInfoSeq.toMap

    QueryPlan(
      SingleRow(coveredIds)(typeInfo),
      PlannerQuery(graph =
        QueryGraph(
          argumentIds = coveredIds,
          patternNodes = patternNodes,
          patternRelationships = patternRels
      ))
    )
  }

  def planSingleRow() =
    QueryPlan( SingleRow(Set.empty)(Map.empty), PlannerQuery.empty )

  def planStarProjection(inner: QueryPlan, expressions: Map[String, Expression]) =
    QueryPlan(
      inner.plan,
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions)))
    )

  def planRegularProjection(inner: QueryPlan, expressions: Map[String, Expression]) =
    QueryPlan(
      Projection(inner.plan, expressions),
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions)))
    )

  def planSkip(inner: QueryPlan, count: Expression) =
    QueryPlan(
      SkipPlan(inner.plan, count),
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkip(Some(count)))))
    )

  def planUnwind(inner: QueryPlan, name: IdName, expression: Expression) =
    QueryPlan(
      UnwindPlan(inner.plan, name, expression),
      inner.solved.updateTailOrSelf(_.withHorizon(UnwindProjection(name, expression)))
    )

  def planLimit(inner: QueryPlan, count: Expression) =
    QueryPlan(
      LimitPlan(inner.plan, count),
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimit(Some(count)))))
    )

  def planSort(inner: QueryPlan, descriptions: Seq[SortDescription], items: Seq[ast.SortItem]) =
    QueryPlan(
      Sort(inner.plan, descriptions),
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))))
    )

  def planSortedLimit(inner: QueryPlan, limit: Expression, items: Seq[ast.SortItem]) =
    QueryPlan(
      SortedLimit(inner.plan, limit, items),
      inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(
        _.withLimit(Some(limit))
         .withSortItems(items))))
    )

  def planSortedSkipAndLimit(inner: QueryPlan, skip: Expression, limit: Expression, items: Seq[ast.SortItem]) =
    planSkip(
      QueryPlan(
        SortedLimit(inner.plan, ast.Add(limit, skip)(limit.position), items),
        inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(
          _.withSkip(Some(skip))
           .withLimit(Some(limit))
           .withSortItems(items))))
      ),
      skip
    )

  def planShortestPaths(inner: QueryPlan, shortestPaths: ShortestPathPattern) =
    QueryPlan(
      FindShortestPaths(inner.plan, shortestPaths),
      inner.solved.updateGraph(_.addShortestPath(shortestPaths))
    )
}
