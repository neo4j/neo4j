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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.{AggregationProjection, PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.SortDescription
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Aggregation
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SelectOrSemiApply
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Limit
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OuterHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Sort
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Apply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.AggregationProjection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.ast.Add
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SortedLimit
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Skip
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Optional

object QueryPlanProducer {
  def solvePredicate(plan: QueryPlan, solved: Expression) =
    QueryPlan(
      plan.plan,
      plan.solved.updateGraph(_.addPredicates(solved))
    )

  def planAggregation(left: QueryPlan, grouping: Map[String, Expression], aggregation: Map[String, Expression]) =
    QueryPlan(
      Aggregation(left.plan, grouping, aggregation),
      left.solved.withProjection(
        AggregationProjection(groupingKeys = grouping, aggregationExpressions = aggregation, Seq.empty, None, None)
      )
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
                 types: Seq[RelTypeName],
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

  def planNodeByLabelScan(idName: IdName, label: Either[String, LabelId], solvedPredicates: Seq[Expression]) =
    QueryPlan(
      NodeByLabelScan(idName, label),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
      )
    )

  def planNodeIndexSeek(idName: IdName,
                        label: LabelToken,
                        propertyKey: PropertyKeyToken,
                        valueExpr: Expression, solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeIndexSeek(idName, label, propertyKey, valueExpr),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
      )
    )

  def planNodeHashJoin(node: IdName, left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      NodeHashJoin(node, left.plan, right.plan),
      left.solved ++ right.solved
    )

  def planNodeIndexUniqueSeek(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: Expression,
                              solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeIndexUniqueSeek(idName, label, propertyKey, valueExpr),
      PlannerQuery(graph = QueryGraph.empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
      )
    )

  def planOptionalExpand(left: QueryPlan,
                         from: IdName,
                         dir: Direction,
                         types: Seq[RelTypeName],
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
      left.solved.updateGraph(_.addPredicates(solved))
    )

  def planSemiApply(left: QueryPlan, right: QueryPlan, predicate: Expression) =
    QueryPlan(
      SemiApply(left.plan, right.plan),
      left.solved.updateGraph(_.addPredicates(predicate))
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
      inner.solved.updateTailOrSelf(_.updateProjections(_.withProjections(expressions)))
    )

  def planRegularProjection(inner: QueryPlan, expressions: Map[String, Expression]) =
    QueryPlan(
      Projection(inner.plan, expressions),
      inner.solved.updateTailOrSelf(_.updateProjections(_.withProjections(expressions)))
    )

  def planLimit(inner: QueryPlan, count: Expression) =
    QueryPlan(
      Limit(inner.plan, count),
      inner.solved.updateTailOrSelf(_.updateProjections(_.withLimit(Some(count))))
    )

  def planSkip(inner: QueryPlan, count: Expression) =
    QueryPlan(
      Skip(inner.plan, count),
      inner.solved.updateTailOrSelf(_.updateProjections(_.withSkip(Some(count))))
    )

  def planSort(inner: QueryPlan, descriptions: Seq[SortDescription], items: Seq[SortItem]) =
    QueryPlan(
      Sort(inner.plan, descriptions),
      inner.solved.updateTailOrSelf(_.updateProjections(_.withSortItems(items)))
    )

  def planSortedLimit(inner: QueryPlan, limit: Expression, items: Seq[SortItem]) =
    QueryPlan(
      SortedLimit(inner.plan, limit, items),
      inner.solved.updateTailOrSelf(_.updateProjections(_.withSortItems(items).withLimit(Some(limit))))
    )

  def planSortedSkipAndLimit(inner: QueryPlan, skip: Expression, limit: Expression, items: Seq[SortItem]) =
    planSkip(
      QueryPlan(
        SortedLimit(inner.plan, Add(limit, skip)(limit.position), items),
        inner.solved.updateTailOrSelf(
          _.updateProjections(
            _.withSortItems(items)
             .withLimit(Some(limit))
          ))
      ),
      skip
    )
}
