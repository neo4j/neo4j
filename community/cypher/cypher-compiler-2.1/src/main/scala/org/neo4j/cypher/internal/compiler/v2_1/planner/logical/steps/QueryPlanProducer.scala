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
import org.neo4j.cypher.internal.compiler.v2_1
import v2_1.planner.logical.plans._
import v2_1.planner.QueryGraph
import v2_1.ast._
import v2_1.{PropertyKeyId, LabelId}

object QueryPlanProducer {
  def planAllNodesScan(idName: IdName) =
    QueryPlan(
      AllNodesScan(idName),
      QueryGraph(patternNodes = Set(idName)))

  def planAntiSemiApply(left: QueryPlan, right: QueryPlan, predicate: PatternExpression, solved: Expression) =
    QueryPlan(
      AntiSemiApply(left.plan, right.plan),
      left.solved.addPredicates(solved))

  def planApply(left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      plan = Apply(left.plan, right.plan),
      solved = left.solved ++ right.solved)

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
      QueryGraph
        .empty
        .addPatternRel(pattern).addPredicates(solvedPredicates: _*)
    )

  def planExpand(left: QueryPlan,
                 from: IdName,
                 dir: Direction,
                 types: Seq[RelTypeName],
                 to: IdName,
                 relName: IdName,
                 length: PatternLength,
                 pattern: PatternRelationship) =
    QueryPlan(Expand(left.plan, from, dir, types, to, relName, length), left.solved.addPatternRel(pattern))

  def planHiddenSelection(predicates: Seq[Expression], left: QueryPlan) =
    QueryPlan(
      Selection(predicates, left.plan),
      left.solved)

  def planNodeByIdSeek(idName: IdName, nodeIds: Seq[Expression], solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeByIdSeek(idName, nodeIds),
      QueryGraph
        .empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
    )

  def planNodeByLabelScan(idName: IdName, label: Either[String, LabelId], solvedPredicates: Seq[Expression]) =
    QueryPlan(
      NodeByLabelScan(idName, label),
      QueryGraph
        .empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
    )

  def planNodeIndexSeek(idName: IdName, label: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression, solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeIndexSeek(idName, label, propertyKeyId, valueExpr),
      QueryGraph
        .empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
    )

  def planNodeHashJoin(node: IdName, left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      NodeHashJoin(node, left.plan, right.plan),
      left.solved ++ right.solved
    )

  def planNodeIndexUniqueSeek(idName: IdName, label: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression, solvedPredicates: Seq[Expression] = Seq.empty) =
    QueryPlan(
      NodeIndexUniqueSeek(idName, label, propertyKeyId, valueExpr),
      QueryGraph
        .empty
        .addPatternNodes(idName)
        .addPredicates(solvedPredicates: _*)
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
      left.solved.withAddedOptionalMatch(solvedQueryGraph)
    )

  def planOptional(inputPlan: QueryPlan) =
    QueryPlan(
      Optional(inputPlan.plan),
      QueryGraph().
        withAddedOptionalMatch(inputPlan.solved).
        withProjections(inputPlan.solved.projections)
    )

  def planOuterHashJoin(node: IdName, left: QueryPlan, right: QueryPlan) =
    QueryPlan(
      OuterHashJoin(node, left.plan, right.plan),
      left.solved.withAddedOptionalMatch(right.solved)
    )

  def planSelection(predicates: Seq[Expression], left: QueryPlan) =
    QueryPlan(Selection(predicates, left.plan), left.solved.addPredicates(predicates: _*))

  def planSelectOrAntiSemiApply(outer: QueryPlan, inner: QueryPlan, expr: Expression, solved: Expression) =
    QueryPlan(
      SelectOrAntiSemiApply(outer.plan, inner.plan, expr),
      outer.solved.addPredicates(solved))

  def planSelectOrSemiApply(outer: QueryPlan, inner: QueryPlan, expr: Expression, solved: Expression) =
    QueryPlan(
      SelectOrSemiApply(outer.plan, inner.plan, expr),
      outer.solved.addPredicates(solved)
    )

  def planSemiApply(left: QueryPlan, right: QueryPlan, predicate: Expression) =
    QueryPlan(
      SemiApply(left.plan, right.plan),
      left.solved.addPredicates(predicate)
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
      QueryGraph
        .empty
        .addPatternRel(pattern)
    )

  def planSingleRow(coveredIds: Set[IdName]) =
    QueryPlan(
      SingleRow(coveredIds),
      QueryGraph(argumentIds = coveredIds)
    )
}
