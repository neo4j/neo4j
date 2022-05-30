/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

case class unionScanRewriter(solveds: Solveds, attributes: Attributes[LogicalPlan]) extends Rewriter {

  override def apply(input: AnyRef): AnyRef = {
    instance.apply(input)
  }

  private val instance: Rewriter = topDown(Rewriter.lift {
    case outer @ OrderedDistinct(
        CollectUnionLabels(idName, labels, arguments, indexOrder),
        groupingExpressions,
        Seq(Variable(orderName))
      ) if idName == orderName =>
      groupingExpressions.get(idName) match {
        case Some(Variable(name)) if name == idName =>
          val unionLabel =
            UnionNodeByLabelsScan(idName, labels, argumentIds = arguments, indexOrder)(SameId(outer.id))
          if (groupingExpressions.size == 1) {
            unionLabel
          } else {
            val res = outer.copy(source = unionLabel)(
              attributes.copy(outer.id)
            )
            solveds.copy(outer.id, res.id)
            res
          }

        case None => outer
      }

    case outer @ OrderedDistinct(
        CollectUnionTypes(directed, idName, start, end, types, arguments, indexOrder),
        groupingExpressions,
        Seq(Variable(orderName))
      ) if idName == orderName =>
      (groupingExpressions.get(idName), groupingExpressions.get(start), groupingExpressions.get(end)) match {
        case (Some(Variable(relVar)), Some(Variable(startVar)), Some(Variable(endVar)))
          if relVar == idName && startVar == start && endVar == end =>
          val unionScan =
            if (directed) {
              DirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds = arguments, indexOrder)(
                SameId(outer.id)
              )
            } else {
              UndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds = arguments, indexOrder)(
                SameId(outer.id)
              )
            }
          if (groupingExpressions.size == 3) {
            unionScan
          } else {
            val res = outer.copy(source = unionScan)(
              attributes.copy(outer.id)
            )
            solveds.copy(outer.id, res.id)
            res
          }

        case _ => outer
      }

  })
}

object CollectUnionLabels {

  def unapply(plan: LogicalPlan): Option[(String, Seq[LabelName], Set[String], IndexOrder)] = {
    var planToTest = plan
    var labels: List[LabelName] = Nil
    var foundState: Option[(String, ColumnOrder, IndexOrder, Set[String])] = None

    // I don't think we will ever plan inconsistent plans with varying name, index order etc as it
    // would probably indicate a planner bug, but if we encounter an inconsistency let's not rewrite
    // and make matters worse.
    def checkConsistency(n: String, co: ColumnOrder, io: IndexOrder, as: Set[String]): Boolean = {
      foundState match {
        case Some((idName, columnOrder, indexOrder, arguments)) =>
          n == idName && co == columnOrder && io == indexOrder && as == arguments
        case None =>
          io match {
            case IndexOrderAscending if !co.isAscending => false
            case IndexOrderDescending if co.isAscending => false
            case _ if n != co.id                        => false
            case _ =>
              foundState = Some((n, co, io, as))
              true
          }
      }
    }

    while (true) {
      planToTest match {
        case OrderedUnion(NodeByLabelScan(n1, l1, a1, o1), NodeByLabelScan(n2, l2, a2, o2), Seq(colOrder))
          if n1 == n2 && o1 == o2 && a1 == a2 && checkConsistency(n1, colOrder, o1, a1) =>
          labels = l1 :: l2 :: labels
          return Some((n1, labels, a1, o1))
        case OrderedUnion(inner: OrderedUnion, NodeByLabelScan(n, l, a, o), Seq(colOrder))
          if checkConsistency(n, colOrder, o, a) =>
          planToTest = inner
          labels = l :: labels
        case _ =>
          return None
      }
    }

    // will not be reached
    throw new IllegalStateException("Reached impossible condition")
  }
}

object CollectUnionTypes {

  def unapply(plan: LogicalPlan)
    : Option[(Boolean, String, String, String, Seq[RelTypeName], Set[String], IndexOrder)] = {
    var planToTest = plan
    var types: List[RelTypeName] = Nil
    var foundState: Option[(String, String, String, ColumnOrder, IndexOrder, Set[String], Boolean)] = None

    // I don't think we will ever plan inconsistent plans with varying name, index order etc as it
    // would probably indicate a planner bug, but if we encounter an inconsistency let's not rewrite
    // and make matters worse.
    def checkConsistency(
      r: String,
      s: String,
      e: String,
      co: ColumnOrder,
      io: IndexOrder,
      as: Set[String],
      directed: Boolean
    ): Boolean = {
      foundState match {
        case Some((idName, start, end, columnOrder, indexOrder, arguments, isDirected)) =>
          r == idName && s == start && e == end && co == columnOrder && io == indexOrder && as == arguments && directed == isDirected
        case None =>
          io match {
            case IndexOrderAscending if !co.isAscending => false
            case IndexOrderDescending if co.isAscending => false
            case _ if r != co.id                        => false
            case _ =>
              foundState = Some((r, s, e, co, io, as, directed))
              true
          }
      }
    }

    while (true) {
      planToTest match {
        case OrderedUnion(
            DirectedRelationshipTypeScan(r1, s1, t1, e1, a1, o1),
            DirectedRelationshipTypeScan(r2, s2, t2, e2, a2, o2),
            Seq(colOrder)
          )
          if r1 == r2 && s1 == s2 && e1 == e2 && o1 == o2 && a1 == a2 && checkConsistency(
            r1,
            s1,
            e1,
            colOrder,
            o1,
            a1,
            directed = true
          ) =>
          types = t1 :: t2 :: types
          return Some((true, r1, s1, e1, types, a1, o1))
        case OrderedUnion(
            UndirectedRelationshipTypeScan(r1, s1, t1, e1, a1, o1),
            UndirectedRelationshipTypeScan(r2, s2, t2, e2, a2, o2),
            Seq(colOrder)
          )
          if r1 == r2 && s1 == s2 && e1 == e2 && o1 == o2 && a1 == a2 && checkConsistency(
            r1,
            s1,
            e1,
            colOrder,
            o1,
            a1,
            directed = false
          ) =>
          types = t1 :: t2 :: types
          return Some((false, r1, s1, e1, types, a1, o1))
        case OrderedUnion(inner: OrderedUnion, DirectedRelationshipTypeScan(r, s, t, e, a, o), Seq(colOrder))
          if checkConsistency(r, s, e, colOrder, o, a, directed = true) =>
          planToTest = inner
          types = t :: types
        case OrderedUnion(inner: OrderedUnion, UndirectedRelationshipTypeScan(r, s, t, e, a, o), Seq(colOrder))
          if checkConsistency(r, s, e, colOrder, o, a, directed = false) =>
          planToTest = inner
          types = t :: types
        case _ =>
          return None
      }
    }

    // will not be reached
    throw new IllegalStateException("Reached impossible condition")
  }
}
