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
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
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

object SingleGrouping {

  def unapply(grouping: Map[String, Expression]): Option[String] = {
    if (grouping.size == 1) {
      grouping.head match {
        case (n, Variable(name)) if n == name => Some(n)
        case _                                => None
      }
    } else {
      None
    }
  }
}
