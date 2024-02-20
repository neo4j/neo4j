/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.scalactic.Equality

import scala.collection.mutable

object SymmetricalLogicalPlanEquality extends Equality[LogicalPlan] {

  override def areEqual(plan: LogicalPlan, obj: Any): Boolean = {
    // implementation is mostly taken from [[LogicalPlan.equals()]]
    obj match {
      case otherPlan: LogicalPlan =>
        if (plan.eq(otherPlan)) return true
        if (plan.getClass != otherPlan.getClass) return false
        val stack = new mutable.Stack[(Iterator[Any], Iterator[Any])]()
        var p1 = Iterator.single[Any](plan)
        var p2 = Iterator.single[Any](otherPlan)
        while (p1.hasNext && p2.hasNext) {
          val continue =
            (p1.next(), p2.next()) match {
              case (cp1: CartesianProduct, cp2: CartesianProduct) =>
                val cp1Children = getMultipliedPlans(cp1)
                val cp2Children = getMultipliedPlans(cp2)
                containSameElements(cp1Children, cp2Children)
              case (u1: Union, u2: Union) =>
                val u1Children = getUnionisedPlans(u1)
                val u2Children = getUnionisedPlans(u2)
                containSameElements(u1Children, u2Children)
              case (
                  UnionNodeByLabelsScan(idName1, labels1, argumentIds1, indexOrder1),
                  UnionNodeByLabelsScan(idName2, labels2, argumentIds2, indexOrder2)
                ) =>
                idName1 == idName2 &&
                argumentIds1 == argumentIds2 &&
                indexOrder1 == indexOrder2 &&
                labels1.toSet.equals(labels2.toSet)
              // TODO: UnionRelationshipTypesScan
              case (lp1: LogicalPlan, lp2: LogicalPlan) =>
                if (lp1.getClass != lp2.getClass) {
                  false
                } else {
                  stack.push((p1, p2))
                  p1 = lp1.productIterator
                  p2 = lp2.productIterator
                  true
                }
              case (_: LogicalPlan, _) => false
              case (_, _: LogicalPlan) => false
              case (a1, a2)            => a1 == a2
            }

          if (!continue) return false
          while (!p1.hasNext && !p2.hasNext && stack.nonEmpty) {
            val (p1New, p2New) = stack.pop()
            p1 = p1New
            p2 = p2New
          }
        }
        p1.isEmpty && p2.isEmpty
      case _ => false
    }
  }

  def getMultipliedPlans(plan: LogicalPlan): Seq[LogicalPlan] = {
    plan match {
      case CartesianProduct(left, right) => getMultipliedPlans(left) ++ getMultipliedPlans(right)
      case e                             => Seq(e)
    }
  }

  def getUnionisedPlans(plan: LogicalPlan): Seq[LogicalPlan] = {
    plan match {
      case Union(left, right) => getUnionisedPlans(left) ++ getUnionisedPlans(right)
      case e                  => Seq(e)
    }
  }

  private def containSameElements(seq1: Seq[LogicalPlan], seq2: Seq[LogicalPlan]): Boolean = {
    val group1 = groupByAreEqual(seq1)
    val group2 = groupByAreEqual(seq2)
    group1.size == group2.size &&
    group1.forall { case (plan, count) =>
      group2.exists { case (plan2, count2) =>
        areEqual(plan, plan2) && count == count2
      }
    }
  }

  private def groupByAreEqual(value: Seq[LogicalPlan]): Seq[(LogicalPlan, Integer)] =
    value.foldLeft(Seq.empty[(LogicalPlan, Integer)]) { (acc, plan) =>
      acc.find(tuple => areEqual(tuple._1, plan)) match {
        case Some((otherPlan, count)) => acc.filterNot(_ == (otherPlan, count)) :+ (otherPlan, count + 1)
        case None                     => acc :+ (plan, 1)
      }
    }
}
