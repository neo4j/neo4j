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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, FunctionInvocation}
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, topDown}
import org.neo4j.cypher.internal.v3_3.logical.plans._

import scala.collection.mutable

case object pruningVarExpander extends Rewriter {

  private def findDistinctSet(plan: LogicalPlan): Set[LogicalPlan] = {
    val distinctSet = mutable.Set[VarExpand]()

    def collectDistinctSet(plan: LogicalPlan,
                           dependencies: Option[Set[String]]): Option[Set[String]] = {

      val lowerDistinctLand: Option[Set[String]] = plan match {
        case Distinct(_, groupExpr) =>
          Some(groupExpr.values.flatMap(_.dependencies.map(_.name)).toSet)

        case Aggregation(_, groupExpr, aggrExpr) if aggrExpr.values.forall(isDistinct) =>
          val variablesInTheDistinctSet = (groupExpr.values.flatMap(_.dependencies.map(_.name)) ++
            aggrExpr.values.flatMap(_.dependencies.map(_.name))).toSet
          Some(variablesInTheDistinctSet)

        case expand: VarExpand
          if dependencies.nonEmpty && !distinctNeedsRelsFromExpand(dependencies, expand) && expand.length.max.nonEmpty =>
          distinctSet += expand
          dependencies

        case Projection(_, expressions) =>
          dependencies.map(_ ++ expressions.values.flatMap(_.dependencies.map(_.name)))

        case Selection(predicates, _) =>
          dependencies.map(_ ++ predicates.flatMap(_.dependencies.map(_.name)))

        case _: Expand |
             _: VarExpand |
             _: Apply |
             _: Optional =>
          dependencies

        case _ =>
          None
      }

      lowerDistinctLand
    }

    var planStack: List[(LogicalPlan, Option[Set[String]])] = Nil
    def push(item: (LogicalPlan, Option[Set[String]])) {
      planStack = item :: planStack
    }
    def pop(): (LogicalPlan, Option[Set[String]]) = {
      val item = planStack.head
      planStack = planStack.tail
      item
    }
    push((plan, None))

    while(planStack.nonEmpty) {
      val (plan: LogicalPlan, deps: Option[Set[String]]) = pop()
      val newDeps = collectDistinctSet(plan, deps)

      plan.lhs.foreach(p => push((p, newDeps)))
      plan.rhs.foreach(p => push((p, newDeps)))
    }

    distinctSet.toSet
  }

  // When the distinct horizon needs the path that includes the var length relationship,
  // we can't use DistinctVarExpand - we need all the paths
  def distinctNeedsRelsFromExpand(inDistinctLand: Option[Set[String]], expand: VarExpand): Boolean = {
    inDistinctLand.forall(vars => vars(expand.relName.name))
  }

  private def isDistinct(e: Expression) = e match {
    case f: FunctionInvocation => f.distinct
    case _ => false
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val distinctSet = findDistinctSet(plan)

        val innerRewriter = topDown(Rewriter.lift {
          case expand@VarExpand(lhs, fromId, dir, _, relTypes, toId, _, length, ExpandAll, _, _, _, _, predicates) if distinctSet(expand) =>
            if (length.min >= 4 && length.max.get >= 5)
              // These constants were selected by benchmarking on randomized graphs, with different
              // degrees of interconnection.
              FullPruningVarExpand(lhs, fromId, dir, relTypes, toId, length.min, length.max.get, predicates)(expand.solved)
            else if (length.max.get > 1)
              PruningVarExpand(lhs, fromId, dir, relTypes, toId, length.min, length.max.get, predicates)(expand.solved)
            else expand
        })
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}
