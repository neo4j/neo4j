/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.aux.v3_4.{HintException, IndexHintException, InternalException, JoinHintException}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.notification.{IndexHintUnfulfillableNotification, JoinHintUnfulfillableNotification}
import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_4.expressions.LabelName

object verifyBestPlan extends PlanTransformer[PlannerQuery] {
  def apply(plan: LogicalPlan, expected: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val constructed = plan.solved
    if (expected != constructed) {
      val unfulfillableIndexHints = findUnfulfillableIndexHints(expected, context.planContext)
      val unfulfillableJoinHints = findUnfulfillableJoinHints(expected, context.planContext)
      val expectedWithoutHints = expected.withoutHints(unfulfillableIndexHints ++ unfulfillableJoinHints)
      if (expectedWithoutHints != constructed) {
        val a: PlannerQuery = expected.withoutHints(expected.allHints)
        val b: PlannerQuery = constructed.withoutHints(constructed.allHints)
        if (a != b) {
          // unknown planner issue failed to find plan (without regard for differences in hints)
          throw new InternalException(s"Expected \n$expected \n\n\nInstead, got: \n$constructed\nPlan: $plan")
        } else {
          // unknown planner issue failed to find plan matching hints (i.e. "implicit hints")
          val expectedHints = expected.allHints
          val actualHints = constructed.allHints
          val missing = expectedHints -- actualHints

          def out(h: Set[Hint]) = h.mkString("`", ", ", "`")

          val details = if (missing.isEmpty)
            s"""Expected:
               |${out(expectedHints)}
               |
               |Instead, got:
               |${out(actualHints)}""".stripMargin
          else
            s"Could not solve these hints: ${out(missing)}"

          val message =
            s"""Failed to fulfil the hints of the query.
               |$details
               |
               |Plan $plan""".stripMargin

          throw new HintException(message)
        }
      } else {
        processUnfulfilledIndexHints(context, unfulfillableIndexHints)
        processUnfulfilledJoinHints(context, unfulfillableJoinHints)
      }
    }
    plan
  }

  private def processUnfulfilledIndexHints(context: LogicalPlanningContext, hints: Set[UsingIndexHint]) = {
    if (hints.nonEmpty) {
      // hints referred to non-existent indexes ("explicit hints")
      if (context.useErrorsOverWarnings) {
        val firstIndexHint = hints.head
        throw new IndexHintException(firstIndexHint.variable.name, firstIndexHint.label.name, firstIndexHint.properties.map(_.name), "No such index")
      } else {
        hints.foreach { hint =>
          context.notificationLogger.log(IndexHintUnfulfillableNotification(hint.label.name, hint.properties.map(_.name)))
        }
      }
    }
  }

  private def processUnfulfilledJoinHints(context: LogicalPlanningContext, hints: Set[UsingJoinHint]) = {
    if (hints.nonEmpty) {
      // we were unable to plan hash join on some requested nodes
      if (context.useErrorsOverWarnings) {
        val firstJoinHint = hints.head
        throw new JoinHintException(firstJoinHint.variables.map(_.name).reduceLeft(_ + ", " + _), "Unable to plan hash join")
      } else {
        hints.foreach { hint =>
          context.notificationLogger.log(JoinHintUnfulfillableNotification(hint.variables.map(_.name).toIndexedSeq))
        }
      }
    }
  }

  private def findUnfulfillableIndexHints(query: PlannerQuery, planContext: PlanContext): Set[UsingIndexHint] = {
    query.allHints.flatMap {
      // using index name:label(property1,property2)
      case UsingIndexHint(_, LabelName(label), properties)
        if planContext.indexGet(label, properties.map(_.name)).isDefined ||
          planContext.uniqueIndexGet(label, properties.map(_.name)).isDefined => None
      // no such index exists
      case hint: UsingIndexHint => Some(hint)
      // don't care about other hints
      case _ => None
    }
  }

  private def findUnfulfillableJoinHints(query: PlannerQuery, planContext: PlanContext): Set[UsingJoinHint] = {
    query.allHints.collect {
      case hint: UsingJoinHint => hint
    }
  }
}
