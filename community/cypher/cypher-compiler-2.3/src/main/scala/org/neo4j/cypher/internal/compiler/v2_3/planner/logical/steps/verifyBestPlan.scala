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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantHandleQueryException, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.notification.{IndexHintUnfulfillableNotification, JoinHintUnfulfillableNotification}
import org.neo4j.cypher.internal.frontend.v2_3.{IndexHintException, JoinHintException}

object verifyBestPlan extends PlanTransformer[PlannerQuery] {
  def apply(plan: LogicalPlan, expected: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val constructed = plan.solved
    if (expected != constructed) {
      val unfulfillableIndexHints = findUnfulfillableIndexHints(expected, context.planContext)
      val unfulfillableJoinHints = findUnfulfillableJoinHints(expected, context.planContext)
      val expectedWithoutHints = expected.withoutHints(unfulfillableIndexHints ++ unfulfillableJoinHints)
      if (expectedWithoutHints != constructed) {
        if (expected.withoutHints(expected.allHints) != constructed.withoutHints(constructed.allHints)) {
          // unknown planner issue failed to find plan (without regard for differences in hints)
          throw new CantHandleQueryException(s"Expected \n$expected \n\n\nInstead, got: \n$constructed")
        } else {
          // unknown planner issue failed to find plan matching hints (i.e. "implicit hints")
          throw new CantHandleQueryException(s"Expected \n${expected.allHints} \n\n\nInstead, got: \n${constructed.allHints}")
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
        throw new IndexHintException(firstIndexHint.identifier.name, firstIndexHint.label.name, firstIndexHint.property.name, "No such index")
      } else {
        hints.foreach { hint =>
          context.notificationLogger.log(IndexHintUnfulfillableNotification(hint.label.name, hint.property.name))
        }
      }
    }
  }

  private def processUnfulfilledJoinHints(context: LogicalPlanningContext, hints: Set[UsingJoinHint]) = {
    if (hints.nonEmpty) {
      // we were unable to plan hash join on some requested nodes
      if (context.useErrorsOverWarnings) {
        val firstJoinHint = hints.head
        throw new JoinHintException(firstJoinHint.identifiers.map(_.name).reduceLeft(_ + ", " + _), "Unable to plan hash join")
      } else {
        hints.foreach { hint =>
          context.notificationLogger.log(JoinHintUnfulfillableNotification(hint.identifiers.map(_.name).toSeq))
        }
      }
    }
  }

  private def findUnfulfillableIndexHints(query: PlannerQuery, planContext: PlanContext): Set[UsingIndexHint] = {
    query.allHints.flatMap {
      // using index name:label(property)
      case hint@UsingIndexHint(Identifier(name), LabelName(label), PropertyKeyName(property))
        if planContext.getIndexRule( label, property ).isDefined ||
          planContext.getUniqueIndexRule( label, property ).isDefined => None
      // no such index exists
      case hint@UsingIndexHint(Identifier(name), LabelName(label), PropertyKeyName(property)) => Option(hint)
      // don't care about other hints
      case hint => None
    }
  }

  private def findUnfulfillableJoinHints(query: PlannerQuery, planContext: PlanContext): Set[UsingJoinHint] = {
    query.allHints.collect {
      case hint: UsingJoinHint => hint
    }
  }
}
