/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.IndexHintException
import org.neo4j.cypher.internal.compiler.v2_3.ast.{LabelName, Identifier, UsingIndexHint}
import org.neo4j.cypher.internal.compiler.v2_3.notification.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantHandleQueryException, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

object verifyBestPlan extends PlanTransformer[PlannerQuery] {
  def apply(plan: LogicalPlan, expected: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val constructed = plan.solved
    if (expected != constructed) {
      val unfulfillableHints = unfulfillableIndexHints(expected, context.planContext)
      val expectedWithoutHints = expected.withoutHints(unfulfillableHints)
      if (expectedWithoutHints != constructed) {
        if (expected.withoutHints(expected.allHints) != constructed.withoutHints(constructed.allHints)) {
          // unknown planner issue failed to find plan (without regard for differences in hints)
          throw new CantHandleQueryException(s"Expected \n$expected \n\n\nInstead, got: \n$constructed")
        } else {
          // unknown planner issue failed to find plan matching hints (i.e. "implicit hints")
          throw new CantHandleQueryException(s"Expected \n${expected.allHints} \n\n\nInstead, got: \n${constructed.allHints}")
        }
      } else {
        // hints referred to non-existent indexes ("explicit hints")
        if (context.useErrorsOverWarnings) {
          val firstIndexHint = unfulfillableHints.head
          throw new IndexHintException(firstIndexHint.identifier.name, firstIndexHint.label.name, firstIndexHint.property.name, "No such index")
        } else {
          unfulfillableHints.foreach { hint =>
            context.notificationLogger.log(IndexHintUnfulfillableNotification(hint.label.name, hint.property.name))
          }
        }
      }
    }
    plan
  }

  private def unfulfillableIndexHints(query: PlannerQuery, planContext: PlanContext): Set[UsingIndexHint] = {
    query.allHints.flatMap {
      // using index name:label(property)
      case hint@UsingIndexHint(Identifier(name), LabelName(label), Identifier(property))
        if planContext.getIndexRule( label, property ).isDefined ||
           planContext.getUniqueIndexRule( label, property ).isDefined => None
      // no such index exists
      case hint@UsingIndexHint(Identifier(name), LabelName(label), Identifier(property)) => Option(hint)
      // don't care about other hints
      case hint => None
    }
  }
}
