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

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Identifier, LabelName}
import org.neo4j.cypher.internal.frontend.v2_3.notification.InternalNotification

object DynamicPropertyNotifier {

  def process(identifiers: Set[Identifier], notification: Set[String] => InternalNotification, qg: QueryGraph)
             (implicit context: LogicalPlanningContext) = {

    val indexedLabels = identifiers.flatMap { identifier =>
      val labels = qg.selections.labelsOnNode(IdName(identifier.name))
      labels.filter(withIndex)
    }

    if (indexedLabels.nonEmpty) {
      val indexedLabelNames = indexedLabels.map(_.name)
      context.notificationLogger.log(notification(indexedLabelNames))
    }
  }

  private def withIndex(labelName: LabelName)(implicit context: LogicalPlanningContext) =
    context.planContext.hasIndexRule(labelName.name)
}
