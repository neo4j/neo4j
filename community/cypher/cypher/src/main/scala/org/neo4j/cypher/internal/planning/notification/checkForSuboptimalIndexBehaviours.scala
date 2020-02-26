/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.planning.notification

import org.neo4j.cypher.internal.compiler.SuboptimalIndexForConstainsQueryNotification
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForEndsWithQueryNotification
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.planner.spi.IndexBehaviour
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.SlowContains
import org.neo4j.cypher.internal.util.InternalNotification

case class checkForSuboptimalIndexBehaviours(planContext: PlanContext) extends NotificationChecker {

  def apply(plan: LogicalPlan): Set[InternalNotification] = {

    plan.treeFold[Set[InternalNotification]](Set.empty) {
      case NodeIndexContainsScan(_, label, property, _, _, _) =>
        acc =>
          val notifications = getBehaviours(label, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForConstainsQueryNotification(label.name, Seq(property.propertyKeyToken.name))
          }
          (acc ++ notifications, None)
      case NodeIndexEndsWithScan(_, label, property, _, _, _) =>
        acc =>
          val notifications = getBehaviours(label, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForEndsWithQueryNotification(label.name, Seq(property.propertyKeyToken.name))
          }
          (acc ++ notifications, None)
    }
  }

  private def getBehaviours(label: LabelToken, property: PropertyKeyToken): Set[IndexBehaviour] = {
    planContext.indexGetForLabelAndProperties(label.name, Seq(property.name)).fold(Set.empty[IndexBehaviour])(_.behaviours)
  }
}
