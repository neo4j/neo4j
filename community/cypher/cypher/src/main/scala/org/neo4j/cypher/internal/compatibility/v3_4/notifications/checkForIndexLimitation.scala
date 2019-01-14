/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.notifications

import org.neo4j.cypher.internal.frontend.v3_4.notification.{InternalNotification, SuboptimalIndexForContainsQueryNotification, SuboptimalIndexForEndsWithQueryNotification}
import org.neo4j.cypher.internal.planner.v3_4.spi.{IndexLimitation, PlanContext, SlowContains}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, LabelId}
import org.neo4j.cypher.internal.v3_4.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_4.logical.plans._

case class checkForIndexLimitation(planContext: PlanContext) extends NotificationChecker {

  def apply(plan: LogicalPlan): Set[InternalNotification] = {
    import org.neo4j.cypher.internal.util.v3_4.Foldable._

    plan.treeFold[Set[InternalNotification]](Set.empty) {
      case NodeIndexContainsScan(_, label, property, _, _) =>
        acc =>
          val notifications = getLimitations(label, property).collect {
            case SlowContains => SuboptimalIndexForContainsQueryNotification(label.name, Seq(property.name))
          }
          (acc ++ notifications, None)
      case NodeIndexEndsWithScan(_, label, property, _, _) =>
        acc =>
          val notifications = getLimitations(label, property).collect {
            case SlowContains => SuboptimalIndexForEndsWithQueryNotification(label.name, Seq(property.name))
          }
          (acc ++ notifications, None)
    }
  }

  private def getLimitations(label: LabelToken, property: PropertyKeyToken): Set[IndexLimitation] = {
    planContext.indexGet(label.name, Seq(property.name)).fold(Set.empty[IndexLimitation])(_.limitations) union
    planContext.uniqueIndexGet(label.name, Seq(property.name)).fold(Set.empty[IndexLimitation])(_.limitations)
  }
}
