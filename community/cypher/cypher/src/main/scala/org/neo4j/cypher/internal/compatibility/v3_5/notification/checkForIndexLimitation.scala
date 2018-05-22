/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.notification

import org.neo4j.cypher.internal.planner.v3_5.spi.{IndexLimitation, PlanContext, SlowContains}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.expressions.{LabelToken, PropertyKeyToken}
import org.opencypher.v9_0.util.{ExperimentalFeatureNotification, InternalNotification}

case class checkForIndexLimitation(planContext: PlanContext) extends NotificationChecker {

  def apply(plan: LogicalPlan): Set[InternalNotification] = {

    plan.treeFold[Set[InternalNotification]](Set.empty) {
      case NodeIndexContainsScan(_, label, property, _, _) =>
        acc =>
          val notifications = getLimitations(label, property).collect {
            // FIXME x2
            case SlowContains => ExperimentalFeatureNotification("blah")
              //SuboptimalIndexForWildcardQueryNotification(label.name, Seq(property.name))
          }
          (acc ++ notifications, None)
      case NodeIndexEndsWithScan(_, label, property, _, _) =>
        acc =>
          val notifications = getLimitations(label, property).collect {
            case SlowContains => ExperimentalFeatureNotification("blah")
              //SuboptimalIndexForWildcardQueryNotification(label.name, Seq(property.name))
          }
          (acc ++ notifications, None)
    }
  }

  private def getLimitations(label: LabelToken, property: PropertyKeyToken): Set[IndexLimitation] = {
    planContext.indexGetForLabelAndProperties(label.name, Seq(property.name)).fold(Set.empty[IndexLimitation])(_.limitations)
  }
}
