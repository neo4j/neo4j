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
package org.neo4j.cypher.internal.planning.notification

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForConstainsQueryNotification
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForEndsWithQueryNotification
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.planner.spi.IndexBehaviour
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.SlowContains
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.graphdb

case class checkForSuboptimalIndexBehaviours(planContext: PlanContext) extends NotificationChecker {

  def apply(plan: LogicalPlan): Set[InternalNotification] = {

    plan.treeFold[Set[InternalNotification]](Set.empty) {
      case NodeIndexContainsScan(varName, label, property, _, _, _, indexType) =>
        acc =>
          val notifications = getNodeIndexBehaviours(indexType, label, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForConstainsQueryNotification(varName, label.name, Seq(property.propertyKeyToken.name), EntityType.NODE)
          }
          SkipChildren(acc ++ notifications)
      case NodeIndexEndsWithScan(varName, label, property, _, _, _, indexType) =>
        acc =>
          val notifications = getNodeIndexBehaviours(indexType, label, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForEndsWithQueryNotification(varName, label.name, Seq(property.propertyKeyToken.name), EntityType.NODE)
          }
          SkipChildren(acc ++ notifications)
      case UndirectedRelationshipIndexContainsScan(varName, _, _, relType, property, _, _, _, indexType) =>
        acc =>
          val notifications = getRelationshipIndexBehaviours(indexType, relType, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForConstainsQueryNotification(varName, relType.name, Seq(property.propertyKeyToken.name), EntityType.RELATIONSHIP)
          }
          SkipChildren(acc ++ notifications)
      case UndirectedRelationshipIndexEndsWithScan(varName, _, _, relType, property, _, _, _, indexType) =>
        acc =>
          val notifications = getRelationshipIndexBehaviours(indexType, relType, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForEndsWithQueryNotification(varName, relType.name, Seq(property.propertyKeyToken.name), EntityType.RELATIONSHIP)
          }
          SkipChildren(acc ++ notifications)
      case DirectedRelationshipIndexContainsScan(varName, _, _, relType, property, _, _, _, indexType) =>
        acc =>
          val notifications = getRelationshipIndexBehaviours(indexType, relType, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForConstainsQueryNotification(varName, relType.name, Seq(property.propertyKeyToken.name), EntityType.RELATIONSHIP)
          }
          SkipChildren(acc ++ notifications)
      case DirectedRelationshipIndexEndsWithScan(varName, _, _, relType, property, _, _, _, indexType) =>
        acc =>
          val notifications = getRelationshipIndexBehaviours(indexType, relType, property.propertyKeyToken).collect {
            case SlowContains => SuboptimalIndexForEndsWithQueryNotification(varName, relType.name, Seq(property.propertyKeyToken.name), EntityType.RELATIONSHIP)
          }
          SkipChildren(acc ++ notifications)
    }
  }

  private def getNodeIndexBehaviours(indexType: graphdb.schema.IndexType, label: LabelToken, property: PropertyKeyToken): Set[IndexBehaviour] = {
    val maybeDescriptor = IndexDescriptor.IndexType.fromPublicApi(indexType).flatMap {
      case IndexDescriptor.IndexType.Text => planContext.textIndexGetForLabelAndProperties(label.name, Seq(property.name))
      case IndexDescriptor.IndexType.Range => planContext.rangeIndexGetForLabelAndProperties(label.name, Seq(property.name))
      case IndexDescriptor.IndexType.Point => planContext.pointIndexGetForLabelAndProperties(label.name, Seq(property.name))
    }

    maybeDescriptor.fold(Set.empty[IndexBehaviour])(_.behaviours)
  }

  private def getRelationshipIndexBehaviours(indexType: graphdb.schema.IndexType, relTypeToken: RelationshipTypeToken, property: PropertyKeyToken): Set[IndexBehaviour] = {
    val maybeDescriptor = IndexDescriptor.IndexType.fromPublicApi(indexType).flatMap {
      case IndexDescriptor.IndexType.Range => planContext.rangeIndexGetForRelTypeAndProperties(relTypeToken.name, Seq(property.name))
      case IndexDescriptor.IndexType.Text => planContext.textIndexGetForRelTypeAndProperties(relTypeToken.name, Seq(property.name))
      case IndexDescriptor.IndexType.Point => planContext.pointIndexGetForRelTypeAndProperties(relTypeToken.name, Seq(property.name))
    }

    maybeDescriptor.fold(Set.empty[IndexBehaviour])(_.behaviours)
  }
}
