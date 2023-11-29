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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonScannable
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsStringRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsValueRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

object DynamicPropertyNotifier {

  def process(
    variables: Set[Variable],
    notification: Set[String] => InternalNotification,
    qg: QueryGraph,
    context: LogicalPlanningContext
  ): Unit = {

    val indexedLabelOrRelTypes = variables.flatMap { variable =>
      val labels = qg.selections.labelsOnNode(variable)
      val relTypes = qg.inlinedRelTypes(variable.name) ++ qg.selections.typesOnRel(variable)

      val indexedLabels = labels.filter(withNodeIndex(_, context))
      val indexedRelTypes = relTypes.filter(withRelIndex(_, context))

      indexedLabels ++ indexedRelTypes
    }

    if (indexedLabelOrRelTypes.nonEmpty) {
      val indexedLabelOrRelTypeNames: Set[String] = indexedLabelOrRelTypes.map(_.name)
      context.staticComponents.notificationLogger.log(notification(indexedLabelOrRelTypeNames))
    }
  }

  private def withNodeIndex(labelName: LabelName, context: LogicalPlanningContext): Boolean = {
    val maybeLabelId = context.semanticTable.id(labelName)
    maybeLabelId.fold(false)(context.staticComponents.planContext.indexExistsForLabel(_))
  }

  private def withRelIndex(relTypeName: RelTypeName, context: LogicalPlanningContext): Boolean = {
    val maybeRelTypeId = context.semanticTable.id(relTypeName)
    maybeRelTypeId.fold(false)(context.staticComponents.planContext.indexExistsForRelType(_))
  }

  def findNonSolvableIdentifiers(
    predicates: Seq[Expression],
    expectedType: EntityType,
    context: LogicalPlanningContext
  ): Set[Variable] = {
    val isExpectedEntity = expectedType match {
      case NODE_TYPE         => (variable: Variable) => context.semanticTable.typeFor(variable).is(CTNode)
      case RELATIONSHIP_TYPE => (variable: Variable) => context.semanticTable.typeFor(variable).is(CTRelationship)
    }

    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case AsDynamicPropertyNonSeekable(nonSeekableId) if isExpectedEntity(nonSeekableId) =>
        Some(nonSeekableId)
      // n['some' + n.prop] STARTS WITH "prefix%..."
      case AsStringRangeNonSeekable(nonSeekableId) if isExpectedEntity(nonSeekableId) =>
        Some(nonSeekableId)
      // n['some' + n.prop] < | <= | > | >= value
      case AsValueRangeNonSeekable(nonSeekableId) if isExpectedEntity(nonSeekableId) =>
        Some(nonSeekableId)

      case AsDynamicPropertyNonScannable(nonScannableId) if isExpectedEntity(nonScannableId) =>
        Some(nonScannableId)

      case _ =>
        None
    }.toSet
  }

  def issueNotifications(
    result: Set[LogicalPlan],
    notification: Set[String] => InternalNotification,
    qg: QueryGraph,
    expectedType: EntityType,
    context: LogicalPlanningContext
  ): Unit = {
    if (result.isEmpty) {
      val nonSolvable = findNonSolvableIdentifiers(qg.selections.flatPredicates, expectedType, context)
      process(nonSolvable, notification, qg, context)
    }
  }
}
