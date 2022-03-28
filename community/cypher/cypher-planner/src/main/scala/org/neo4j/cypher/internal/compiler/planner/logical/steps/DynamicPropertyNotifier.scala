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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.InternalNotification

object DynamicPropertyNotifier {

  def process(variables: Set[Variable], notification: Set[String] => InternalNotification, qg: QueryGraph, context: LogicalPlanningContext) = {

    val indexedLabelOrRelTypes = variables.flatMap { variable =>
      val labels = qg.selections.labelsOnNode(variable.name)
      val relTypes = qg.inlinedRelTypes(variable.name) ++ qg.selections.typesOnRel(variable.name)

      val indexedLabels = labels.filter(withNodeIndex(_, context))
      val indexedRelTypes = relTypes.filter(withRelIndex(_, context))

      indexedLabels ++ indexedRelTypes
    }

    if (indexedLabelOrRelTypes.nonEmpty) {
      val indexedLabelOrRelTypeNames: Set[String] = indexedLabelOrRelTypes.map(_.name)
      context.notificationLogger.log(notification(indexedLabelOrRelTypeNames))
    }
  }

  private def withNodeIndex(labelName: LabelName, context: LogicalPlanningContext): Boolean = {
    val maybeLabelId = context.semanticTable.id(labelName)
    maybeLabelId.fold(false)(context.planContext.indexExistsForLabel(_))
  }

  private def withRelIndex(relTypeName: RelTypeName, context: LogicalPlanningContext): Boolean = {
    val maybeRelTypeId = context.semanticTable.id(relTypeName)
    maybeRelTypeId.fold(false)(context.planContext.indexExistsForRelType(_))
  }
}
