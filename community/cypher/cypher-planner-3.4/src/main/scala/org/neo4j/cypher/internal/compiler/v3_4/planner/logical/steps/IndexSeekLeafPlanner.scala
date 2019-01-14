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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, QueryExpression}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LabelToken, PropertyKeyToken}

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: String,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[String],
                              context: LogicalPlanningContext)
                             (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan =
      context.logicalPlanProducer.planNodeIndexSeek(idName, label, propertyKeys, valueExpr, solvedPredicates,
        predicatesForCardinalityEstimation, hint, argumentIds, context)

  protected def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)

  protected def findIndexesFor(label: String, properties: Seq[String], context: LogicalPlanningContext): Option[IndexDescriptor] = {
    if (uniqueIndexDefinedFor(label, properties, context).isDefined) None else anyIndex(label, properties, context)
  }

  private def anyIndex(label: String, properties: Seq[String], context: LogicalPlanningContext) =
    context.planContext.indexGet(label, properties)

  private def uniqueIndexDefinedFor(label: String, properties: Seq[String], context: LogicalPlanningContext) =
    context.planContext.uniqueIndexGet(label, properties)
}
