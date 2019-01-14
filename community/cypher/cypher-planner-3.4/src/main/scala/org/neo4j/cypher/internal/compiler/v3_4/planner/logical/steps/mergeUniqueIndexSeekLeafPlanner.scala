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

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, QueryExpression}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LabelToken, PropertyKeyToken}

/*
 * Plan the following type of plan
 *
 *  - as := AssertSame
 *  - ui := NodeUniqueIndexSeek)
 *
 *       (as)
 *       /  \
 *    (as) (ui2)
 *    /  \
 * (ui1) (ui2)
 */
object mergeUniqueIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {

  override def apply(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): Seq[LogicalPlan] = {
    val resultPlans: Set[LeafPlansForVariable] = producePlanFor(qg.selections.flatPredicates.toSet, qg, context)
    val grouped: Map[String, Set[LeafPlansForVariable]] = resultPlans.groupBy(_.id)

    grouped.map {
      case (id, plans) =>
        plans.flatMap(_.plans).reduce[LogicalPlan] {
          case (p1, p2) => context.logicalPlanProducer.planAssertSameNode(id, p1, p2, context)
        }
    }.toSeq

  }

  override def constructPlan(idName: String,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[String],
                             context: LogicalPlanningContext)
                            (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan =
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, propertyKeys, valueExpr, solvedPredicates, hint, argumentIds, context)

  override def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.uniqueIndexesGetForLabel(labelId)

  override def findIndexesFor(label: String, properties: Seq[String], context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.uniqueIndexGet(label, properties)
}
