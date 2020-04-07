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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  override protected def constructPlan(idName: String,
                                       label: LabelToken,
                                       properties: Seq[IndexedProperty],
                                       isUnique: Boolean,
                                       valueExpr: QueryExpression[Expression],
                                       hint: Option[UsingIndexHint],
                                       argumentIds: Set[String],
                                       providedOrder: ProvidedOrder,
                                       context: LogicalPlanningContext,
                                       onlyExists: Boolean)
                                      (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan =
    if (onlyExists) {
      context.logicalPlanProducer.planNodeIndexScan(idName, label, properties, solvedPredicates, hint, argumentIds, providedOrder, context)
    } else if (isUnique) {
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName,
                                                          label,
                                                          properties,
                                                          valueExpr,
                                                          solvedPredicates,
                                                          predicatesForCardinalityEstimation,
                                                          hint,
                                                          argumentIds,
                                                          providedOrder,
                                                          context)
    } else {
      context.logicalPlanProducer.planNodeIndexSeek(idName,
                                                    label,
                                                    properties,
                                                    valueExpr,
                                                    solvedPredicates,
                                                    predicatesForCardinalityEstimation,
                                                    hint,
                                                    argumentIds,
                                                    providedOrder,
                                                    context)
    }

  override def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)
}
