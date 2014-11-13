/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalPlan, IdName}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext

case class LogicalPlanningContext(planContext: PlanContext,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver,
                                  cardinalityInput: QueryGraphCardinalityInput) {
  def recurse(plan: LogicalPlan) = {
    val newInput = cardinalityInput.recurse(plan)(metrics.cardinality)
    copy(cardinalityInput = newInput)
  }

  def statistics = planContext.statistics
  def cost = metrics.cost
  def cardinality = metrics.cardinality
}

object NodeIdName {
  def unapply(v: Any)(implicit context: LogicalPlanningContext): Option[IdName] = v match {
    case identifier @ Identifier(name) if context.semanticTable.isNode(identifier) => Some(IdName(identifier.name))
    case _                                                                         => None
  }
}

object RelationshipIdName {
  def unapply(v: Any)(implicit context: LogicalPlanningContext): Option[IdName] = v match {
    case identifier @ Identifier(name) if context.semanticTable.isRelationship(identifier) => Some(IdName(identifier.name))
    case _                                                                                 => None
  }
}
