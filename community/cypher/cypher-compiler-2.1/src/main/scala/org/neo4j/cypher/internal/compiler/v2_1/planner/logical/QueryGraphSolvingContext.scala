/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.{PlannerQuery, QueryGraph, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{PatternExpression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName


case class LogicalPlanningContext(planContext: PlanContext,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver) {

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

