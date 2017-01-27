/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.InternalNotificationLogger
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{IdName, LogicalPlan, StrictnessMode}
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_1.ast.Variable

case class LogicalPlanningContext(planContext: PlanContext,
                                  logicalPlanProducer: LogicalPlanProducer,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver,
                                  input: QueryGraphSolverInput = QueryGraphSolverInput.empty,
                                  notificationLogger: InternalNotificationLogger,
                                  useErrorsOverWarnings: Boolean = false,
                                  errorIfShortestPathFallbackUsedAtRuntime: Boolean = false,
                                  config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                                  leafPlanUpdater: LogicalPlan => LogicalPlan = identity) {
  def withStrictness(strictness: StrictnessMode) = copy(input = input.withPreferredStrictness(strictness))

  def recurse(plan: LogicalPlan) = copy(input = input.recurse(plan))

  def forExpressionPlanning(nodes: Iterable[Variable], rels: Iterable[Variable]): LogicalPlanningContext = {
    val tableWithNodes = nodes.foldLeft(semanticTable) { case (table, node) => table.addNode(node) }
    val tableWithRels = rels.foldLeft(tableWithNodes) { case (table, rel) => table.addRelationship(rel) }
    copy(
      input = input.copy(inboundCardinality = input.inboundCardinality.max(Cardinality(1))),
      semanticTable = tableWithRels
    )
  }

  def statistics = planContext.statistics

  def cost = metrics.cost

  def cardinality = metrics.cardinality
}

object NodeIdName {
  def unapply(v: Any)(implicit context: LogicalPlanningContext): Option[IdName] = v match {
    case variable@Variable(name) if context.semanticTable.isNode(variable) => Some(IdName(variable.name))
    case _ => None
  }
}

object RelationshipIdName {
  def unapply(v: Any)(implicit context: LogicalPlanningContext): Option[IdName] = v match {
    case variable@Variable(name) if context.semanticTable.isRelationship(variable) => Some(IdName(variable.name))
    case _ => None
  }
}
