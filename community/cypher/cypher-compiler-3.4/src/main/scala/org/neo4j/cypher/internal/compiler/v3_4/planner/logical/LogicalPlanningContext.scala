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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.csv.reader.Configuration.DEFAULT_LEGACY_STYLE_QUOTING
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.{Cardinality, IdName, StrictnessMode}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_4.expressions.Variable

case class LogicalPlanningContext(planContext: PlanContext,
                                  logicalPlanProducer: LogicalPlanProducer,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver,
                                  input: QueryGraphSolverInput = QueryGraphSolverInput.empty,
                                  notificationLogger: InternalNotificationLogger,
                                  useErrorsOverWarnings: Boolean = false,
                                  errorIfShortestPathFallbackUsedAtRuntime: Boolean = false,
                                  errorIfShortestPathHasCommonNodesAtRuntime: Boolean = true,
                                  legacyCsvQuoteEscaping: Boolean = DEFAULT_LEGACY_STYLE_QUOTING,
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
