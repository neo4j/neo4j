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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.csv.reader.Configuration.DEFAULT_LEGACY_STYLE_QUOTING
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.attribution.IdGen

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
                                  csvBufferSize: Int = 2 * 1024 * 1024,
                                  config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                                  leafPlanUpdater: LeafPlanUpdater = EmptyUpdater,
                                  costComparisonListener: CostComparisonListener,
                                  planningAttributes: PlanningAttributes,
                                  innerVariableNamer: InnerVariableNamer,
                                  /*
                                   * A set of all properties over which aggregation is performed,
                                   * where we potentially could use a NodeIndexScan.
                                   * E.g. WITH n.prop1 AS prop RETURN min(prop), count(m.prop2) => Set(("n", "prop1"), ("m", "prop2"))
                                   */
                                  aggregatingProperties: Set[(String, String)] = Set.empty,
                                  idGen: IdGen,
                                  executionModel: ExecutionModel) {
  def withLimitSelectivity(s: Selectivity): LogicalPlanningContext =
    copy(input = input.withLimitSelectivity(s))

  def withAggregationProperties(properties: Set[(String, String)]): LogicalPlanningContext =
    copy(aggregatingProperties = properties)

  def withUpdatedLabelInfo(plan: LogicalPlan): LogicalPlanningContext =
    copy(input = input.withUpdatedLabelInfo(plan, planningAttributes.solveds))

  def withAddedLeafPlanUpdater(newUpdater: LeafPlanUpdater): LogicalPlanningContext = {
    copy(leafPlanUpdater = ChainedUpdater(leafPlanUpdater, newUpdater))
  }

  def withLeafPlanUpdater(newUpdater: LeafPlanUpdater): LogicalPlanningContext = {
    copy(leafPlanUpdater = newUpdater)
  }

  def withConfig(newConfig: QueryPlannerConfiguration): LogicalPlanningContext = {
    copy(config = newConfig)
  }

  def statistics: GraphStatistics = planContext.statistics

  def cost: CostModel = metrics.cost

  def cardinality: CardinalityModel = metrics.cardinality
}

object NodeIdName {
  def unapply(v: Any, context: LogicalPlanningContext): Option[String] = v match {
    case variable@Variable(_) if context.semanticTable.isNode(variable) => Some(variable.name)
    case _ => None
  }
}

object RelationshipIdName {
  def unapply(v: Any, context: LogicalPlanningContext): Option[String] = v match {
    case variable@Variable(_) if context.semanticTable.isRelationship(variable) => Some(variable.name)
    case _ => None
  }
}
