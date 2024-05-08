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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost

case class RealLogicalPlanningConfiguration(cypherCompilerConfig: CypherPlannerConfiguration)
    extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  override def cardinalityModel(
    queryGraphCardinalityModel: QueryGraphCardinalityModel,
    selectivityCalculator: SelectivityCalculator,
    evaluator: ExpressionEvaluator
  ): CardinalityModel = {
    new StatisticsBackedCardinalityModel(queryGraphCardinalityModel, selectivityCalculator, evaluator)
  }

  // noinspection ScalaUnnecessaryParentheses
  override def costModel(executionModel: ExecutionModel): PartialFunction[
    (
      LogicalPlan,
      QueryGraphSolverInput,
      SemanticTable,
      Cardinalities,
      ProvidedOrders,
      Set[PropertyAccess],
      GraphStatistics,
      CostModelMonitor
    ),
    Cost
  ] = {
    case (plan, input, semanticTable, cardinalities, providedOrders, propertyAccess, statistics, monitor) =>
      CardinalityCostModel(executionModel, CancellationChecker.neverCancelled()).costFor(
        plan,
        input,
        semanticTable,
        cardinalities,
        providedOrders,
        propertyAccess,
        statistics,
        monitor
      )
  }

  override def graphStatistics: GraphStatistics = HardcodedGraphStatistics
  override def indexes: Map[IndexDef, IndexAttributes] = Map.empty
  override def nodeConstraints: Set[(String, Set[String])] = Set.empty
  override def relationshipConstraints: Set[(String, Set[String])] = Set.empty
  override def procedureSignatures: Set[ProcedureSignature] = Set.empty
  override def labelCardinality: Map[String, Cardinality] = Map.empty
  override def knownLabels: Set[String] = Set.empty
  override def knownRelationships: Set[String] = Set.empty
  override def labelsById: Map[Int, String] = Map.empty
  override def relTypesById: Map[Int, String] = Map.empty
  override def executionModel: ExecutionModel = ExecutionModel.default
  override def lookupRelationshipsByType: LookupRelationshipsByType = LookupRelationshipsByType.default

  override def qg: QueryGraph = ???
}
