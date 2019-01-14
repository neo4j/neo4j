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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CardinalityCostModel, ExpressionEvaluator, Metrics, StatisticsBackedCardinalityModel}
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, IndexOrderCapability}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, ProcedureSignature}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, Cost}

case class RealLogicalPlanningConfiguration(cypherCompilerConfig: CypherPlannerConfiguration)
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator): CardinalityModel = {
    new StatisticsBackedCardinalityModel(queryGraphCardinalityModel, evaluator)
  }

  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = {
    val model: Metrics.CostModel = CardinalityCostModel(cypherCompilerConfig)
    ({
      case (plan: LogicalPlan, input: QueryGraphSolverInput, cardinalities: Cardinalities) => model(plan, input, cardinalities)
    })
  }

  override def graphStatistics: GraphStatistics = HardcodedGraphStatistics
  override def indexes: Set[(String, Seq[String])] = Set.empty
  override def uniqueIndexes: Set[(String, Seq[String])] = Set.empty
  override def indexesWithValues: Set[(String, Seq[String])] = Set.empty
  override def indexesWithOrdering: Map[(String, Seq[String]), IndexOrderCapability] = Map.empty
  override def procedureSignatures: Set[ProcedureSignature] = Set.empty
  override def labelCardinality: Map[String, Cardinality] = Map.empty
  override def knownLabels: Set[String] = Set.empty
  override def labelsById: Map[Int, String] = Map.empty

  override def qg: QueryGraph = ???
}
