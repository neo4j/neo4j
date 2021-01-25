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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.cypherCompilerConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ComponentConnectorPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.JoinDisconnectedQueryGraphComponents
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableFor5

class CypherPlannerTest extends CypherFunSuite {
  /**
   * This test is here to remind us that the customPlanContextCreator can be changed for
   * debugging purposes, but that change should never be committed.
   */
  test("customPlanContextCreator should be None") {
    CypherPlanner.customPlanContextCreator should be (None)
  }

  private def shouldBeIDPComponentConnectorPlanner(expectedMaxTableSize: Int, expectedIterationDurationLimit: Long)(componentConnector: JoinDisconnectedQueryGraphComponents): Assertion =
    componentConnector match {
      case ccp:ComponentConnectorPlanner =>
        ccp.config.maxTableSize should equal(expectedMaxTableSize)
        ccp.config.iterationDurationLimit should equal(expectedIterationDurationLimit)
      case x =>
        fail(s"Expected a ComponentConnectorPlanner but got: $x")
    }

  private def shouldBeGreedyComponentConnectorPlanner(expectedMaxTableSize: Int, expectedIterationDurationLimit: Long)(componentConnector: JoinDisconnectedQueryGraphComponents): Assertion =
    componentConnector should be(cartesianProductsOrValueJoins)

  private val configOptions: TableFor5[CypherPlannerOption, CypherConnectComponentsPlannerOption, Int, Long, (Int, Long) => JoinDisconnectedQueryGraphComponents => Assertion] = Table(
    ("planner",                   "componentConnector",                         "expectedMaxTableSize",              "expectedIterationDurationLimit",              "componentConnectorAssertion"),
    (CypherPlannerOption.default, CypherConnectComponentsPlannerOption.default, DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.idp,     CypherConnectComponentsPlannerOption.default, DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.cost,    CypherConnectComponentsPlannerOption.default, DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.dp,      CypherConnectComponentsPlannerOption.default, DPSolverConfig.maxTableSize,         DPSolverConfig.iterationDurationLimit,         shouldBeIDPComponentConnectorPlanner),

    (CypherPlannerOption.default, CypherConnectComponentsPlannerOption.idp,     DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.idp,     CypherConnectComponentsPlannerOption.idp,     DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.cost,    CypherConnectComponentsPlannerOption.idp,     DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeIDPComponentConnectorPlanner),
    (CypherPlannerOption.dp,      CypherConnectComponentsPlannerOption.idp,     DPSolverConfig.maxTableSize,         DPSolverConfig.iterationDurationLimit,         shouldBeIDPComponentConnectorPlanner),

    (CypherPlannerOption.default, CypherConnectComponentsPlannerOption.greedy,  DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeGreedyComponentConnectorPlanner),
    (CypherPlannerOption.idp,     CypherConnectComponentsPlannerOption.greedy,  DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeGreedyComponentConnectorPlanner),
    (CypherPlannerOption.cost,    CypherConnectComponentsPlannerOption.greedy,  DefaultIDPSolverConfig.maxTableSize, DefaultIDPSolverConfig.iterationDurationLimit, shouldBeGreedyComponentConnectorPlanner),
    (CypherPlannerOption.dp,      CypherConnectComponentsPlannerOption.greedy,  DPSolverConfig.maxTableSize,         DPSolverConfig.iterationDurationLimit,         shouldBeGreedyComponentConnectorPlanner),
  )

  test("should use correct solvers") {
    forAll(configOptions) {
      (planner: CypherPlannerOption, componentConnector: CypherConnectComponentsPlannerOption, expectedMaxTableSize: Int, expectedIterationDurationLimit: Long, componentConnectorAssertion: (Int, Long) => JoinDisconnectedQueryGraphComponents => Assertion) =>
        val qgSolver = CypherPlanner.createQueryGraphSolver(cypherCompilerConfig, planner, componentConnector, mock[Monitors])

        qgSolver.singleComponentSolver.solverConfig.maxTableSize should equal(expectedMaxTableSize)
        qgSolver.singleComponentSolver.solverConfig.iterationDurationLimit should equal(expectedIterationDurationLimit)
        componentConnectorAssertion(expectedMaxTableSize, expectedIterationDurationLimit)(qgSolver.componentConnector)
    }
  }
}
