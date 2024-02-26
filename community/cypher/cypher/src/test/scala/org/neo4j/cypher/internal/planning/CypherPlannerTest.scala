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

import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_3
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ComponentConnectorPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.JoinDisconnectedQueryGraphComponents
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodesAllCardinality
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.NullLog
import org.neo4j.monitoring
import org.neo4j.values.virtual.MapValue
import org.scalatest.Assertion
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor5

import java.time.Clock
import scala.collection.mutable

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
        val qgSolver = CypherPlanner.createQueryGraphSolver(CypherPlannerConfiguration.defaults(), planner, componentConnector, mock[Monitors])

        qgSolver.singleComponentSolver.solverConfig.maxTableSize should equal(expectedMaxTableSize)
        qgSolver.singleComponentSolver.solverConfig.iterationDurationLimit should equal(expectedIterationDurationLimit)
        componentConnectorAssertion(expectedMaxTableSize, expectedIterationDurationLimit)(qgSolver.componentConnector)
    }
  }

  test("CompilationPhases.parsing anonymous names should not clash with CompilationPhases.planPipeLine anonymous names") {
    // AddUniquenessPredicates is part of CompilationPhases.parsing and isolateAggregation is part of CompilationPhases.planPipeLine
    // These two phases both make use of the AnonymousVariableNameGenerator. This test is to show that they use the same AnonymousVariableNameGenerator
    // and no clashing names are generated.

    val stats = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality.EMPTY
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = Cardinality.EMPTY
      override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = Cardinality.EMPTY
      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = Some(Selectivity.ZERO)
      override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] = Some(Selectivity.ZERO)
    }

    val getTx = () => 1L
    val planContext = new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        stats, new MutableGraphStatisticsSnapshot(mutable.Map(NodesAllCardinality -> 1.0))
      )
      override def getPropertiesWithExistenceConstraint: Set[String] = Set.empty
      override def canLookupNodesByLabel: Boolean = true
      override def lastCommittedTxIdProvider: () => Long = getTx
      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = Iterator.empty
    }

    CypherPlanner.customPlanContextCreator = Some((_, _, _) => planContext)

    val planner = CypherPlanner(CypherPlannerConfiguration.defaults(),
      Clock.systemUTC(),
      new monitoring.Monitors(),
      NullLog.getInstance(),
      TestExecutorCaffeineCacheFactory,
      CypherPlannerOption.default,
      getTx,
      compatibilityMode = Compatibility4_3)

    val query =
      """MATCH (n)
        |WITH 1 + count(*) AS result
        |MATCH (a)-[r]-(b)-[q*]-(c)
        |RETURN result
        |""".stripMargin
    val preParserQuery = PreParsedQuery(query, query, QueryOptions.default)

    val tc = mock[TransactionalContext](org.mockito.Mockito.RETURNS_DEEP_STUBS)

    val statement = planner
      .parseAndPlan(preParserQuery, NO_TRACING, tc, MapValue.EMPTY, InterpretedRuntime)
      .logicalPlanState
      .statement

    val withAnons = statement
      .folder.treeFindByClass[With].get
      .folder.findAllByClass[Variable]
      .map(_.name)
      .map(NameDeduplicator.removeGeneratedNamesAndParams)

    val whereAnons = statement
      .folder.treeFindByClass[Where].get
      .folder.findAllByClass[Variable]
      .map(_.name)
      .map(NameDeduplicator.removeGeneratedNamesAndParams)

    withAnons should contain noElementsOf whereAnons
    // To protect from future changes, lets make sure we find anonymous variables in both cases
    withAnons should not be empty
    whereAnons should not be empty
  }
}
