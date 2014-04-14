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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.commons.{CypherTestSuite, CypherTestSupport}
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, LogicalPlan, IdName}
import org.neo4j.cypher.internal.compiler.v2_1.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Metrics.{CardinalityModel, SelectivityModel}

trait LogicalPlanningTestSupport extends CypherTestSupport {
  self: CypherTestSuite with MockitoSugar =>

  val kernelMonitors = new org.neo4j.kernel.monitoring.Monitors
  val monitors = new Monitors(kernelMonitors)
  val monitorTag = "compiler2.1"
  val parser = new CypherParser(monitors.newMonitor[ParserMonitor](monitorTag))
  val semanticChecker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
  val astRewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag), shouldExtractParameters = false)

  class SpyableMetricsFactory extends MetricsFactory {
    def newSelectivityEstimator(statistics: GraphStatistics) =
      SimpleMetricsFactory.newSelectivityEstimator(statistics)
    def newCostModel(cardinality: CardinalityModel) =
      SimpleMetricsFactory.newCostModel(cardinality)
    def newCardinalityEstimator(statistics: GraphStatistics, selectivity: SelectivityModel) =
      SimpleMetricsFactory.newCardinalityEstimator(statistics, selectivity)
  }

  def newMetricsFactory = SimpleMetricsFactory

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics) = newMetricsFactory.newMetrics(stats)

  def newMockedGraphStatistics = mock[GraphStatistics]

  def newMockedMetricsFactory = spy(new SpyableMetricsFactory)

  def newMockedLogicalPlanContext(planContext: PlanContext,
                                  metrics: Metrics = self.mock[Metrics],
                                  semanticTable: SemanticTable = self.mock[SemanticTable],
                                  queryGraph: QueryGraph = self.mock[QueryGraph],
                                  strategy: PlanningStrategy = new GreedyPlanningStrategy()) =

    LogicalPlanContext(planContext, metrics, semanticTable, queryGraph, strategy)

  implicit class RichLogicalPlan(plan: LogicalPlan) {
    def asTableEntry = plan.coveredIds -> plan
  }

  def newMockedStatistics = mock[GraphStatistics]

  def newMockedPlanContext(implicit statistics: GraphStatistics = newMockedStatistics) = {
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    context
  }

  def newMockedLogicalPlan(ids: String*)(implicit context: LogicalPlanContext): LogicalPlan =
    newMockedLogicalPlanWithPatterns(ids.map(IdName).toSet)

  def newMockedLogicalPlanWithPatterns(ids: Set[IdName], patterns: Seq[PatternRelationship] = Seq.empty)(implicit context: LogicalPlanContext): LogicalPlan = {
    val plan = mock[LogicalPlan]
    doReturn(s"MockedLogicalPlan(ids = $ids})").when(plan).toString
    doReturn(ids).when(plan).coveredIds
    doReturn(Seq.empty).when(plan).solvedPredicates
    doReturn(patterns).when(plan).solvedPatterns
    plan
  }

  def newPlanner(metricsFactory: MetricsFactory): Planner =
    new Planner(monitors, metricsFactory, monitors.newMonitor[PlanningMonitor]())

  def produceLogicalPlan(queryText: String)
                        (implicit planner: Planner, planContext: PlanContext) = {
    val parsedStatement = parser.parse(queryText)
    semanticChecker.check(queryText, parsedStatement)
    val (rewrittenStatement, _) = astRewriter.rewrite(queryText, parsedStatement)
    rewrittenStatement match {
      case ast: Query =>
        val semanticTable = semanticChecker.check(queryText, rewrittenStatement)
        planner.produceLogicalPlan(ast, semanticTable)(planContext)
      case _ =>
        throw new IllegalArgumentException("produceLogicalPlan only supports ast.Query input")
    }
  }

  implicit def withPos[T](expr: InputPosition => T): T = expr(DummyPosition(0))

  implicit def idName(name: String) = IdName(name)
}
