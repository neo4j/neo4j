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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, PlanContext}
import org.neo4j.graphdb.Direction
import collection.mutable

trait LogicalPlanningTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  self: CypherFunSuite =>

  val kernelMonitors = new org.neo4j.kernel.monitoring.Monitors
  val monitors = new Monitors(kernelMonitors)
  val monitorTag = "compiler2.1"
  val parser = new CypherParser(monitors.newMonitor[ParserMonitor[Statement]](monitorTag))
  val semanticChecker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
  val astRewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag), shouldExtractParameters = false)
  val mockRel = newPatternRelationship("a", "b", "r")
  val tokenResolver = new SimpleTokenResolver()

  def newPatternRelationship(start: IdName, end: IdName, rel: IdName, dir: Direction = Direction.OUTGOING, types: Seq[RelTypeName] = Seq.empty, length: PatternLength = SimplePatternLength) = {
    PatternRelationship(rel, (start, end), dir, types, length)
  }

  class SpyableMetricsFactory extends MetricsFactory {
    def newCardinalityEstimator(statistics: GraphStatistics, selectivity: PredicateSelectivityCombiner, semanticTable: SemanticTable) =
      SimpleMetricsFactory.newCardinalityEstimator(statistics, selectivity, semanticTable)
    def newCostModel(cardinality: CardinalityModel) =
      SimpleMetricsFactory.newCostModel(cardinality)
  }

  def newMockedQueryGraph = mock[QueryGraph]

  def newMockedPipeExecutionPlanBuilderContext: PipeExecutionBuilderContext = {
    val context = mock[PipeExecutionBuilderContext]
    val cardinality = new Metrics.CardinalityModel {
      def apply(v1: LogicalPlan) = Cardinality(1)
    }
    when(context.cardinality).thenReturn(cardinality)
    context
  }

  def newMetricsFactory = SimpleMetricsFactory

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics, semanticTable: SemanticTable) =
    newMetricsFactory.newMetrics(stats, semanticTable)

  def newMockedGraphStatistics = mock[GraphStatistics]

  def newMockedSemanticTable: SemanticTable = {
    val m = mock[SemanticTable]
    when(m.resolvedLabelIds).thenReturn(mutable.Map.empty[String, LabelId])
    m
  }

  def newMockedMetricsFactory = spy(new SpyableMetricsFactory)

  def newMockedStrategy(plan: LogicalPlan) = {
    val strategy = mock[QueryGraphSolver]
    doReturn(plan).when(strategy).plan(any())(any(), any())
    strategy
  }

  def newMockedLogicalPlanningContext(planContext: PlanContext,
                                        metrics: Metrics = self.mock[Metrics],
                                        semanticTable: SemanticTable = newMockedSemanticTable,
                                        strategy: QueryGraphSolver = new GreedyQueryGraphSolver()): LogicalPlanningContext =
    LogicalPlanningContext(planContext, metrics, semanticTable, strategy)

  implicit class RichLogicalPlan(plan: LogicalPlan) {
    def asTableEntry = plan.availableSymbols -> plan
  }

  def newMockedStatistics = mock[GraphStatistics]
  def hardcodedStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(implicit statistics: GraphStatistics = newMockedStatistics) = {
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    context
  }

  def newMockedLogicalPlanWithProjections(ids: String*)(implicit context: LogicalPlanningContext) = {
    val projections =
      RegularQueryProjection(projections = ids.map((id) => id -> ident(id)).toMap)

    FakePlan(ids.map(IdName).toSet)(PlannerQuery(
        horizon = projections,
        graph = QueryGraph.empty.addPatternNodes(ids.map(IdName).toSeq: _*)
      )
    )
  }

  def newMockedLogicalPlan(idNames: Set[IdName]): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(idNames.toSeq: _*)
    FakePlan(idNames)(PlannerQuery(qg))
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.map(IdName).toSet)

  def newMockedLogicalPlan2(ids: Set[IdName], solved: PlannerQuery): LogicalPlan =
    FakePlan(ids)(solved)

  def newMockedLogicalPlanWithPatterns(ids: Set[IdName], patterns: Seq[PatternRelationship] = Seq.empty)
                                      (implicit context: LogicalPlanningContext): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(ids.toSeq: _*).addPatternRels(patterns)
    FakePlan(ids)(PlannerQuery(qg))
  }

  def newPlanner(metricsFactory: MetricsFactory): Planner =
    new Planner(monitors, metricsFactory, monitors.newMonitor[PlanningMonitor]())

  def produceLogicalPlan(queryText: String)(implicit planner: Planner, planContext: PlanContext): LogicalPlan = {
    val parsedStatement = parser.parse(queryText)
    val semanticState = semanticChecker.check(queryText, parsedStatement)
    val (rewrittenStatement, _) = astRewriter.rewrite(queryText, parsedStatement, semanticState)
    Planner.rewriteStatement(rewrittenStatement, semanticState.scopeTree) match {
      case ast: Query =>
        val semanticState = semanticChecker.check(queryText, ast)
        val semanticTable = SemanticTable(types = semanticState.typeTable)
        tokenResolver.resolve(ast)(semanticTable, planContext)
        val (logicalPlan, _) = planner.produceLogicalPlan(ast, semanticTable)(planContext)
        logicalPlan

      case _ =>
        throw new IllegalArgumentException("produceLogicalPlan only supports ast.Query input")
    }
  }

  implicit def idName(name: String): IdName = IdName(name)
}

case class FakePlan(availableSymbols: Set[IdName])(val solved: PlannerQuery) extends LogicalPlan {
  def rhs = None
  def lhs = None
}
