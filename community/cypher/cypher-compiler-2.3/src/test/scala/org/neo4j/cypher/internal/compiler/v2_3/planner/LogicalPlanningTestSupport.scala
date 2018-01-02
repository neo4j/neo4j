/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.planner.execution.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy.{GreedyPlanTable, GreedyQueryGraphSolver, expandsOnly, expandsOrJoins}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.helpers.Clock

import scala.collection.mutable

trait LogicalPlanningTestSupport extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val monitors = mock[Monitors]
  val parser = new CypherParser
  val semanticChecker = new SemanticChecker
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val astRewriter = new ASTRewriter(rewriterSequencer, shouldExtractParameters = false)
  val mockRel = newPatternRelationship("a", "b", "r")
  val tokenResolver = new SimpleTokenResolver()
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  def solvedWithEstimation(cardinality: Cardinality) = CardinalityEstimation.lift(PlannerQuery.empty, cardinality)

  def newPatternRelationship(start: IdName, end: IdName, rel: IdName, dir: SemanticDirection = SemanticDirection.OUTGOING, types: Seq[RelTypeName] = Seq.empty, length: PatternLength = SimplePatternLength) = {
    PatternRelationship(rel, (start, end), dir, types, length)
  }

  class SpyableMetricsFactory extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel)
    def newCostModel() =
      SimpleMetricsFactory.newCostModel()
    def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel =
      SimpleMetricsFactory.newQueryGraphCardinalityModel(statistics)
  }

  def newMockedQueryGraph = mock[QueryGraph]

  def newMockedPipeExecutionPlanBuilderContext: PipeExecutionBuilderContext = {
    val context = mock[PipeExecutionBuilderContext]
    val cardinality = new Metrics.CardinalityModel {
      def apply(pq: PlannerQuery, ignored: QueryGraphSolverInput, ignoredAsWell: SemanticTable) = pq match {
        case PlannerQuery.empty => Cardinality(1)
        case _ => Cardinality(104999.99999)
      }
    }
    when(context.cardinality).thenReturn(cardinality)
    val semanticTable = new SemanticTable(resolvedRelTypeNames = mutable.Map("existing1" -> RelTypeId(1), "existing2" -> RelTypeId(2), "existing3" -> RelTypeId(3)))

    when(context.semanticTable).thenReturn(semanticTable)

    context
  }

  def newMetricsFactory = SimpleMetricsFactory

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics) = newMetricsFactory.newMetrics(stats)

  def newMockedGraphStatistics = mock[GraphStatistics]

  def newMockedSemanticTable: SemanticTable = {
    val m = mock[SemanticTable]
    when(m.resolvedLabelIds).thenReturn(mutable.Map.empty[String, LabelId])
    when(m.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    when(m.resolvedRelTypeNames).thenReturn(mutable.Map.empty[String, RelTypeId])
    m
  }

  def newMockedMetricsFactory = spy(new SpyableMetricsFactory)

  def newMockedStrategy(plan: LogicalPlan) = {
    val strategy = mock[QueryGraphSolver]
    doReturn(plan).when(strategy).plan(any())(any(), any())
    strategy
  }

  def mockedMetrics: Metrics = newSimpleMetrics(hardcodedStatistics)

  def newMockedLogicalPlanningContext(planContext: PlanContext,
                                      metrics: Metrics = mockedMetrics,
                                      semanticTable: SemanticTable = newMockedSemanticTable,
                                      strategy: QueryGraphSolver = new CompositeQueryGraphSolver(
                                        new GreedyQueryGraphSolver(expandsOrJoins),
                                        new GreedyQueryGraphSolver(expandsOnly)
                                      ),
                                      cardinality: Cardinality = Cardinality(1),
                                      strictness: Option[StrictnessMode] = None,
                                      notificationLogger: InternalNotificationLogger = devNullLogger,
                                      useErrorsOverWarnings: Boolean = false): LogicalPlanningContext =
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings)

  def newMockedStatistics = mock[GraphStatistics]
  def hardcodedStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(implicit statistics: GraphStatistics = newMockedStatistics) = {
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    context
  }

  def newMockedLogicalPlanWithProjections(ids: String*): LogicalPlan = {
    val projections = RegularQueryProjection(projections = ids.map((id) => id -> ident(id)).toMap)
    FakePlan(ids.map(IdName(_)).toSet)(CardinalityEstimation.lift(PlannerQuery(
        horizon = projections,
        graph = QueryGraph.empty.addPatternNodes(ids.map(IdName(_)): _*)
      ), Cardinality(0))
    )
  }

  def newMockedLogicalPlan(idNames: Set[IdName], cardinality: Cardinality = Cardinality(1), hints: Set[Hint] = Set[Hint]()): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addHints(hints)
    FakePlan(idNames)(CardinalityEstimation.lift(PlannerQuery(qg), cardinality))
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.map(IdName(_)).toSet)

  def newMockedLogicalPlanWithSolved(ids: Set[IdName], solved: PlannerQuery with CardinalityEstimation): LogicalPlan =
    FakePlan(ids)(solved)

  def newMockedLogicalPlanWithPatterns(ids: Set[IdName], patterns: Seq[PatternRelationship] = Seq.empty): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(ids.toSeq: _*).addPatternRelationships(patterns)
    FakePlan(ids)(CardinalityEstimation.lift(PlannerQuery(qg), Cardinality(0)))
  }

  def newPlanner(metricsFactory: MetricsFactory): CostBasedExecutablePlanBuilder = {
    val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))
    CostBasedPipeBuilderFactory.create(
      monitors = monitors,
      metricsFactory = metricsFactory,
      queryPlanner = queryPlanner,
      rewriterSequencer = rewriterSequencer,
      plannerName = None,
      runtimeBuilder = InterpretedRuntimeBuilder(InterpretedPlanBuilder(Clock.SYSTEM_CLOCK, monitors)),
      semanticChecker = semanticChecker,
      useErrorsOverWarnings = false,
      idpMaxTableSize = DefaultIDPSolverConfig.maxTableSize,
      idpIterationDuration = DefaultIDPSolverConfig.iterationDurationLimit)
  }

  def produceLogicalPlan(queryText: String)(implicit planner: CostBasedExecutablePlanBuilder, planContext: PlanContext): LogicalPlan = {
    val parsedStatement = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val semanticState = semanticChecker.check(queryText, parsedStatement, mkException)
    val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryText, parsedStatement, semanticState)
    CostBasedExecutablePlanBuilder.rewriteStatement(rewrittenStatement, semanticState.scopeTree, SemanticTable(types = semanticState.typeTable), rewriterSequencer, semanticChecker, postConditions, monitors.newMonitor[AstRewritingMonitor]()) match {
      case (ast: Query, newTable) =>
        val semanticState = semanticChecker.check(queryText, ast, mkException)
        tokenResolver.resolve(ast)(newTable, planContext)
        val (logicalPlan, _) = planner.produceLogicalPlan(ast, newTable)(planContext, devNullLogger)
        logicalPlan

      case _ =>
        throw new IllegalArgumentException("produceLogicalPlan only supports ast.Query input")
    }
  }

  def identHasLabel(name: String, labelName: String): HasLabels = {
    val labelNameObj: LabelName = LabelName(labelName)_
    HasLabels(Identifier(name)_, Seq(labelNameObj))_
  }

  def greedyPlanTableWith(plans: LogicalPlan*)(implicit ctx: LogicalPlanningContext) =
    plans.foldLeft(GreedyPlanTable.empty)(_ + _)
}

case class FakePlan(availableSymbols: Set[IdName])(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LogicalPlanWithoutExpressions with LazyLogicalPlan {
  def rhs = None
  def lhs = None
}
