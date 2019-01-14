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
package org.neo4j.cypher.internal.compiler.v3_4.planner

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters.namePatternPredicatePatternElements
import org.neo4j.cypher.internal.compiler.v3_4.phases._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{devNullListener, LogicalPlanProducer}
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer.newPlain
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, LabelId, PropertyKeyId, RelTypeId}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.csv.reader.Configuration

import scala.collection.mutable

trait LogicalPlanningTestSupport extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val monitors = mock[Monitors]
  val parser = new CypherParser
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val astRewriter = new ASTRewriter(rewriterSequencer, literalExtraction = Never, getDegreeRewriting = true)
  val mockRel = newPatternRelationship("a", "b", "r")

  def newPatternRelationship(start: String, end: String, rel: String, dir: SemanticDirection = SemanticDirection.OUTGOING, types: Seq[RelTypeName] = Seq.empty, length: PatternLength = SimplePatternLength) = {
    PatternRelationship(rel, (start, end), dir, types, length)
  }

  class SpyableMetricsFactory extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator) =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, evaluator)
    def newCostModel(config: CypherCompilerConfiguration) =
      SimpleMetricsFactory.newCostModel(config)
    def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel =
      SimpleMetricsFactory.newQueryGraphCardinalityModel(statistics)
  }

  def newMockedQueryGraph = mock[QueryGraph]

  def newMetricsFactory = SimpleMetricsFactory

  def newExpressionEvaluator = new ExpressionEvaluator {
    override def evaluateExpression(expr: Expression): Option[Any] = None
  }

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics) =
    newMetricsFactory.newMetrics(stats, newExpressionEvaluator, config)

  def newMockedGraphStatistics = mock[GraphStatistics]

  def newMockedSemanticTable: SemanticTable = {
    val m = mock[SemanticTable]
    when(m.resolvedLabelNames).thenReturn(mutable.Map.empty[String, LabelId])
    when(m.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    when(m.resolvedRelTypeNames).thenReturn(mutable.Map.empty[String, RelTypeId])
    when(m.id(any[PropertyKeyName]())).thenReturn(None)
    when(m.id(any[LabelName])).thenReturn(None)
    when(m.id(any[RelTypeName])).thenReturn(None)
    m
  }

  def newMockedMetricsFactory = spy(new SpyableMetricsFactory)

  def newMockedStrategy(plan: LogicalPlan) = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any(), any(), any[Solveds], any[Cardinalities])).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan = {
        val solveds = invocation.getArgument[Solveds](2)
        val cardinalities = invocation.getArgument[Cardinalities](3)
        solveds.set(plan.id, PlannerQuery.empty)
        cardinalities.set(plan.id, 0.0)
        plan
      }
    })
    strategy
  }

  def newMockedStrategyWithMultiplePlans(plans: LogicalPlan*): QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    val planIter = plans.iterator
    when(strategy.plan(any(), any(), any[Solveds], any[Cardinalities])).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan = {
        val solveds = invocation.getArgument[Solveds](2)
        val cardinalities = invocation.getArgument[Cardinalities](3)
        val plan = planIter.next()
        solveds.set(plan.id, PlannerQuery.empty)
        cardinalities.set(plan.id, 0.0)
        plan
      }
    })
    strategy
  }

  def mockedMetrics: Metrics = newSimpleMetrics(hardcodedStatistics)

  def newMockedLogicalPlanningContext(planContext: PlanContext,
                                      metrics: Metrics = mockedMetrics,
                                      semanticTable: SemanticTable = newMockedSemanticTable,
                                      strategy: QueryGraphSolver = new IDPQueryGraphSolver(
                                        SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]),
                                        cartesianProductsOrValueJoins, mock[IDPQueryGraphSolverMonitor]),
                                      cardinality: Cardinality = Cardinality(1),
                                      strictness: Option[StrictnessMode] = None,
                                      notificationLogger: InternalNotificationLogger = devNullLogger,
                                      useErrorsOverWarnings: Boolean = false): (LogicalPlanningContext, Solveds, Cardinalities) = {
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    (LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, solveds, cardinalities, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener),
      solveds, cardinalities)
  }

  def newMockedLogicalPlanningContextWithFakeAttributes(planContext: PlanContext,
                                      metrics: Metrics = mockedMetrics,
                                      semanticTable: SemanticTable = newMockedSemanticTable,
                                      strategy: QueryGraphSolver = new IDPQueryGraphSolver(
                                        SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]),
                                        cartesianProductsOrValueJoins, mock[IDPQueryGraphSolverMonitor]),
                                      cardinality: Cardinality = Cardinality(1),
                                      strictness: Option[StrictnessMode] = None,
                                      notificationLogger: InternalNotificationLogger = devNullLogger,
                                      useErrorsOverWarnings: Boolean = false): LogicalPlanningContext = {
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, new StubSolveds, new StubCardinalities, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, csvBufferSize = config.csvBufferSize,
                           config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener)
  }

  def newMockedStatistics = mock[GraphStatistics]
  def hardcodedStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(implicit statistics: GraphStatistics = newMockedStatistics) = {
    val context = mock[PlanContext]
    doReturn(statistics, Nil: _*).when(context).statistics
    context
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan = {
    newMockedLogicalPlan(ids.toSet)
  }

  def newMockedLogicalPlan(solveds: Solveds, cardinalities: Cardinalities, ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.toSet, solveds, cardinalities)

  def newMockedLogicalPlanWithProjections(solveds: Solveds, ids: String*): LogicalPlan = {
    val projections = RegularQueryProjection(projections = ids.map((id) => id -> varFor(id)).toMap)
    val solved = RegularPlannerQuery(
      horizon = projections,
      queryGraph = QueryGraph.empty.addPatternNodes(ids: _*)
    )
    val res = FakePlan(ids.toSet)
    solveds.set(res.id, solved)
    res
  }

  def newMockedLogicalPlan(idNames: Set[String],
                           solveds: Solveds = new Solveds,
                           cardinalities: Cardinalities = new Cardinalities,
                           hints: Set[Hint] = Set[Hint]()): LogicalPlan = {
    val solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addHints(hints))
    newMockedLogicalPlanWithSolved(solveds, cardinalities, idNames, solved, Cardinality(1))
  }

  def newMockedLogicalPlanWithSolved(solveds: Solveds = new Solveds,
                                     cardinalities: Cardinalities = new Cardinalities,
                                     idNames: Set[String],
                                     solved: PlannerQuery,
                                     cardinality: Cardinality = Cardinality(1)): LogicalPlan = {
    val res = FakePlan(idNames)
    solveds.set(res.id, solved)
    cardinalities.set(res.id, cardinality)
    res
  }

  def newMockedLogicalPlanWithPatterns(solveds: Solveds = new Solveds,
                                       cardinalities: Cardinalities = new Cardinalities,
                                       idNames: Set[String],
                                       patterns: Seq[PatternRelationship] = Seq.empty): LogicalPlan = {
    val solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addPatternRelationships(patterns))
    newMockedLogicalPlanWithSolved(solveds, cardinalities, idNames, solved, Cardinality(0))
  }

  val config = CypherCompilerConfiguration(
    queryCacheSize = 100,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0.5, 1000),
    useErrorsOverWarnings = false,
    idpMaxTableSize = DefaultIDPSolverConfig.maxTableSize,
    idpIterationDuration = DefaultIDPSolverConfig.iterationDurationLimit,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    csvBufferSize = Configuration.DEFAULT_BUFFER_SIZE_4MB,
    nonIndexedLabelWarningThreshold = 10000,
    planWithMinimumCardinalityEstimates = true,
    lenientCreateRelationship = false
  )

  def buildPlannerQuery(query: String, lookup: Option[QualifiedName => ProcedureSignature] = None) = {
    val queries: Seq[PlannerQuery] = buildPlannerUnionQuery(query, lookup).queries
    queries.head
  }

  val pipeLine =
    Parsing andThen
    PreparatoryRewriting andThen
    SemanticAnalysis(warn = true) andThen
    AstRewriting(newPlain, literalExtraction = Never) andThen
    RewriteProcedureCalls andThen
    Namespacer andThen
    rewriteEqualityToInPredicate andThen
    CNFNormalizer andThen
    LateAstRewriting andThen
//    ResolveTokens andThen
    // This fakes pattern expression naming for testing purposes
    // In the actual code path, this renaming happens as part of planning
    //
    // cf. QueryPlanningStrategy
    //
    Do(rewriteStuff _) andThen
    CreatePlannerQuery

  private def rewriteStuff(input: BaseState, context: CompilerContext): BaseState = {
    val newStatement = input.statement().endoRewrite(namePatternPredicatePatternElements)
    input.withStatement(newStatement)
  }

  def buildPlannerUnionQuery(query: String, procLookup: Option[QualifiedName => ProcedureSignature] = None,
                             fcnLookup: Option[QualifiedName => Option[UserFunctionSignature]] = None) = {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "foo"),
      inputSignature = IndexedSeq.empty,
      deprecationInfo = None,
      outputSignature = Some(IndexedSeq(FieldSignature("all", CTInteger))),
      accessMode = ProcedureReadOnlyAccess(Array.empty)
    )
    val mkException = new SyntaxExceptionCreator(query, Some(pos))
    val procs: (QualifiedName) => ProcedureSignature = procLookup.getOrElse(_ => signature)
    val funcs: (QualifiedName) => Option[UserFunctionSignature] = fcnLookup.getOrElse(_ => None)
    val planContext = new TestSignatureResolvingPlanContext(procs, funcs)
    val state = LogicalPlanState(query, None, CostBasedPlannerName.default, new Solveds, new Cardinalities)
    val context = ContextHelper.create(exceptionCreator = mkException, planContext = planContext, logicalPlanIdGen = idGen)
    val output = pipeLine.transform(state, context)

    output.unionQuery
  }

  def identHasLabel(name: String, labelName: String): HasLabels = {
    val labelNameObj: LabelName = LabelName(labelName)_
    HasLabels(Variable(name)_, Seq(labelNameObj))_
  }
}

case class FakePlan(availableSymbols: Set[String] = Set.empty)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {
  def rhs = None
  def lhs = None
}
