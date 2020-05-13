/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.spy
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.TestSignatureResolvingPlanContext
import org.neo4j.cypher.internal.compiler.phases.CreatePlannerQuery
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.frontend.phases.ASTRewriter
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.Do
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.LateAstRewriting
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.frontend.phases.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.StrictnessMode
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.LazyLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer.newPlain
import org.neo4j.cypher.internal.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.rewriting.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport

import scala.collection.mutable

trait LogicalPlanningTestSupport extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val monitors = mock[Monitors]
  val parser = new CypherParser
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val innerVariableNamer = new GeneratingNamer
  val astRewriter = new ASTRewriter(rewriterSequencer, literalExtraction = Never, getDegreeRewriting = true, innerVariableNamer = innerVariableNamer)
  val mockRel = newPatternRelationship("a", "b", "r")

  def newPatternRelationship(start: String, end: String, rel: String, dir: SemanticDirection = SemanticDirection.OUTGOING, types: Seq[RelTypeName] = Seq.empty, length: PatternLength = SimplePatternLength) = {
    PatternRelationship(rel, (start, end), dir, types, length)
  }

  class SpyableMetricsFactory extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator) =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, evaluator)
    def newCostModel(config: CypherPlannerConfiguration) =
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
    when(strategy.plan(any(), any(), any())).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan = {
        val context = invocation.getArgument[LogicalPlanningContext](2)
        val solveds = context.planningAttributes.solveds
        val cardinalities = context.planningAttributes.cardinalities
        val providedOrders = context.planningAttributes.providedOrders
        solveds.set(plan.id, SinglePlannerQuery.empty)
        cardinalities.set(plan.id, 0.0)
        providedOrders.set(plan.id, ProvidedOrder.empty)
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
                                      useErrorsOverWarnings: Boolean = false): LogicalPlanningContext = {
    val planningAttributes = PlanningAttributes.newAttributes
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener,
      planningAttributes = planningAttributes,
      innerVariableNamer = innerVariableNamer,
      idGen = idGen)
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
    val planningAttributes = newStubbedPlanningAttributes
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, csvBufferSize = config.csvBufferSize,
      config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener,
      planningAttributes = planningAttributes,
      innerVariableNamer = innerVariableNamer,
      idGen = idGen)
  }

  def newMockedStatistics: InstrumentedGraphStatistics = mock[InstrumentedGraphStatistics]
  def hardcodedStatistics: GraphStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(statistics: InstrumentedGraphStatistics = newMockedStatistics): PlanContext = {
    val context = mock[PlanContext]
    doReturn(statistics, Nil: _*).when(context).statistics
    context
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan = {
    newMockedLogicalPlan(ids.toSet)
  }

  def newMockedLogicalPlan(planningAttributes: PlanningAttributes, ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.toSet, planningAttributes)

  def newMockedLogicalPlanWithProjections(planningAttributes: PlanningAttributes, ids: String*): LogicalPlan = {
    val projections = RegularQueryProjection(projections = ids.map(id => id -> varFor(id)).toMap)
    val solved = RegularSinglePlannerQuery(
      queryGraph = QueryGraph.empty.addPatternNodes(ids: _*),
      horizon = projections
    )
    val res = FakePlan(ids.toSet)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, Cardinality(1))
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  def newMockedLogicalPlan(idNames: Set[String],
                           planningAttributes: PlanningAttributes = PlanningAttributes.newAttributes,
                           hints: Set[Hint] = Set[Hint](),
                           availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addHints(hints))
    newMockedLogicalPlanWithSolved(planningAttributes, idNames, solved, Cardinality(1), availablePropertiesFromIndexes = availablePropertiesFromIndexes)
  }

  def newMockedLogicalPlanWithSolved(planningAttributes: PlanningAttributes = PlanningAttributes.newAttributes,
                                     idNames: Set[String],
                                     solved: SinglePlannerQuery,
                                     cardinality: Cardinality = Cardinality(1),
                                     providedOrder: ProvidedOrder = ProvidedOrder.empty,
                                     availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val res = FakePlan(idNames, availablePropertiesFromIndexes)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, cardinality)
    planningAttributes.providedOrders.set(res.id, providedOrder)
    res
  }

  def newMockedLogicalPlanWithPatterns(planningAttributes: PlanningAttributes,
                                       idNames: Set[String],
                                       patterns: Seq[PatternRelationship] = Seq.empty,
                                       availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addPatternRelationships(patterns))
    newMockedLogicalPlanWithSolved(planningAttributes, idNames, solved, Cardinality(0), availablePropertiesFromIndexes = availablePropertiesFromIndexes)
  }

  val config = CypherPlannerConfiguration(
    queryCacheSize = 100,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0.5, 1000),
    useErrorsOverWarnings = false,
    idpMaxTableSize = DefaultIDPSolverConfig.maxTableSize,
    idpIterationDuration = DefaultIDPSolverConfig.iterationDurationLimit,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    csvBufferSize = 4 * 1024 * 1024,
    nonIndexedLabelWarningThreshold = 10000,
    planSystemCommands = false,
    readPropertiesFromCursor = false
  )

  def buildSinglePlannerQuery(query: String, lookup: Option[QualifiedName => ProcedureSignature] = None): SinglePlannerQuery = {
    buildPlannerQuery(query, lookup).query match {
      case pq: SinglePlannerQuery => pq
      case _ => throw new IllegalArgumentException("This method cannot be used for UNION queries")
    }
  }

  val pipeLine =
    Parsing andThen
      PreparatoryRewriting(Deprecations.V1) andThen
      SemanticAnalysis(warn = true, SemanticFeature.CorrelatedSubQueries) andThen
      AstRewriting(newPlain, literalExtraction = Never, innerVariableNamer = new GeneratingNamer) andThen
      RewriteProcedureCalls andThen
      Namespacer andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      // This fakes pattern expression naming for testing purposes
      // In the actual code path, this renaming happens as part of planning
      //
      // cf. QueryPlanningStrategy
      //
      Do(rewriteStuff _) andThen
      CreatePlannerQuery

  private def rewriteStuff(input: BaseState, context: PlannerContext): BaseState = {
    val newStatement = input.statement().endoRewrite(namePatternPredicatePatternElements)
    input.withStatement(newStatement)
  }

  def buildPlannerQuery(query: String,
                        procLookup: Option[QualifiedName => ProcedureSignature] = None,
                        fcnLookup: Option[QualifiedName => Option[UserFunctionSignature]] = None): PlannerQuery = {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "foo"),
      inputSignature = IndexedSeq.empty,
      deprecationInfo = None,
      outputSignature = Some(IndexedSeq(FieldSignature("all", CTInteger))),
      accessMode = ProcedureReadOnlyAccess(Array.empty),
      id = 42
    )
    val exceptionFactory = Neo4jCypherExceptionFactory(query, Some(pos))
    val procs: QualifiedName => ProcedureSignature = procLookup.getOrElse(_ => signature)
    val funcs: QualifiedName => Option[UserFunctionSignature] = fcnLookup.getOrElse(_ => None)
    val planContext = new TestSignatureResolvingPlanContext(procs, funcs)
    val state = LogicalPlanState(query, None, CostBasedPlannerName.default, PlanningAttributes.newAttributes)
    val context = ContextHelper.create(cypherExceptionFactory = exceptionFactory, planContext = planContext, logicalPlanIdGen = idGen)
    val output = pipeLine.transform(state, context)

    output.query
  }
}

case object namePatternPredicatePatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = instance.apply(in)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case expr: PatternExpression =>
      val (rewrittenExpr, _) = PatternExpressionPatternElementNamer(expr)
      rewrittenExpr
  })
}

case class FakePlan(availableSymbols: Set[String] = Set.empty, propertyMap: Map[Property, String] = Map.empty)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {
  def rhs = None
  def lhs = None
}
