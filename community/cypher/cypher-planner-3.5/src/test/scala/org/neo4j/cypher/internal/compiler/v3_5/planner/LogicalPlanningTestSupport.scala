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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.csv.reader.Configuration
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.{LogicalPlanProducer, devNullListener}
import org.neo4j.cypher.internal.compiler.v3_5.test_helpers.ContextHelper
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, GraphStatistics, InstrumentedGraphStatistics, PlanContext, PlanningAttributes}
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.frontend.phases._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.parser.CypherParser
import org.neo4j.cypher.internal.v3_5.rewriting.RewriterStepSequencer.newPlain
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters._
import org.neo4j.cypher.internal.v3_5.rewriting.{Deprecations, RewriterStepSequencer}
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.{CypherFunSuite, CypherTestSupport}

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
        solveds.set(plan.id, PlannerQuery.empty)
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
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val providedOrders = new ProvidedOrders
    val planningAttributes = PlanningAttributes(solveds, cardinalities, providedOrders)
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener,
      planningAttributes = planningAttributes)
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
    val solveds = new StubSolveds
    val cardinalities = new StubCardinalities
    val providedOrders = new StubProvidedOrders
    val planningAttributes = new PlanningAttributes(solveds, cardinalities, providedOrders)
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, csvBufferSize = config.csvBufferSize,
                           config = QueryPlannerConfiguration.default, costComparisonListener = devNullListener,
      planningAttributes = planningAttributes)
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
    val solved = RegularPlannerQuery(
      horizon = projections,
      queryGraph = QueryGraph.empty.addPatternNodes(ids: _*)
    )
    val res = FakePlan(ids.toSet)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, Cardinality(1))
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  def newMockedLogicalPlan(idNames: Set[String],
                           planningAttributes: PlanningAttributes = PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders),
                           hints: Set[Hint] = Set[Hint](),
                           availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addHints(hints))
    newMockedLogicalPlanWithSolved(planningAttributes, idNames, solved, Cardinality(1), availablePropertiesFromIndexes)
  }

  def newMockedLogicalPlanWithSolved(planningAttributes: PlanningAttributes = PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders),
                                     idNames: Set[String],
                                     solved: PlannerQuery,
                                     cardinality: Cardinality = Cardinality(1),
                                     availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val res = FakePlan(idNames, availablePropertiesFromIndexes)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, cardinality)
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  def newMockedLogicalPlanWithPatterns(planningAttributes: PlanningAttributes,
                                       idNames: Set[String],
                                       patterns: Seq[PatternRelationship] = Seq.empty,
                                       availablePropertiesFromIndexes: Map[Property, String] = Map.empty): LogicalPlan = {
    val solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addPatternRelationships(patterns))
    newMockedLogicalPlanWithSolved(planningAttributes, idNames, solved, Cardinality(0), availablePropertiesFromIndexes)
  }

  def newAttributes() : PlanningAttributes = {
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val providedOrders = new ProvidedOrders
    PlanningAttributes(solveds, cardinalities, providedOrders)
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
    PreparatoryRewriting(Deprecations.V1) andThen
    SemanticAnalysis(warn = true) andThen
    AstRewriting(newPlain, literalExtraction = Never) andThen
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
    val state = LogicalPlanState(query, None, CostBasedPlannerName.default, PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders))
    val context = ContextHelper.create(exceptionCreator = mkException, planContext = planContext, logicalPlanIdGen = idGen)
    val output = pipeLine.transform(state, context)

    output.unionQuery
  }

  def identHasLabel(name: String, labelName: String): HasLabels = {
    val labelNameObj: LabelName = LabelName(labelName)_
    HasLabels(Variable(name)_, Seq(labelNameObj))_
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

  override val availableCachedNodeProperties: Map[Property, CachedNodeProperty] = propertyMap.mapValues(s => {
    val (nodeName, propertyKey) = s.span(_ != '.')
    CachedNodeProperty(nodeName, PropertyKeyName(propertyKey.tail)(InputPosition.NONE))(InputPosition.NONE)
  })
}
