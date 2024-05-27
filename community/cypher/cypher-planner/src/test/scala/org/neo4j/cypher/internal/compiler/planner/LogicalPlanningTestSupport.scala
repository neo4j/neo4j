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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.spy
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.common
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.TestSignatureResolvingPlanContext
import org.neo4j.cypher.internal.compiler.helpers.FakeLeafPlan
import org.neo4j.cypher.internal.compiler.phases.CreatePlannerQuery
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.NameDeduplication
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.Settings
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.StaticComponents
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy.NoInference
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.MoveBoundaryNodePredicates
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.Parse
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ProjectNamedPathsRewriter
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.collapseMultipleInPredicates
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizerTest
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.constraints.SchemaValueType

trait LogicalPlanningTestSupport extends AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val monitors = mock[Monitors]
  val parser = JavaCCParser
  val mockRel = newPatternRelationship("a", "b", "r")

  def newPatternRelationship(
    start: String,
    end: String,
    rel: String,
    dir: SemanticDirection = SemanticDirection.OUTGOING,
    types: Seq[RelTypeName] = Seq.empty,
    length: PatternLength = SimplePatternLength
  ) = {
    PatternRelationship(varFor(rel), (varFor(start), varFor(end)), dir, types, length)
  }

  /**
   * For Mockito to work, we need a class that it can inherit from. This is not possible for SimpleMetricsFactory. Thus, we have use for this class here.
   */
  class SpyableSimpleMetricsFactory extends MetricsFactory {

    override def newCardinalityEstimator(
      queryGraphCardinalityModel: QueryGraphCardinalityModel,
      selectivityCalculator: SelectivityCalculator,
      evaluator: ExpressionEvaluator
    ): CardinalityModel =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, selectivityCalculator, evaluator)

    override def newCostModel(
      executionModel: ExecutionModel,
      cancellationChecker: CancellationChecker
    ): Metrics.CostModel =
      SimpleMetricsFactory.newCostModel(executionModel, cancellationChecker)

    override def newQueryGraphCardinalityModel(
      planContext: PlanContext,
      selectivityCalculator: SelectivityCalculator,
      labelInferenceStrategy: LabelInferenceStrategy
    ): QueryGraphCardinalityModel = {
      SimpleMetricsFactory.newQueryGraphCardinalityModel(planContext, selectivityCalculator, labelInferenceStrategy)
    }
  }

  def newMockedQueryGraph = mock[QueryGraph]

  def newMetricsFactory = SimpleMetricsFactory

  def newExpressionEvaluator = new ExpressionEvaluator {
    override def evaluateExpression(expr: Expression): Option[Any] = None
  }

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics): Metrics = {
    val planContext = notImplementedPlanContext(stats)
    newMetricsFactory.newMetrics(
      planContext,
      newExpressionEvaluator,
      ExecutionModel.default,
      CancellationChecker.neverCancelled()
    )
  }

  def notImplementedPlanContext(stats: GraphStatistics) = {
    new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        stats,
        new MutableGraphStatisticsSnapshot()
      )

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

      override def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Set[String] =
        Set.empty

      override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = Map.empty

      override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] =
        Map.empty

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = Iterator.empty

      override def txStateHasChanges(): Boolean = false
    }
  }

  def newMockedGraphStatistics = mock[GraphStatistics]

  def newMockedSemanticTable: SemanticTable = {
    val m = mock[SemanticTable]
    when(m.resolvedLabelNames).thenReturn(Map.empty[String, LabelId])
    when(m.resolvedPropertyKeyNames).thenReturn(Map.empty[String, PropertyKeyId])
    when(m.resolvedRelTypeNames).thenReturn(Map.empty[String, RelTypeId])
    when(m.id(any[PropertyKeyName]())).thenReturn(None)
    when(m.id(any[LabelName])).thenReturn(None)
    when(m.id(any[RelTypeName])).thenReturn(None)
    when(m.types).thenReturn(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo])
    when(m.typeFor(any[Expression])).thenReturn(SemanticTable.TypeGetter(None))
    when(m.typeFor(any[String])).thenReturn(SemanticTable.TypeGetter(None))
    m
  }

  def newMockedMetricsFactory: MetricsFactory = spy(new SpyableSimpleMetricsFactory)

  def newMockedStrategy(plan: LogicalPlan) = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any(), any(), any())).thenAnswer(new Answer[BestResults[LogicalPlan]] {
      override def answer(invocation: InvocationOnMock): BestResults[LogicalPlan] = {
        val context = invocation.getArgument[LogicalPlanningContext](2)
        val solveds = context.staticComponents.planningAttributes.solveds
        val cardinalities = context.staticComponents.planningAttributes.cardinalities
        val providedOrders = context.staticComponents.planningAttributes.providedOrders
        solveds.set(plan.id, SinglePlannerQuery.empty)
        cardinalities.set(plan.id, 0.0)
        providedOrders.set(plan.id, ProvidedOrder.empty)
        BestResults(plan, None)
      }
    })
    strategy
  }

  def newMockedStrategyWithSortedPlan(plan: LogicalPlan, sortedPlan: LogicalPlan) = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any(), any(), any())).thenAnswer(new Answer[BestResults[LogicalPlan]] {
      override def answer(invocation: InvocationOnMock): BestResults[LogicalPlan] = {
        val context = invocation.getArgument[LogicalPlanningContext](2)
        val solveds = context.staticComponents.planningAttributes.solveds
        val cardinalities = context.staticComponents.planningAttributes.cardinalities
        val providedOrders = context.staticComponents.planningAttributes.providedOrders
        Seq(plan, sortedPlan).foreach { p =>
          solveds.set(p.id, SinglePlannerQuery.empty)
          cardinalities.set(p.id, 0.0)
          providedOrders.set(p.id, ProvidedOrder.empty)
        }
        BestResults(plan, Some(sortedPlan))
      }
    })
    strategy
  }

  def mockedMetrics: Metrics = newSimpleMetrics(hardcodedStatistics)

  private def newMockQueryGraphSolver =
    LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents.queryGraphSolver()

  def newMockedLogicalPlanningContext(
    planContext: PlanContext,
    metrics: Metrics = mockedMetrics,
    semanticTable: SemanticTable = newMockedSemanticTable,
    strategy: QueryGraphSolver = newMockQueryGraphSolver,
    notificationLogger: InternalNotificationLogger = devNullLogger,
    useErrorsOverWarnings: Boolean = false,
    costComparisonListener: CostComparisonListener = devNullListener
  ): LogicalPlanningContext = {
    val planningAttributes = PlanningAttributes.newAttributes
    newLogicalPlanningContextWithGivenAttributes(
      planContext,
      metrics,
      semanticTable,
      strategy,
      notificationLogger,
      useErrorsOverWarnings,
      planningAttributes,
      costComparisonListener
    )
  }

  def newMockedLogicalPlanningContextWithFakeAttributes(
    planContext: PlanContext,
    metrics: Metrics = mockedMetrics,
    semanticTable: SemanticTable = newMockedSemanticTable,
    strategy: QueryGraphSolver = newMockQueryGraphSolver,
    notificationLogger: InternalNotificationLogger = devNullLogger,
    useErrorsOverWarnings: Boolean = false,
    costComparisonListener: CostComparisonListener = devNullListener
  ): LogicalPlanningContext = {
    val planningAttributes = newStubbedPlanningAttributes
    newLogicalPlanningContextWithGivenAttributes(
      planContext,
      metrics,
      semanticTable,
      strategy,
      notificationLogger,
      useErrorsOverWarnings,
      planningAttributes,
      costComparisonListener
    )
  }

  private def newLogicalPlanningContextWithGivenAttributes(
    planContext: PlanContext,
    metrics: Metrics,
    semanticTable: SemanticTable,
    strategy: QueryGraphSolver,
    notificationLogger: InternalNotificationLogger,
    useErrorsOverWarnings: Boolean,
    planningAttributes: PlanningAttributes,
    costComparisonListener: CostComparisonListener
  ): LogicalPlanningContext = {
    val staticComponents = StaticComponents(
      planContext = planContext,
      notificationLogger = notificationLogger,
      planningAttributes = planningAttributes,
      logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen),
      queryGraphSolver = strategy,
      metrics = metrics,
      idGen = idGen,
      anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
      cancellationChecker = CancellationChecker.NeverCancelled,
      semanticTable = semanticTable,
      costComparisonListener = costComparisonListener,
      readOnly = false,
      labelInferenceStrategy = NoInference
    )

    val settings = Settings(
      executionModel = ExecutionModel.default,
      debugOptions = CypherDebugOptions.default,
      predicatesAsUnionMaxSize = config.predicatesAsUnionMaxSize(),
      useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping()
    )

    LogicalPlanningContext(staticComponents, settings)
  }

  def newMockedStatistics: InstrumentedGraphStatistics = mock[InstrumentedGraphStatistics]
  def hardcodedStatistics: GraphStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(statistics: InstrumentedGraphStatistics = newMockedStatistics): PlanContext = {
    val context = mock[PlanContext]
    doReturn(statistics, Nil: _*).when(context).statistics
    doReturn(Some(TokenIndexDescriptor(common.EntityType.NODE, IndexOrderCapability.BOTH)), Nil: _*).when(
      context
    ).nodeTokenIndex
    context
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan = {
    newMockedLogicalPlan(ids.toSet)
  }

  def newMockedLogicalPlan(planningAttributes: PlanningAttributes, ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.toSet, planningAttributes)

  def newMockedLogicalPlanWithProjections(planningAttributes: PlanningAttributes, ids: String*): LogicalPlan = {
    val projections = RegularQueryProjection(projections = ids.map(id => varFor(id) -> varFor(id)).toMap)
    val solved = RegularSinglePlannerQuery(
      queryGraph = QueryGraph.empty.addPatternNodes(ids.map(varFor): _*),
      horizon = projections
    )
    val res = FakeLeafPlan(ids.toSet)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, Cardinality(1))
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  def textSelections(id: String): Selections = {
    Selections(Set(Predicate(Set.empty, in(prop(id, "name"), listOfString("test")))))
  }

  def newMockedLogicalPlan(
    idNames: Set[String],
    planningAttributes: PlanningAttributes = PlanningAttributes.newAttributes,
    hints: Set[Hint] = Set[Hint](),
    selections: Selections = Selections()
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      QueryGraph.empty.addPatternNodes(idNames.map(varFor).toSeq: _*).addHints(hints).addSelections(selections)
    )
    newMockedLogicalPlanWithSolved(
      planningAttributes,
      idNames,
      solved,
      Cardinality(1)
    )
  }

  def newMockedLogicalPlanWithSolved(
    planningAttributes: PlanningAttributes = PlanningAttributes.newAttributes,
    idNames: Set[String],
    solved: PlannerQuery,
    cardinality: Cardinality = Cardinality(1),
    providedOrder: ProvidedOrder = ProvidedOrder.empty
  ): LogicalPlan = {
    val res = FakeLeafPlan(idNames)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, cardinality)
    planningAttributes.providedOrders.set(res.id, providedOrder)
    res
  }

  def newMockedLogicalPlanWithPatterns(
    planningAttributes: PlanningAttributes,
    idNames: Set[String],
    patterns: Set[PatternRelationship] = Set.empty,
    hints: Set[Hint] = Set[Hint](),
    selections: Selections = Selections()
  ): LogicalPlan = {
    val solved =
      RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(idNames.map(varFor).toSeq: _*).addPatternRelationships(
        patterns
      ).addHints(hints).addSelections(selections))
    newMockedLogicalPlanWithSolved(
      planningAttributes,
      idNames,
      solved,
      Cardinality(0)
    )
  }

  val config: CypherPlannerConfiguration = CypherPlannerConfiguration.defaults()
  def semanticFeatures: List[SemanticFeature] = Nil

  def buildSinglePlannerQuery(
    query: String,
    procedureLookup: Option[QualifiedName => ProcedureSignature] = None,
    functionLookup: Option[QualifiedName => Option[UserFunctionSignature]] = None
  ): SinglePlannerQuery = {
    buildPlannerQuery(query, procedureLookup, functionLookup) match {
      case pq: SinglePlannerQuery => pq
      case _                      => throw new IllegalArgumentException("This method cannot be used for UNION queries")
    }
  }

  lazy val cnfNormalizerTransformer = CNFNormalizerTest.getTransformer(semanticFeatures)

  lazy val pipeLine: Transformer[PlannerContext, BaseState, LogicalPlanState] =
    Parse(useAntlr = GraphDatabaseInternalSettings.cypher_parser_antlr_enabled.defaultValue()) andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true, semanticFeatures: _*) andThen
      AstRewriting() andThen
      RewriteProcedureCalls andThen
      SemanticAnalysis(warn = true, semanticFeatures: _*) andThen
      Namespacer andThen
      ProjectNamedPathsRewriter andThen
      rewriteEqualityToInPredicate andThen
      cnfNormalizerTransformer andThen
      collapseMultipleInPredicates andThen
      MoveBoundaryNodePredicates andThen
      CreatePlannerQuery(semanticFeatures.toSet) andThen
      NameDeduplication

  def buildPlannerQuery(
    query: String,
    procLookup: Option[QualifiedName => ProcedureSignature] = None,
    fcnLookup: Option[QualifiedName => Option[UserFunctionSignature]] = None
  ): PlannerQuery = {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "foo"),
      inputSignature = IndexedSeq.empty,
      deprecationInfo = None,
      outputSignature = Some(IndexedSeq(FieldSignature("all", CTInteger))),
      accessMode = ProcedureReadOnlyAccess,
      id = 42
    )
    val exceptionFactory = Neo4jCypherExceptionFactory(query, Some(pos))
    val procs: QualifiedName => ProcedureSignature = procLookup.getOrElse(_ => signature)
    val funcs: QualifiedName => Option[UserFunctionSignature] = fcnLookup.getOrElse(_ => None)
    val planContext = new TestSignatureResolvingPlanContext(procs, funcs)
    val state = LogicalPlanState(
      query,
      CostBasedPlannerName.default,
      PlanningAttributes.newAttributes,
      new AnonymousVariableNameGenerator()
    )
    val context = ContextHelper.create(
      cypherExceptionFactory = exceptionFactory,
      planContext = planContext,
      logicalPlanIdGen = idGen
    )
    val output = pipeLine.transform(state, context)

    output.query
  }
}
