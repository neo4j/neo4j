/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters.{namePatternPredicatePatternElements, _}
import org.neo4j.cypher.internal.compiler.v3_3.phases._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext, ProcedureSignature, _}
import org.neo4j.cypher.internal.compiler.v3_3.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer.newPlain
import org.neo4j.cypher.internal.frontend.v3_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.ir.v3_3._

import scala.collection.mutable

trait LogicalPlanningTestSupport extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val monitors = mock[Monitors]
  val parser = new CypherParser
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val astRewriter = new ASTRewriter(rewriterSequencer, literalExtraction = Never)
  val mockRel = newPatternRelationship("a", "b", "r")
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  def solvedWithEstimation(cardinality: Cardinality) = CardinalityEstimation.lift(PlannerQuery.empty, cardinality)

  def newPatternRelationship(start: IdName, end: IdName, rel: IdName, dir: SemanticDirection = SemanticDirection.OUTGOING, types: Seq[RelTypeName] = Seq.empty, length: PatternLength = SimplePatternLength) = {
    PatternRelationship(rel, (start, end), dir, types, length)
  }

  class SpyableMetricsFactory extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator) =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, evaluator)
    def newCostModel() =
      SimpleMetricsFactory.newCostModel()
    def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel =
      SimpleMetricsFactory.newQueryGraphCardinalityModel(statistics)
  }

  def newMockedQueryGraph = mock[QueryGraph]

  def newMetricsFactory = SimpleMetricsFactory

  def newSimpleMetrics(stats: GraphStatistics = newMockedGraphStatistics) = newMetricsFactory.newMetrics(stats, mock[ExpressionEvaluator])

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
    doReturn(plan).when(strategy).plan(any())(any())
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
                                      useErrorsOverWarnings: Boolean = false): LogicalPlanningContext =
    LogicalPlanningContext(planContext, LogicalPlanProducer(metrics.cardinality), metrics, semanticTable,
      strategy, QueryGraphSolverInput(Map.empty, cardinality, strictness),
      notificationLogger = notificationLogger, useErrorsOverWarnings = useErrorsOverWarnings,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping, config = QueryPlannerConfiguration.default)

  def newMockedStatistics = mock[GraphStatistics]
  def hardcodedStatistics = HardcodedGraphStatistics

  def newMockedPlanContext(implicit statistics: GraphStatistics = newMockedStatistics) = {
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    context
  }

  def newMockedLogicalPlanWithProjections(ids: String*): LogicalPlan = {
    val projections = RegularQueryProjection(projections = ids.map((id) => id -> varFor(id)).toMap)
    FakePlan(ids.map(IdName(_)).toSet)(CardinalityEstimation.lift(RegularPlannerQuery(
        horizon = projections,
        queryGraph = QueryGraph.empty.addPatternNodes(ids.map(IdName(_)): _*)
      ), Cardinality(0))
    )
  }

  def newMockedLogicalPlan(idNames: Set[IdName], cardinality: Cardinality = Cardinality(1), hints: Set[Hint] = Set[Hint]()): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(idNames.toSeq: _*).addHints(hints)
    FakePlan(idNames)(CardinalityEstimation.lift(RegularPlannerQuery(qg), cardinality))
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan =
    newMockedLogicalPlan(ids.map(IdName(_)).toSet)

  def newMockedLogicalPlanWithSolved(ids: Set[IdName], solved: PlannerQuery with CardinalityEstimation): LogicalPlan =
    FakePlan(ids)(solved)

  def newMockedLogicalPlanWithPatterns(ids: Set[IdName], patterns: Seq[PatternRelationship] = Seq.empty): LogicalPlan = {
    val qg = QueryGraph.empty.addPatternNodes(ids.toSeq: _*).addPatternRelationships(patterns)
    FakePlan(ids)(CardinalityEstimation.lift(RegularPlannerQuery(qg), Cardinality(0)))
  }

  val config = CypherCompilerConfiguration(
    queryCacheSize = 100,
    statsDivergenceThreshold = 0.5,
    queryPlanTTL = 1000,
    useErrorsOverWarnings = false,
    idpMaxTableSize = DefaultIDPSolverConfig.maxTableSize,
    idpIterationDuration = DefaultIDPSolverConfig.iterationDurationLimit,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    nonIndexedLabelWarningThreshold = 10000
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
    LogicalPlanState(input).copy(maybeStatement = Some(newStatement))
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
    val state = LogicalPlanState(query, None, CostBasedPlannerName.default)

    val context = ContextHelper.create(exceptionCreator = mkException, planContext = planContext)
    val output = pipeLine.transform(state, context)

    output.unionQuery
  }

  def identHasLabel(name: String, labelName: String): HasLabels = {
    val labelNameObj: LabelName = LabelName(labelName)_
    HasLabels(Variable(name)_, Seq(labelNameObj))_
  }
}

case class FakePlan(availableSymbols: Set[IdName])(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {
  def rhs = None
  def lhs = None
}
