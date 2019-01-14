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

import org.neo4j.csv.reader.Configuration
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v3_4.phases._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter.unnestApply
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{LogicalPlanProducer, devNullListener}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LogicalPlanningContext, _}
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer.newPlain
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.{GraphStatistics, IDPPlannerName, IndexDescriptor}
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attribute, Attributes}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, PropertyKeyId}
import org.neo4j.cypher.internal.v3_4.expressions.PatternExpression
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.helpers.collection.Visitable
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.language.reflectiveCalls
import scala.reflect.ClassTag

trait LogicalPlanningTestSupport2 extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  var parser = new CypherParser
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  var astRewriter = new ASTRewriter(rewriterSequencer, literalExtraction = Never, getDegreeRewriting = true)
  final var planner = new QueryPlanner()
  var queryGraphSolver: QueryGraphSolver = new IDPQueryGraphSolver(SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]), cartesianProductsOrValueJoins, mock[IDPQueryGraphSolverMonitor])
  val cypherCompilerConfig = CypherCompilerConfiguration(
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
  val realConfig = new RealLogicalPlanningConfiguration(cypherCompilerConfig)

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable = config.updateSemanticTableWithTokens(SemanticTable())

    def metricsFactory = new MetricsFactory {
      def newCostModel(ignore: CypherCompilerConfiguration) =
        (plan: LogicalPlan, input: QueryGraphSolverInput, cardinalities: Cardinalities) => config.costModel()((plan, input, cardinalities))

      def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator) =
        config.cardinalityModel(queryGraphCardinalityModel, mock[ExpressionEvaluator])

      def newQueryGraphCardinalityModel(statistics: GraphStatistics) = QueryGraphCardinalityModel.default(statistics)
    }

    def table = Map.empty[PatternExpression, QueryGraph]

    def planContext = new NotImplementedPlanContext {
      override def statistics: GraphStatistics =
        config.graphStatistics

      override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val label = config.labelsById(labelId)
        config.indexes.filter(p => p._1 == label).flatMap(p => indexGet(p._1, p._2)).iterator
      }

      override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val label = config.labelsById(labelId)
        config.uniqueIndexes.filter(p => p._1 == label).flatMap(p => uniqueIndexGet(p._1, p._2)).iterator
      }

      override def uniqueIndexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
        if (config.uniqueIndexes((labelName, propertyKeys)))
          Some(IndexDescriptor(
            semanticTable.resolvedLabelNames(labelName).id,
            propertyKeys.map(semanticTable.resolvedPropertyKeyNames(_).id)
          ))
        else
          None

      override def indexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
        if (config.indexes((labelName, propertyKeys)) || config.uniqueIndexes((labelName, propertyKeys)))
          Some(IndexDescriptor(
            semanticTable.resolvedLabelNames(labelName).id,
            propertyKeys.map(semanticTable.resolvedPropertyKeyNames(_).id)
          ))
        else
          None

      override def indexExistsForLabel(labelName: String): Boolean =
        config.indexes.exists(_._1 == labelName) || config.uniqueIndexes.exists(_._1 == labelName)

      override def getOptPropertyKeyId(propertyKeyName: String) =
        semanticTable.resolvedPropertyKeyNames.get(propertyKeyName).map(_.id)

      override def getOptLabelId(labelName: String): Option[Int] =
        semanticTable.resolvedLabelNames.get(labelName).map(_.id)

      override def getOptRelTypeId(relType: String): Option[Int] =
        semanticTable.resolvedRelTypeNames.get(relType).map(_.id)
    }

    def pipeLine(): Transformer[CompilerContext, BaseState, LogicalPlanState] =
      Parsing andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true) andThen
      AstRewriting(newPlain, literalExtraction = Never) andThen
      RewriteProcedureCalls andThen
      Namespacer andThen
      transitiveClosure andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      ResolveTokens andThen
      CreatePlannerQuery andThen
      OptionalMatchRemover andThen
      QueryPlanner().adds(CompilationContains[LogicalPlan]) andThen
      Do[CompilerContext, LogicalPlanState, LogicalPlanState]((state, context) => removeApply(state, context, state.solveds, new Attributes(idGen, state.cardinalities)))

    // This fakes pattern expression naming for testing purposes
    // In the actual code path, this renaming happens as part of planning
    //
    // cf. QueryPlanningStrategy
    //
    private def namePatternPredicates(input: LogicalPlanState, context: CompilerContext): LogicalPlanState = {
      val newStatement = input.statement.endoRewrite(namePatternPredicatePatternElements)
      input.copy(maybeStatement = Some(newStatement))
    }

    private def removeApply(input: LogicalPlanState, context: CompilerContext, solveds: Solveds, attributes: Attributes): LogicalPlanState = {
      val newPlan = input.logicalPlan.endoRewrite(fixedPoint(unnestApply(solveds, attributes)))
      input.copy(maybeLogicalPlan = Some(newPlan))
    }

    def getLogicalPlanFor(queryString: String): (Option[PeriodicCommit], LogicalPlan, SemanticTable, Solveds, Cardinalities) = {
      val mkException = new SyntaxExceptionCreator(queryString, Some(pos))
      val metrics = metricsFactory.newMetrics(planContext.statistics, mock[ExpressionEvaluator], cypherCompilerConfig)
      def context = ContextHelper.create(planContext = planContext,
        exceptionCreator = mkException,
        queryGraphSolver = queryGraphSolver,
        metrics = metrics,
        config = cypherCompilerConfig,
        logicalPlanIdGen = idGen
      )

      val state = InitialState(queryString, None, IDPPlannerName)
      val output = pipeLine().transform(state, context)
      val logicalPlan = output.logicalPlan.asInstanceOf[ProduceResult].source
      (output.periodicCommit, logicalPlan, output.semanticTable(), output.solveds, output.cardinalities)
    }

    def estimate(qg: QueryGraph, input: QueryGraphSolverInput = QueryGraphSolverInput.empty) =
      metricsFactory.
        newMetrics(config.graphStatistics, mock[ExpressionEvaluator], cypherCompilerConfig).
        queryGraphCardinalityModel(qg, input, semanticTable)

    def withLogicalPlanningContext[T](f: (C, LogicalPlanningContext, Solveds, Cardinalities) => T): T = {
      val metrics = metricsFactory.newMetrics(config.graphStatistics, mock[ExpressionEvaluator], cypherCompilerConfig)
      val solveds = new Solveds
      val cardinalities = new Cardinalities
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, solveds, cardinalities, idGen)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        logicalPlanProducer = logicalPlanProducer,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        input = QueryGraphSolverInput.empty,
        notificationLogger = devNullLogger,
        costComparisonListener = devNullListener
      )
      f(config, ctx, solveds, cardinalities)
    }


    def withLogicalPlanningContextWithFakeAttributes[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(config.graphStatistics, mock[ExpressionEvaluator], cypherCompilerConfig)
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, new StubSolveds, new StubCardinalities, idGen)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        logicalPlanProducer = logicalPlanProducer,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        input = QueryGraphSolverInput.empty,
        notificationLogger = devNullLogger,
        costComparisonListener = devNullListener
      )
      f(config, ctx)
    }
  }

  def set[T](plan: LogicalPlan, attribute: Attribute[T], t: T): LogicalPlan = {
    attribute.set(plan.id, t)
    plan
  }

  def setC(plan: LogicalPlan, cardinalities: Cardinalities, c: Cardinality): LogicalPlan = {
    cardinalities.set(plan.id, c)
    plan
  }

  def fakeLogicalPlanFor(id: String*): FakePlan = FakePlan(id.toSet)

  def fakeLogicalPlanFor(solveds: Solveds, cardinalities: Cardinalities, id: String*): FakePlan = {
    val res = FakePlan(id.toSet)
    solveds.set(res.id, PlannerQuery.empty)
    cardinalities.set(res.id, 0.0)
    res
  }

  def planFor(queryString: String): (Option[PeriodicCommit], LogicalPlan, SemanticTable, Solveds, Cardinalities) =
    new given().getLogicalPlanFor(queryString)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  class givenPlanWithMinimumCardinalityEnabled
    extends StubbedLogicalPlanningConfiguration(RealLogicalPlanningConfiguration(cypherCompilerConfig.copy(planWithMinimumCardinalityEstimates = true)))

  class fromDbStructure(dbStructure: Visitable[DbStructureVisitor])
    extends DelegatingLogicalPlanningConfiguration(DbStructureLogicalPlanningConfiguration(cypherCompilerConfig)(dbStructure))

  implicit def propertyKeyId(label: String)(implicit semanticTable: SemanticTable): PropertyKeyId =
    semanticTable.resolvedPropertyKeyNames(label)

  def using[T <: LogicalPlan](implicit tag: ClassTag[T]): BeMatcher[LogicalPlan] = new BeMatcher[LogicalPlan] {
    import org.neo4j.cypher.internal.util.v3_4.Foldable._
    override def apply(actual: LogicalPlan): MatchResult = {
      val matches = actual.treeFold(false) {
        case lp if tag.runtimeClass.isInstance(lp) => acc => (true, None)
      }
      MatchResult(
        matches = matches,
        rawFailureMessage = s"Plan should use ${tag.runtimeClass.getSimpleName}",
        rawNegatedFailureMessage = s"Plan should not use ${tag.runtimeClass.getSimpleName}")
    }
  }
}
