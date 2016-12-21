/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationState, Context, LateAstRewriting}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp.{IDPQueryGraphSolver, IDPQueryGraphSolverMonitor, SingleComponentPlanner, cartesianProductsOrValueJoins}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.rewriter.unnestApply
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{LogicalPlanningContext, _}
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.frontend.v3_2.{Foldable, PropertyKeyId, SemanticTable, _}
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, IdName, PeriodicCommit}
import org.neo4j.helpers.collection.Visitable
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.language.reflectiveCalls
import scala.reflect.ClassTag

case class SemanticPlan(plan: LogicalPlan, semanticTable: SemanticTable)

trait LogicalPlanningTestSupport2 extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(0))
  var parser = new CypherParser
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  var astRewriter = new ASTRewriter(rewriterSequencer, shouldExtractParameters = false)
  final var planner = new QueryPlanner() {
    def internalPlan(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan =
      planSingleQuery(query)
  }
  var queryGraphSolver: QueryGraphSolver = new IDPQueryGraphSolver(SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]), cartesianProductsOrValueJoins, mock[IDPQueryGraphSolverMonitor])
  val realConfig = new RealLogicalPlanningConfiguration

  def solvedWithEstimation(cardinality: Cardinality) = CardinalityEstimation.lift(PlannerQuery.empty, cardinality)

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable = config.updateSemanticTableWithTokens(SemanticTable())

    def metricsFactory = new MetricsFactory {
      def newCostModel() =
        (plan: LogicalPlan, input: QueryGraphSolverInput) => config.costModel()(plan -> input)

      def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
        config.cardinalityModel(queryGraphCardinalityModel)

      def newQueryGraphCardinalityModel(statistics: GraphStatistics) = QueryGraphCardinalityModel.default(statistics)
    }

    def table = Map.empty[PatternExpression, QueryGraph]

    def planContext = new NotImplementedPlanContext {
      override def statistics: GraphStatistics =
        config.graphStatistics

      override def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
        if (config.uniqueIndexes((labelName, propertyKey)))
          Some(new IndexDescriptor(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None

      override def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] = {
        if (config.uniqueIndexes((labelName, propertyKey)))
          Some(new UniquenessConstraint(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None
      }

      override def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
        if (config.indexes((labelName, propertyKey)) || config.uniqueIndexes((labelName, propertyKey)))
          Some(new IndexDescriptor(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None

      override def hasIndexRule(labelName: String): Boolean =
        config.indexes.exists(_._1 == labelName) || config.uniqueIndexes.exists(_._1 == labelName)

      override def getOptPropertyKeyId(propertyKeyName: String) =
        semanticTable.resolvedPropertyKeyNames.get(propertyKeyName).map(_.id)

      override def getOptLabelId(labelName: String): Option[Int] =
        semanticTable.resolvedLabelIds.get(labelName).map(_.id)

      override def getOptRelTypeId(relType: String): Option[Int] =
        semanticTable.resolvedRelTypeNames.get(relType).map(_.id)
    }

    private def context = Context(null, null, null, planContext, null, null, mock[AstRewritingMonitor], null, null, null, null)

    private val pipeLine =
      Namespacer andThen rewriteEqualityToInPredicate andThen CNFNormalizer andThen LateAstRewriting andThen ResolveTokens

    def planFor(queryString: String): SemanticPlan = {

      val parsedStatement = parser.parse(queryString)
      val mkException = new SyntaxExceptionCreator(queryString, Some(pos))
      val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
      val semanticState = SemanticChecker.check(cleanedStatement, mkException)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, cleanedStatement, semanticState)

      val state = CompilationState(queryString, None, "", Some(rewrittenStatement), Some(semanticState))
      val output = pipeLine.transform(state, context)

      val ast = output.statement.asInstanceOf[Query]

      val unionQuery = toUnionQuery(ast, output.semanticTable)
      val metrics = metricsFactory.newMetrics(planContext.statistics)
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
      val planningContext = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, output.semanticTable, queryGraphSolver, QueryGraphSolverInput.empty, notificationLogger = devNullLogger)
      val plannerQuery = unionQuery.queries.head
      val resultPlan = planner.internalPlan(plannerQuery)(planningContext)
      SemanticPlan(resultPlan.endoRewrite(fixedPoint(unnestApply)), output.semanticTable)
    }

    def getLogicalPlanFor(queryString: String): (Option[PeriodicCommit], LogicalPlan, SemanticTable) = {
      val parsedStatement = parser.parse(queryString)
      val mkException = new SyntaxExceptionCreator(queryString, Some(pos))
      val semanticState = SemanticChecker.check(parsedStatement, mkException)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, parsedStatement, semanticState)

      val state = CompilationState(queryString, None, "", Some(rewrittenStatement), Some(semanticState))
      val output = pipeLine.transform(state, context)
      val table = output.semanticTable
      config.updateSemanticTableWithTokens(table)
      val ast = output.statement.asInstanceOf[Query]

      ResolveTokens.resolve(ast)(output.semanticTable, planContext)
      val unionQuery = toUnionQuery(ast, semanticTable)
      val metrics = metricsFactory.newMetrics(planContext.statistics)
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
      val logicalPlanningContext = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, table, queryGraphSolver, QueryGraphSolverInput.empty, notificationLogger = devNullLogger)
      val (periodicCommit, plan) = planner.plan(unionQuery)(logicalPlanningContext)
      (periodicCommit, plan, table)
    }

    def estimate(qg: QueryGraph, input: QueryGraphSolverInput = QueryGraphSolverInput.empty) =
      metricsFactory.newMetrics(config.graphStatistics).queryGraphCardinalityModel(qg, input, semanticTable)

    def withLogicalPlanningContext[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(config.graphStatistics)
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        logicalPlanProducer = logicalPlanProducer,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        input = QueryGraphSolverInput.empty,
        notificationLogger = devNullLogger
      )
      f(config, ctx)
    }
  }

  def fakeLogicalPlanFor(id: String*): FakePlan = FakePlan(id.map(IdName(_)).toSet)(solved)

  def planFor(queryString: String): SemanticPlan = new given().planFor(queryString)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  class fromDbStructure(dbStructure: Visitable[DbStructureVisitor])
    extends DelegatingLogicalPlanningConfiguration(DbStructureLogicalPlanningConfiguration(dbStructure))

  implicit def propertyKeyId(label: String)(implicit plan: SemanticPlan): PropertyKeyId =
    plan.semanticTable.resolvedPropertyKeyNames(label)

  def using[T <: LogicalPlan](implicit tag: ClassTag[T]): BeMatcher[LogicalPlan] = new BeMatcher[LogicalPlan] {
    import Foldable._
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
