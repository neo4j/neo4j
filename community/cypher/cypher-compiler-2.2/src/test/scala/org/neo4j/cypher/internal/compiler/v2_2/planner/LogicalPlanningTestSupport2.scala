/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.greedy.{GreedyPlanTable, GreedyQueryGraphSolver, expandsOnly, expandsOrJoins}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.{LogicalPlanRewriter, unnestApply}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.helpers.collection.Visitable
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor

import scala.language.reflectiveCalls

case class SemanticPlan(plan: LogicalPlan, semanticTable: SemanticTable)

trait LogicalPlanningTestSupport2 extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  self: CypherFunSuite =>

  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(0))
  var parser = new CypherParser(mock[ParserMonitor[Statement]])
  var semanticChecker = new SemanticChecker(mock[SemanticCheckMonitor])
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  var astRewriter = new ASTRewriter(rewriterSequencer, mock[AstRewritingMonitor], shouldExtractParameters = false)
  var tokenResolver = new SimpleTokenResolver()
  var monitor = mock[PlanningMonitor]
  val planRewriter = LogicalPlanRewriter(rewriterSequencer)
  final var planner = new DefaultQueryPlanner(planRewriter) {
    def internalPlan(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan =
      planSingleQuery(query)
  }
  var queryGraphSolver: QueryGraphSolver = new CompositeQueryGraphSolver(
    new GreedyQueryGraphSolver(expandsOrJoins),
    new GreedyQueryGraphSolver(expandsOnly)
  )

  val realConfig = new RealLogicalPlanningConfiguration

  def solvedWithEstimation(cardinality: Cardinality) = CardinalityEstimation.lift(PlannerQuery.empty, cardinality)

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable = config.computeSemanticTable

    def metricsFactory = new MetricsFactory {
      def newCostModel() =
        (plan: LogicalPlan, input: QueryGraphSolverInput) => config.costModel()(plan -> input)

      def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
        config.cardinalityModel(queryGraphCardinalityModel)

      def newQueryGraphCardinalityModel(statistics: GraphStatistics) = QueryGraphCardinalityModel.default(statistics)
    }

    def table = Map.empty[PatternExpression, QueryGraph]

    def planContext = new PlanContext {
      def statistics: GraphStatistics =
        config.graphStatistics

      def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
        if (config.uniqueIndexes((labelName, propertyKey)))
          Some(new IndexDescriptor(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None

      def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] = {
        if (config.uniqueIndexes((labelName, propertyKey)))
          Some(new UniquenessConstraint(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None
      }

      def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
        if (config.indexes((labelName, propertyKey)) || config.uniqueIndexes((labelName, propertyKey)))
          Some(new IndexDescriptor(
            semanticTable.resolvedLabelIds(labelName).id,
            semanticTable.resolvedPropertyKeyNames(propertyKey).id
          ))
        else
          None

      def getOptPropertyKeyId(propertyKeyName: String) =
        semanticTable.resolvedPropertyKeyNames.get(propertyKeyName).map(_.id)

      def getOptLabelId(labelName: String): Option[Int] =
        semanticTable.resolvedLabelIds.get(labelName).map(_.id)

      def getOptRelTypeId(relType: String): Option[Int] =
        semanticTable.resolvedRelTypeNames.get(relType).map(_.id)

      def checkNodeIndex(idxName: String): Unit = ???

      def checkRelIndex(idxName: String): Unit = ???

      def getOrCreateFromSchemaState[T](key: Any, f: => T): T = ???

      def getRelTypeName(id: Int): String = ???

      def getRelTypeId(relType: String): Int = ???

      def getLabelName(id: Int): String = ???

      def getPropertyKeyId(propertyKeyName: String): Int = ???

      def getPropertyKeyName(id: Int): String = ???

      def getLabelId(labelName: String): Int = ???

      def txIdProvider: () => Long = ???
    }

    def planFor(queryString: String): SemanticPlan = {
      val parsedStatement = parser.parse(queryString)
      val mkException = new SyntaxExceptionCreator(queryString, Some(pos))
      val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
      val semanticState = semanticChecker.check(queryString, cleanedStatement, mkException)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, cleanedStatement, semanticState)
      val postRewriteSemanticState = semanticChecker.check(queryString, rewrittenStatement, mkException)
      val semanticTable = SemanticTable(types = postRewriteSemanticState.typeTable)
      CostBasedPipeBuilder.rewriteStatement(rewrittenStatement, postRewriteSemanticState.scopeTree, semanticTable, rewriterSequencer, semanticChecker, postConditions, mock[AstRewritingMonitor]) match {
        case (ast: Query, newTable) =>
          tokenResolver.resolve(ast)(newTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics)
          val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
          val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, newTable, queryGraphSolver, QueryGraphSolverInput.empty)
          val plannerQuery = unionQuery.queries.head
          val resultPlan = planner.internalPlan(plannerQuery)(context)
          SemanticPlan(resultPlan.endoRewrite(unnestApply), newTable)
      }
    }

    def getLogicalPlanFor(queryString: String): (LogicalPlan, SemanticTable) = {
      val parsedStatement = parser.parse(queryString)
      val mkException = new SyntaxExceptionCreator(queryString, Some(pos))
      val semanticState = semanticChecker.check(queryString, parsedStatement, mkException)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, parsedStatement, semanticState)

      CostBasedPipeBuilder.rewriteStatement(rewrittenStatement, semanticState.scopeTree, semanticTable, rewriterSequencer, semanticChecker, postConditions, mock[AstRewritingMonitor]) match {
        case (ast: Query, newTable) =>
          tokenResolver.resolve(ast)(newTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics)
          val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
          val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, newTable, queryGraphSolver, QueryGraphSolverInput.empty)
          (planner.plan(unionQuery)(context), newTable)
      }
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
        input = QueryGraphSolverInput.empty
      )
      f(config, ctx)
    }
  }

  def fakeLogicalPlanFor(id: String*): FakePlan = FakePlan(id.map(IdName(_)).toSet)(solved)

  def planFor(queryString: String): SemanticPlan = new given().planFor(queryString)

  def greedyPlanTableWith(plans: LogicalPlan*)(implicit ctx: LogicalPlanningContext) =
    plans.foldLeft(GreedyPlanTable.empty)(_ + _)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  class fromDbStructure(dbStructure: Visitable[DbStructureVisitor])
    extends DelegatingLogicalPlanningConfiguration(DbStructureLogicalPlanningConfiguration(dbStructure))

  implicit def lazyLabel(label: String)(implicit plan: SemanticPlan): LazyLabel =
    LazyLabel(LabelName(label)(_))(plan.semanticTable)

  implicit def propertyKeyId(label: String)(implicit plan: SemanticPlan): PropertyKeyId =
    plan.semanticTable.resolvedPropertyKeyNames(label)
}
