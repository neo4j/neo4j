/**
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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.unnestApply
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, PlanContext}
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor

import scala.language.implicitConversions

case class SemanticPlan(plan: LogicalPlan, semanticTable: SemanticTable)

trait LogicalPlanningTestSupport2 extends CypherTestSupport with AstConstructionTestSupport {
  self: CypherFunSuite =>

  var kernelMonitors = new org.neo4j.kernel.monitoring.Monitors
  var monitors = new Monitors(kernelMonitors)
  var monitorTag = "compiler2.1"
  var parser = new CypherParser(monitors.newMonitor[ParserMonitor[Statement]](monitorTag))
  var semanticChecker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
  var astRewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag), shouldExtractParameters = false)
  var tokenResolver = new SimpleTokenResolver()
  var monitor = mock[PlanningMonitor]
  var strategy = new QueryPlanningStrategy() {
    def internalPlan(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan =
     planSingleQuery(query)
  }
  var queryGraphSolver: QueryGraphSolver = new CompositeQueryGraphSolver(
    new GreedyQueryGraphSolver(expandsOrJoins),
    new GreedyQueryGraphSolver(expandsOnly)
  )

  val realConfig = new RealLogicalPlanningConfiguration

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable = config.computeSemanticTable

    def metricsFactory = new MetricsFactory {
      def newCostModel(cardinality: Metrics.CardinalityModel) =
        (plan: LogicalPlan, c: QueryGraphCardinalityInput) =>
        config.costModel(cardinality)(plan)

      def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
        config.cardinalityModel(queryGraphCardinalityModel, semanticTable)

      def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable) =
        QueryGraphCardinalityModel.default(statistics, semanticTable)
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
        if (config.indexes((labelName, propertyKey)))
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
      val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
      val semanticState = semanticChecker.check(queryString, cleanedStatement)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, cleanedStatement, semanticState)
      val postRewriteSemanticState = semanticChecker.check(queryString, rewrittenStatement)
      val semanticTable = SemanticTable(types = postRewriteSemanticState.typeTable)
      Planner.rewriteStatement(rewrittenStatement, postRewriteSemanticState.scopeTree, semanticTable, postConditions) match {
        case (ast: Query, newTable) =>
          tokenResolver.resolve(ast)(newTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics, newTable)
          val context = LogicalPlanningContext(planContext, metrics, newTable, queryGraphSolver, QueryGraphCardinalityInput(Map.empty, Cardinality(1)))
          val plannerQuery = unionQuery.queries.head
          val resultPlan = strategy.internalPlan(plannerQuery)(context)
          SemanticPlan(resultPlan.endoRewrite(unnestApply), newTable)
      }
    }

    def getLogicalPlanFor(queryString: String): (LogicalPlan, SemanticTable) = {
      val parsedStatement = parser.parse(queryString)
      val semanticState = semanticChecker.check(queryString, parsedStatement)
      val (rewrittenStatement, _, postConditions) = astRewriter.rewrite(queryString, parsedStatement, semanticState)

      Planner.rewriteStatement(rewrittenStatement, semanticState.scopeTree, semanticTable, postConditions) match {
        case (ast: Query, newTable) =>
          tokenResolver.resolve(ast)(newTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics, newTable)
          val context = LogicalPlanningContext(planContext, metrics, newTable, queryGraphSolver, QueryGraphCardinalityInput(Map.empty, Cardinality(1)))
          (strategy.plan(unionQuery)(context), newTable)
      }
    }

    def withLogicalPlanningContext[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(config.graphStatistics, semanticTable)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        cardinalityInput = QueryGraphCardinalityInput(Map.empty, Cardinality(1))
      )
      f(config, ctx)
    }
  }

  def fakeLogicalPlanFor(id: String*): FakePlan = FakePlan(id.map(IdName(_)).toSet)(PlannerQuery.empty)

  def planFor(queryString: String): SemanticPlan = new given().planFor(queryString)

  def planTableWith(plans: LogicalPlan*)(implicit ctx: LogicalPlanningContext) =
    plans.foldLeft(ctx.strategy.emptyPlanTable)(_ + _)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  implicit def idName(name: String): IdName = IdName(name)

  implicit def lazyLabel(label: String)(implicit plan: SemanticPlan): LazyLabel =
    LazyLabel(LabelName(label)(_))(plan.semanticTable)

  implicit def propertyKeyId(label: String)(implicit plan: SemanticPlan): PropertyKeyId =
    plan.semanticTable.resolvedPropertyKeyNames(label)
}
