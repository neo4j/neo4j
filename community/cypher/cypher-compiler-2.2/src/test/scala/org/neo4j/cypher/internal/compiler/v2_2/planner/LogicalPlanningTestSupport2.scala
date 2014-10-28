/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.{normalizeWithClauses, normalizeReturnClauses}
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.unnestEmptyApply
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, PlanContext}
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor
import org.scalatest.matchers._

trait BeLikeMatcher {
  class BeLike(pf: PartialFunction[Object, Unit]) extends Matcher[Object] {

    def apply(left: Object) = {
      MatchResult(
        pf.isDefinedAt(left),
        s"""$left did not match the partial function""",
        s"""$left matched the partial function"""
      )
    }
  }

  def beLike(pf: PartialFunction[Object, Unit]) = new BeLike(pf)
}

object BeLikeMatcher extends BeLikeMatcher

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
  var queryGraphSolver = new GreedyQueryGraphSolver()

  val realConfig = new RealLogicalPlanningConfiguration

  trait LogicalPlanningConfiguration {
    def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable): Metrics.CardinalityModel
    def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost]
    def graphStatistics: GraphStatistics
    def indexes: Set[(String, String)]
    def uniqueIndexes: Set[(String, String)]
    def labelCardinality: Map[String, Cardinality]
    def knownLabels: Set[String]
    def qg: QueryGraph

    class given extends StubbedLogicalPlanningConfiguration(this)

    def planFor(queryString: String): SemanticPlan =
      LogicalPlanningEnvironment(this).planFor(queryString)

    def getLogicalPlanFor(query: String): (LogicalPlan, SemanticTable) =
      LogicalPlanningEnvironment(this).getLogicalPlanFor(query)

    def withLogicalPlanningContext[T](f: LogicalPlanningContext => T): T = {
      LogicalPlanningEnvironment(this).withLogicalPlanningContext(f)
    }

    protected def mapCardinality(pf: PartialFunction[LogicalPlan, Double]): PartialFunction[LogicalPlan, Cardinality] = pf.andThen(Cardinality.apply)
  }

  case class RealLogicalPlanningConfiguration() extends LogicalPlanningConfiguration {
    def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable) = {
      val model = new StatisticsBackedCardinalityModel(queryGraphCardinalityModel)
      ({
        case (plan: LogicalPlan) => model(plan)
      })
    }
    def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost] = {
      val model = new CardinalityCostModel(cardinality)
      ({
        case (plan: LogicalPlan) => model(plan)
      })
    }
    def graphStatistics: GraphStatistics =
      HardcodedGraphStatistics
    def indexes = Set.empty
    def uniqueIndexes = Set.empty
    def labelCardinality = Map.empty
    def knownLabels = Set.empty
    def qg: QueryGraph = ???
  }

  class StubbedLogicalPlanningConfiguration(parent: LogicalPlanningConfiguration) extends LogicalPlanningConfiguration {
    var knownLabels: Set[String] = Set.empty
    var cardinality: PartialFunction[LogicalPlan, Cardinality] = PartialFunction.empty
    var cost: PartialFunction[LogicalPlan, Cost] = PartialFunction.empty
    var selectivity: PartialFunction[Expression, Selectivity] = PartialFunction.empty
    var labelCardinality: Map[String, Cardinality] = Map.empty
    var statistics = null
    var qg: QueryGraph = null

    var indexes: Set[(String, String)] = Set.empty
    var uniqueIndexes: Set[(String, String)] = Set.empty
    def indexOn(label: String, property: String) {
      indexes = indexes + (label -> property)
    }
    def uniqueIndexOn(label: String, property: String) {
      uniqueIndexes = uniqueIndexes + (label -> property)
    }

    def costModel(cardinality: Metrics.CardinalityModel) =
      cost.orElse(parent.costModel(cardinality))

    def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable): Metrics.CardinalityModel = {
      val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
        case (name: String, cardinality: Cardinality) =>
          semanticTable.resolvedLabelIds(name) -> cardinality
      }
      val labelScanCardinality: PartialFunction[LogicalPlan, Cardinality] = {
        case NodeByLabelScan(_, Right(labelId), _) if labelIdCardinality.contains(labelId) =>
          labelIdCardinality(labelId)
      }

      labelScanCardinality
        .orElse(cardinality)
        .orElse(PartialFunction(parent.cardinalityModel(queryGraphCardinalityModel, semanticTable)))
    }

    def graphStatistics: GraphStatistics =
      Option(statistics).getOrElse(parent.graphStatistics)
  }

  case class LogicalPlanningEnvironment(config: LogicalPlanningConfiguration) {
    lazy val semanticTable: SemanticTable = {
      val table = SemanticTable()
      def addLabelIfUnknown(labelName: String) =
        if (!table.resolvedLabelIds.contains(labelName))
          table.resolvedLabelIds.put(labelName, LabelId(table.resolvedLabelIds.size))

      config.indexes.foreach { case (label, property) =>
        addLabelIfUnknown(label)
        table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
      }
      config.uniqueIndexes.foreach { case (label, property) =>
        addLabelIfUnknown(label)
        table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
      }
      config.labelCardinality.keys.foreach(addLabelIfUnknown)
      config.knownLabels.foreach(addLabelIfUnknown)
      table
    }

    def metricsFactory = new MetricsFactory {
      def newCostModel(cardinality: Metrics.CardinalityModel) =
        config.costModel(cardinality)
      def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
        config.cardinalityModel(queryGraphCardinalityModel, semanticTable)

      def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable) =
        QueryGraphCardinalityModel.default(statistics, semanticTable)

      def newCandidateListCreator(): (Seq[LogicalPlan]) => CandidateList = CandidateList.apply
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
      def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] = ???
      def checkRelIndex(idxName: String): Unit = ???
      def getOrCreateFromSchemaState[T](key: Any, f: => T): T = ???
      def getRelTypeName(id: Int): String = ???
      def getRelTypeId(relType: String): Int = ???
      def getLabelName(id: Int): String = ???
      def getPropertyKeyId(propertyKeyName: String): Int = ???
      def getPropertyKeyName(id: Int): String = ???
      def getLabelId(labelName: String): Int = ???
      def getLastCommittedTransactionId: Long = 0
    }

    def planFor(queryString: String): SemanticPlan = {
      val parsedStatement = parser.parse(queryString)
      val cleanedStatement: Statement = parsedStatement.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
      val semanticState = semanticChecker.check(queryString, cleanedStatement)
      val (rewrittenStatement, _) = astRewriter.rewrite(queryString, cleanedStatement, semanticState)
      val postRewriteSemanticState = semanticChecker.check(queryString, rewrittenStatement)
      val semanticTable = SemanticTable(types = postRewriteSemanticState.typeTable)
      val plannerQuery: LogicalPlan = Planner.rewriteStatement(rewrittenStatement, postRewriteSemanticState.scopeTree) match {
        case ast: Query =>
          tokenResolver.resolve(ast)(semanticTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)
          val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
          val plannerQuery = unionQuery.queries.head
          strategy.internalPlan(plannerQuery)(context)
      }

      SemanticPlan(plannerQuery.endoRewrite(unnestEmptyApply), semanticTable)
    }

    def getLogicalPlanFor(queryString: String): (LogicalPlan, SemanticTable) = {
      val parsedStatement = parser.parse(queryString)
      val semanticState = semanticChecker.check(queryString, parsedStatement)
      val (rewrittenStatement, _) = astRewriter.rewrite(queryString, parsedStatement, semanticState)
      semanticChecker.check(queryString, rewrittenStatement)

      Planner.rewriteStatement(rewrittenStatement, semanticState.scopeTree) match {
        case ast: Query =>
          tokenResolver.resolve(ast)(semanticTable, planContext)
          val unionQuery = ast.asUnionQuery
          val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)
          val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
          (strategy.plan(unionQuery)(context), semanticTable)
      }
    }

    def withLogicalPlanningContext[T](f: LogicalPlanningContext => T): T = {
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        metrics = metricsFactory.newMetrics(config.graphStatistics, semanticTable),
        semanticTable = semanticTable,
        strategy = queryGraphSolver
      )
      f(ctx)
    }
  }

  def fakeLogicalPlanFor(id: String*): FakePlan = FakePlan(id.map(IdName(_)).toSet)(PlannerQuery.empty)

  def planFor(queryString: String): SemanticPlan = new given().planFor(queryString)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  implicit def idName(name: String): IdName = IdName(name)
  implicit def labelId(label: String)(implicit plan: SemanticPlan): LabelId =
    plan.semanticTable.resolvedLabelIds(label)
  implicit def propertyKeyId(label: String)(implicit plan: SemanticPlan): PropertyKeyId =
    plan.semanticTable.resolvedPropertyKeyNames(label)
}
