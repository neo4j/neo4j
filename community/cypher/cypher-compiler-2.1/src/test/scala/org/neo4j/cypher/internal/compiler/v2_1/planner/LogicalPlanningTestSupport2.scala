/**
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.commons.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.spi.{GraphStatistics, PlanContext}
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

case class SemanticPlan(plan: QueryPlan, semanticTable: SemanticTable)

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
    def internalPlan(query: PlannerQuery)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph], leafPlan: Option[QueryPlan] = None): QueryPlan =
     planSingleQuery(query)
  }
  var queryGraphSolver = new GreedyQueryGraphSolver()

  val realConfig = new RealLogicalPlanningConfiguration

  trait LogicalPlanningConfiguration {
    def selectivityModel(statistics: GraphStatistics, semanticTable: SemanticTable): PartialFunction[Expression, Multiplier]
    def cardinalityModel(statistics: GraphStatistics, selectivity: SelectivityModel, semanticTable: SemanticTable): PartialFunction[LogicalPlan, Cardinality]
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

    def withLogicalPlanningContext[T](f: (LogicalPlanningContext, Map[PatternExpression, QueryGraph]) => T): T = {
      LogicalPlanningEnvironment(this).withLogicalPlanningContext(f)
    }

    protected def mapCardinality(pf:PartialFunction[LogicalPlan, Double]): PartialFunction[LogicalPlan, Cardinality] = pf.andThen(Cardinality.apply)
  }

  case class RealLogicalPlanningConfiguration() extends LogicalPlanningConfiguration {
    def cardinalityModel(statistics: GraphStatistics, selectivity: SelectivityModel, semanticTable: SemanticTable) = {
      val model = new StatisticsBackedCardinalityModel(statistics, selectivity)(semanticTable)
      ({
        case (plan: LogicalPlan) => model(plan)
      })
    }
    def selectivityModel(statistics: GraphStatistics, semanticTable: SemanticTable) = {
      val model = new StatisticsBasedSelectivityModel(statistics)(semanticTable)
      ({
        case (expr: Expression) => model(expr)
      })
    }
    def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost] = {
      val model = new SimpleCostModel(cardinality)
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
    var selectivity: PartialFunction[Expression, Multiplier] = PartialFunction.empty
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

    def cardinalityModel(statistics: GraphStatistics, selectivity: Metrics.SelectivityModel, semanticTable: SemanticTable) = {
      val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
        case (name: String, cardinality: Cardinality) =>
          semanticTable.resolvedLabelIds(name) -> cardinality
      }
      val labelScanCardinality: PartialFunction[LogicalPlan, Cardinality] = {
        case NodeByLabelScan(_, Right(labelId)) if labelIdCardinality.contains(labelId) =>
          labelIdCardinality(labelId)
      }

      labelScanCardinality
        .orElse(cardinality)
        .orElse(parent.cardinalityModel(statistics, selectivity, semanticTable))
    }
    def selectivityModel(statistics: GraphStatistics, semanticTable: SemanticTable) =
      selectivity.orElse(parent.selectivityModel(statistics, semanticTable))
    def graphStatistics: GraphStatistics =
      Option(statistics).getOrElse(parent.graphStatistics)
  }

  case class LogicalPlanningEnvironment(config: LogicalPlanningConfiguration) {
    private val plannerQueryBuilder = new SimplePlannerQueryBuilder
    private val planner: Planner = new Planner(
      monitors,
      metricsFactory,
      monitor,
      tokenResolver,
      plannerQueryBuilder,
      None,
      strategy,
      queryGraphSolver
    )

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
      def newSelectivityEstimator(statistics: GraphStatistics, semanticTable: SemanticTable) =
        config.selectivityModel(statistics, semanticTable)
      def newCostModel(cardinality: Metrics.CardinalityModel) =
        config.costModel(cardinality)
      def newCardinalityEstimator(statistics: GraphStatistics, selectivity: Metrics.SelectivityModel, semanticTable: SemanticTable) =
        config.cardinalityModel(statistics, selectivity, semanticTable)
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
    }

    def planFor(queryString: String): SemanticPlan = {
      val parsedStatement = parser.parse(queryString)
      semanticChecker.check(queryString, parsedStatement)
      val (rewrittenStatement, _) = astRewriter.rewrite(queryString, parsedStatement)
      val semanticTable = semanticChecker.check(queryString, rewrittenStatement)
      val plannerQuery: QueryPlan = Planner.rewriteStatement(rewrittenStatement) match {
        case ast: Query =>
          tokenResolver.resolve(ast)(semanticTable, planContext)
          val QueryPlanInput(unionQuery, patternInExpression) = plannerQueryBuilder.produce(ast)
          val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)
          val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
          val plannerQuery = unionQuery.queries.head
          strategy.internalPlan(plannerQuery)(context, patternInExpression)
      }

      SemanticPlan(plannerQuery, semanticTable)
    }

    def getLogicalPlanFor(queryString: String): (LogicalPlan, SemanticTable) = {
      val parsedStatement = parser.parse(queryString)
      semanticChecker.check(queryString, parsedStatement)
      val (rewrittenStatement, _) = astRewriter.rewrite(queryString, parsedStatement)
      val semanticTable = semanticChecker.check(queryString, rewrittenStatement)

      Planner.rewriteStatement(rewrittenStatement) match {
        case ast: Query =>
          tokenResolver.resolve(ast)(semanticTable, planContext)
          val QueryPlanInput(unionQuery, patternInExpression) = plannerQueryBuilder.produce(ast)
          val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)
          val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
          (strategy.plan(unionQuery)(context, patternInExpression), semanticTable)
      }
    }

    def withLogicalPlanningContext[T](f: (LogicalPlanningContext, Map[PatternExpression, QueryGraph]) => T): T = {
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        metrics = metricsFactory.newMetrics(config.graphStatistics, semanticTable),
        semanticTable = semanticTable,
        strategy = queryGraphSolver
      )
      f(ctx, table)
    }
  }

  def fakeQueryPlanFor(id: String*): QueryPlan = QueryPlan(FakePlan(id.map(IdName).toSet), PlannerQuery.empty)

  def planFor(queryString: String): SemanticPlan = new given().planFor(queryString)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  implicit def idName(name: String): IdName = IdName(name)
  implicit def labelId(label: String)(implicit plan: SemanticPlan): LabelId =
    plan.semanticTable.resolvedLabelIds(label)
  implicit def propertyKeyId(label: String)(implicit plan: SemanticPlan): PropertyKeyId =
    plan.semanticTable.resolvedPropertyKeyNames(label)
}
