/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.helpers.FakeLeafPlan
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.parsing
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ComponentConnectorPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.ir.PeriodicCommit
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.OrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.ValueCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport
import org.scalatest.mockito.MockitoSugar

import scala.language.implicitConversions

object LogicalPlanningTestSupport2 extends MockitoSugar {

  val pushdownPropertyReads: Boolean = true
  val deduplicateNames: Boolean = true

  val configurationThatForcesCompacting: CypherPlannerConfiguration = {
    val builder = Config.newBuilder()
    //NOTE: 10 is the minimum allowed value
    builder.set(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold, Long.box(10L))
    val dbConfig = builder.build()
    CypherPlannerConfiguration.fromCypherConfiguration(CypherConfiguration.fromConfig(dbConfig), dbConfig, planSystemCommands = false)
  }

  sealed trait QueryGraphSolverSetup {
    def queryGraphSolver(): QueryGraphSolver
    def queryGraphSolver(solverConfig: SingleComponentIDPSolverConfig): QueryGraphSolver
    def useIdpConnectComponents: Boolean
  }
  case object QueryGraphSolverWithIDPConnectComponents extends QueryGraphSolverSetup {
    val useIdpConnectComponents: Boolean = true

    def queryGraphSolver(): QueryGraphSolver = queryGraphSolver(DefaultIDPSolverConfig)

    def queryGraphSolver(solverConfig: SingleComponentIDPSolverConfig): QueryGraphSolver = {
      val solverMonitor = mock[IDPQueryGraphSolverMonitor]
      val singleComponentPlanner = SingleComponentPlanner(solverConfig)(solverMonitor)
      val connectorPlanner = ComponentConnectorPlanner(singleComponentPlanner, solverConfig)(solverMonitor)
      new IDPQueryGraphSolver(singleComponentPlanner, connectorPlanner)(solverMonitor)
    }
  }
  case object QueryGraphSolverWithGreedyConnectComponents extends QueryGraphSolverSetup {
    val useIdpConnectComponents: Boolean = false

    def queryGraphSolver(): QueryGraphSolver = queryGraphSolver(DefaultIDPSolverConfig)

    def queryGraphSolver(solverConfig: SingleComponentIDPSolverConfig): QueryGraphSolver = {
      val solverMonitor = mock[IDPQueryGraphSolverMonitor]
      val singleComponentPlanner = SingleComponentPlanner(solverConfig)(solverMonitor)
      val connectorPlanner = cartesianProductsOrValueJoins
      new IDPQueryGraphSolver(singleComponentPlanner, connectorPlanner)(solverMonitor)
    }
  }


  final case object NameDeduplication extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {
    override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING
    override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
      from
        .withMaybeQuery(from.maybeQuery.map(removeGeneratedNamesAndParamsOnTree))
        .withMaybeLogicalPlan(from.maybeLogicalPlan.map(removeGeneratedNamesAndParamsOnTree))
    }
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }

  val defaultCypherCompilerConfig: CypherPlannerConfiguration = CypherPlannerConfiguration.defaults()

  def defaultParsingConfig(cypherCompilerConfig: CypherPlannerConfiguration): ParsingConfig =
    ParsingConfig(
      literalExtractionStrategy = Never,
      parameterTypeMapping = Map.empty,
      semanticFeatures = cypherCompilerConfig.enabledSemanticFeatures,
      useJavaCCParser = cypherCompilerConfig.useJavaCCParser)

  def pipeLine(parsingConfig: ParsingConfig,
               pushdownPropertyReads: Boolean = pushdownPropertyReads,
               deduplicateNames: Boolean = deduplicateNames,
              ): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    // if you ever want to have parameters in here, fix the map
    val p1 = parsing(parsingConfig) andThen
      prepareForCaching andThen
      planPipeLine(pushdownPropertyReads = pushdownPropertyReads, semanticFeatures = parsingConfig.semanticFeatures)
    if (deduplicateNames) {
      p1 andThen NameDeduplication
    } else {
      p1
    }
  }
}

trait LogicalPlanningTestSupport2 extends CypherTestSupport with AstConstructionTestSupport with LogicalPlanConstructionTestSupport with UsingMatcher {
  self: CypherFunSuite =>

  val parser = JavaCCParser
  val pushdownPropertyReads: Boolean = LogicalPlanningTestSupport2.pushdownPropertyReads
  val deduplicateNames: Boolean = LogicalPlanningTestSupport2.deduplicateNames
  var queryGraphSolver: QueryGraphSolver = QueryGraphSolverWithIDPConnectComponents.queryGraphSolver()
  val cypherCompilerConfig: CypherPlannerConfiguration = LogicalPlanningTestSupport2.defaultCypherCompilerConfig

  val realConfig: RealLogicalPlanningConfiguration = RealLogicalPlanningConfiguration(cypherCompilerConfig)

  def createInitState(queryString: String): BaseState = InitialState(queryString, IDPPlannerName, new AnonymousVariableNameGenerator)

  def pipeLine(deduplicateNames: Boolean = deduplicateNames): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    val parsingConfig = LogicalPlanningTestSupport2.defaultParsingConfig(cypherCompilerConfig)
    LogicalPlanningTestSupport2.pipeLine(parsingConfig, pushdownPropertyReads, deduplicateNames)
  }

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable: SemanticTable = config.updateSemanticTableWithTokens(SemanticTable())

    def metricsFactory: MetricsFactory = new MetricsFactory {
      override def newCostModel(executionModel: ExecutionModel): CostModel =
        (plan: LogicalPlan, input: QueryGraphSolverInput, semanticTable: SemanticTable, cardinalities: Cardinalities, providedOrders: ProvidedOrders, monitor: CostModelMonitor) =>
          config.costModel(executionModel)((plan, input, semanticTable, cardinalities, providedOrders, monitor))

      override def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel,
                                           selectivityCalculator: SelectivityCalculator,
                                           evaluator: ExpressionEvaluator): CardinalityModel = {
        config.cardinalityModel(queryGraphCardinalityModel, selectivityCalculator, evaluator)
      }

      override def newQueryGraphCardinalityModel(planContext: PlanContext,
                                                 selectivityCalculator: SelectivityCalculator): QueryGraphCardinalityModel =
        QueryGraphCardinalityModel.default(planContext, selectivityCalculator)
    }

    def table = Map.empty[PatternExpression, QueryGraph]

    def planContext: NotImplementedPlanContext = new NotImplementedPlanContext {
      private def indexesForLabel(label: String, indexType: IndexType): Iterator[IndexDescriptor] =
        config.indexes.collect {
          case (indexDef@IndexDef(IndexDefinition.EntityType.Node(`label`), _, `indexType`), _) =>
            newIndexDescriptor(indexDef, config.indexes(indexDef))
        }.toIterator

      private def indexesForRelType(relType: String, indexType: IndexType): Iterator[IndexDescriptor] =
        config.indexes.collect {
          case (indexDef@IndexDef(IndexDefinition.EntityType.Relationship(`relType`), _, `indexType`), _) =>
            newIndexDescriptor(indexDef, config.indexes(indexDef))
        }.toIterator

      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        config.graphStatistics,
        new MutableGraphStatisticsSnapshot())

      override def btreeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        config.labelsById.get(labelId).toIterator.flatMap(label => indexesForLabel(label, IndexType.Btree))
      }

      override def btreeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        config.relTypesById.get(relTypeId).toIterator.flatMap(relType => indexesForRelType(relType, IndexType.Btree))
      }

      override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        config.labelsById.get(labelId).toIterator.flatMap(label => indexesForLabel(label, IndexType.Text))
      }

      override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        config.relTypesById.get(relTypeId).toIterator.flatMap(relType => indexesForRelType(relType, IndexType.Text))
      }

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = config.indexes.map {
        case (indexDef: IndexDef, indexAttributes: IndexAttributes) => newIndexDescriptor(indexDef, indexAttributes)
      }.toIterator

      private def newIndexDescriptor(indexDef: IndexDef, indexAttributes: IndexAttributes) = {
        // Our fake index either can always or never return property values
        val canGetValue = if (indexAttributes.withValues) CanGetValue else DoNotGetValue
        val valueCapability: ValueCapability = _ => indexDef.propertyKeys.map(_ => canGetValue)
        val orderCapability: OrderCapability = _ => indexAttributes.withOrdering
        val entityType = indexDef.entityType match {
          case IndexDefinition.EntityType.Node(label) => IndexDescriptor.EntityType.Node(
            semanticTable.resolvedLabelNames(label))
          case IndexDefinition.EntityType.Relationship(relType) => IndexDescriptor.EntityType.Relationship(
            semanticTable.resolvedRelTypeNames(relType))
        }
        IndexDescriptor(
          indexDef.indexType,
          entityType,
          indexDef.propertyKeys.map(semanticTable.resolvedPropertyKeyNames(_)),
          valueCapability = valueCapability,
          orderCapability = orderCapability,
          isUnique = indexAttributes.isUnique
        )
      }

      override def canLookupNodesByLabel: Boolean = true

      override def canLookupRelationshipsByType: Boolean =
        config.lookupRelationshipsByType.canLookupRelationshipsByType

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        config.nodeConstraints.filter(p => p._1 == labelName).flatMap(p => p._2)
      }

      override def getRelationshipPropertiesWithExistenceConstraint(relTypeName: String): Set[String] = {
        config.relationshipConstraints.filter(p => p._1 == relTypeName).flatMap(p => p._2)
      }

      override def getPropertiesWithExistenceConstraint: Set[String] = {
        config.relationshipConstraints.flatMap(_._2) ++ config.nodeConstraints.flatMap(_._2)
      }

      override def procedureSignature(name: QualifiedName): ProcedureSignature = {
        config.procedureSignatures.find(_.name == name).get
      }

      override def btreeIndexExistsForLabel(labelId: Int): Boolean = {
        val labelName = config.labelsById(labelId)
        config.indexes.keys.exists {
          case IndexDef(IndexDefinition.EntityType.Node(`labelName`), _, IndexType.Btree) => true
          case _ => false
        }
      }

      override def btreeIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
        config.indexes.contains(IndexDef(IndexDefinition.EntityType.Node(labelName), propertyKey, IndexType.Btree))


      override def btreeIndexExistsForRelTypeAndProperties(relTypeName: String,
                                                           propertyKey: Seq[String]): Boolean =
        config.indexes.contains(IndexDef(IndexDefinition.EntityType.Relationship(relTypeName), propertyKey, IndexType.Btree))

      override def btreeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val indexDef = IndexDef(IndexDefinition.EntityType.Node(labelName), propertyKeys, IndexType.Btree)
        config.indexes.get(indexDef).map(indexType => newIndexDescriptor(indexDef, indexType))
      }

      override def btreeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val indexDef = IndexDef(IndexDefinition.EntityType.Relationship(relTypeName), propertyKeys, IndexType.Btree)
        config.indexes.get(indexDef).map(indexType => newIndexDescriptor(indexDef, indexType))
      }

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
        semanticTable.resolvedPropertyKeyNames.get(propertyKeyName).map(_.id)

      override def getOptLabelId(labelName: String): Option[Int] =
        semanticTable.resolvedLabelNames.get(labelName).map(_.id)

      override def getOptRelTypeId(relType: String): Option[Int] =
        semanticTable.resolvedRelTypeNames.get(relType).map(_.id)

      override def txStateHasChanges(): Boolean = false
    }

    def getLogicalPlanFor(queryString: String,
                          cypherConfig:CypherPlannerConfiguration = cypherCompilerConfig,
                          queryGraphSolver: QueryGraphSolver = queryGraphSolver,
                          stripProduceResults: Boolean = true,
                          deduplicateNames: Boolean = deduplicateNames,
                          debugOptions: CypherDebugOptions = CypherDebugOptions.default
                         ): (Option[PeriodicCommit], LogicalPlan, SemanticTable, PlanningAttributes) = {
      val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
      val metrics = metricsFactory.newMetrics(planContext, simpleExpressionEvaluator, config.executionModel, cypherConfig.planningTextIndexesEnabled)
      def context = ContextHelper.create(planContext = planContext,
        cypherExceptionFactory = exceptionFactory,
        queryGraphSolver = queryGraphSolver,
        metrics = metrics,
        config = cypherConfig,
        logicalPlanIdGen = idGen,
        debugOptions = debugOptions,
        executionModel = config.executionModel
      )

      val state = createInitState(queryString)
      val output = pipeLine(deduplicateNames).transform(state, context)
      val logicalPlan = output.logicalPlan match {
        case p:ProduceResult if stripProduceResults => p.source
        case p => p
      }
      (output.maybePeriodicCommit.flatten, logicalPlan, output.semanticTable(), output.planningAttributes)
    }

    def withLogicalPlanningContext[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(planContext, simpleExpressionEvaluator, config.executionModel, planningTextIndexesEnabled = false)
      val planningAttributes = PlanningAttributes.newAttributes
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        logicalPlanProducer = logicalPlanProducer,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        predicatesAsUnionMaxSize = cypherCompilerConfig.predicatesAsUnionMaxSize,
        input = QueryGraphSolverInput.empty,
        notificationLogger = devNullLogger,
        costComparisonListener = devNullListener,
        planningAttributes = planningAttributes,
        idGen = idGen,
        executionModel = config.executionModel,
        debugOptions = CypherDebugOptions.default,
        anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
        cancellationChecker = CancellationChecker.NeverCancelled,
      )
      f(config, ctx)
    }


    def withLogicalPlanningContextWithFakeAttributes[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(planContext, simpleExpressionEvaluator, config.executionModel, planningTextIndexesEnabled = false)
      val planningAttributes = newStubbedPlanningAttributes
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen)
      val ctx = LogicalPlanningContext(
        planContext = planContext,
        logicalPlanProducer = logicalPlanProducer,
        metrics = metrics,
        semanticTable = semanticTable,
        strategy = queryGraphSolver,
        predicatesAsUnionMaxSize = cypherCompilerConfig.predicatesAsUnionMaxSize,
        input = QueryGraphSolverInput.empty,
        notificationLogger = devNullLogger,
        costComparisonListener = devNullListener,
        planningAttributes = planningAttributes,
        idGen = idGen,
        executionModel = config.executionModel,
        debugOptions = CypherDebugOptions.default,
        anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
        cancellationChecker = CancellationChecker.NeverCancelled,
      )
      f(config, ctx)
    }
  }

  def set[T](plan: LogicalPlan, attribute: Attribute[LogicalPlan, T], t: T): LogicalPlan = {
    attribute.set(plan.id, t)
    plan
  }

  def setC(plan: LogicalPlan, cardinalities: Cardinalities, c: Cardinality): LogicalPlan = {
    cardinalities.set(plan.id, c)
    plan
  }

  def fakeLogicalPlanFor(id: String*): FakeLeafPlan = FakeLeafPlan(id.toSet)

  def fakeLogicalPlanFor(planningAttributes: PlanningAttributes, id: String*): FakeLeafPlan = {
    val res = FakeLeafPlan(id.toSet)
    planningAttributes.solveds.set(res.id, SinglePlannerQuery.empty)
    planningAttributes.cardinalities.set(res.id, 0.0)
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  def planFor(queryString: String,
              config:CypherPlannerConfiguration = cypherCompilerConfig,
              queryGraphSolver: QueryGraphSolver = queryGraphSolver,
              stripProduceResults: Boolean = true,
              deduplicateNames: Boolean = deduplicateNames
             ): (Option[PeriodicCommit], LogicalPlan, SemanticTable, PlanningAttributes) =
    new given().getLogicalPlanFor(queryString, config, queryGraphSolver, stripProduceResults, deduplicateNames)

  class given extends StubbedLogicalPlanningConfiguration(realConfig)

  class givenPlanWithMinimumCardinalityEnabled
    extends StubbedLogicalPlanningConfiguration(RealLogicalPlanningConfiguration(cypherCompilerConfig))

  implicit def propertyKeyId(label: String)(implicit semanticTable: SemanticTable): PropertyKeyId =
    semanticTable.resolvedPropertyKeyNames(label)

}
