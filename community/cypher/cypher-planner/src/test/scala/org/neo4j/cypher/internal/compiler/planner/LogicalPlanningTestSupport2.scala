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

import org.neo4j.common
import org.neo4j.common.EntityType
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.randomVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.FakeLeafPlan
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.parsing
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.defaultParsingConfig
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.Settings
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.StaticComponents
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy.NoInference
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ComponentConnectorPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.CompressAnonymousVariables
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrderedIndexPlansUseCachedProperties
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.If
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodeVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.RelationshipVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.VectorIndexError
import org.neo4j.cypher.internal.planner.spi.VectorIndexError.WrongEntityType
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.scalatestplus.mockito.MockitoSugar

import scala.language.implicitConversions

object LogicalPlanningTestSupport2 extends MockitoSugar {

  val pushdownPropertyReads: Boolean = true
  val compressAnonymousVariables: Boolean = true
  val deduplicateNames: Boolean = true
  val allowSubqueryExpressionDuplicationInCnfNormalizer: Boolean = false

  sealed trait QueryGraphSolverSetup {
    def queryGraphSolver(): QueryGraphSolver

    def queryGraphSolver(
      solverConfig: SingleComponentIDPSolverConfig,
      disableExistsSubqueryCaching: Boolean
    ): QueryGraphSolver
    def useIdpConnectComponents: Boolean
  }

  case object QueryGraphSolverWithIDPConnectComponents extends QueryGraphSolverSetup {
    val useIdpConnectComponents: Boolean = true

    def queryGraphSolver(): QueryGraphSolver =
      queryGraphSolver(DefaultIDPSolverConfig, disableExistsSubqueryCaching = false)

    def queryGraphSolver(
      solverConfig: SingleComponentIDPSolverConfig,
      disableExistsSubqueryCaching: Boolean
    ): QueryGraphSolver = {
      val solverMonitor = mock[IDPQueryGraphSolverMonitor]
      val singleComponentPlanner = SingleComponentPlanner(solverConfig)(solverMonitor)
      val connectorPlanner = ComponentConnectorPlanner(singleComponentPlanner, solverConfig)(solverMonitor)
      val existsPlanner =
        if (disableExistsSubqueryCaching) ExistsSubqueryPlanner
        else ExistsSubqueryPlannerWithCaching()
      IDPQueryGraphSolver(singleComponentPlanner, connectorPlanner, existsPlanner)(solverMonitor)
    }
  }

  case object QueryGraphSolverWithGreedyConnectComponents extends QueryGraphSolverSetup {
    val useIdpConnectComponents: Boolean = false

    def queryGraphSolver(): QueryGraphSolver =
      queryGraphSolver(DefaultIDPSolverConfig, disableExistsSubqueryCaching = false)

    def queryGraphSolver(
      solverConfig: SingleComponentIDPSolverConfig,
      disableExistsSubqueryCaching: Boolean
    ): QueryGraphSolver = {
      val solverMonitor = mock[IDPQueryGraphSolverMonitor]
      val singleComponentPlanner = SingleComponentPlanner(solverConfig)(solverMonitor)
      val connectorPlanner = cartesianProductsOrValueJoins
      val existsPlanner =
        if (disableExistsSubqueryCaching) ExistsSubqueryPlanner
        else ExistsSubqueryPlannerWithCaching()
      IDPQueryGraphSolver(singleComponentPlanner, connectorPlanner, existsPlanner)(solverMonitor)
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

    override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
      // When different variables share the same name, OrderedIndexPlansUseCachedProperties can report false positives.
      OrderedIndexPlansUseCachedProperties
    )
  }

  val defaultCypherCompilerConfig: CypherPlannerConfiguration = CypherPlannerConfiguration.withSettings(
    Map(GraphDatabaseInternalSettings.planning_intersection_scans_enabled -> java.lang.Boolean.TRUE)
  )

  val defaultParsingConfig: ParsingConfig = ParsingConfig(
    resolveCallables = StrictResolveCallables.NoResolver,
    extractLiterals = ExtractLiteral.NEVER
  )

  def pipeLine(
    parsingConfig: ParsingConfig,
    pushdownPropertyReads: Boolean = pushdownPropertyReads,
    compressAnonymousVariables: Boolean = compressAnonymousVariables,
    deduplicateNames: Boolean = deduplicateNames,
    allowSubqueryDuplicationInCnfNormalizer: Boolean = allowSubqueryExpressionDuplicationInCnfNormalizer
  ): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    val p1 =
      new Transformer[PlannerContext, BaseState, BaseState] {
        override def transform(from: BaseState, context: PlannerContext): BaseState =
          parsing(
            parsingConfig.copy(resolveCallables = StrictResolveCallables(context.planContext))
          ).transform(from, context)
        override def name: String = "parsing-with-context-resolver"
        override def postConditions: Set[StepSequencer.Condition] = Set.empty
      } andThen
        prepareForCaching andThen
        planPipeLine(
          pushdownPropertyReads = pushdownPropertyReads,
          allowSubqueryDuplicationInCnf = allowSubqueryDuplicationInCnfNormalizer
        )
    p1 andThen
      If((_: LogicalPlanState) => compressAnonymousVariables)(
        CompressAnonymousVariables
      ) andThen
      If((_: LogicalPlanState) => deduplicateNames)(
        NameDeduplication
      )
  }

  /**
   * There are some AST changes done at the parser level for semantic analysis that won't affect the plan.
   * This rewriter can be expanded to update those parts.
   */
  def rewriteASTDifferences(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case u: SetExactPropertiesFromMapItem     => u.copy(rhsMustBeMap = false)(u.position)
      case u: SetIncludingPropertiesFromMapItem => u.copy(rhsMustBeMap = false)(u.position)
      case v: Variable if v.isIsolated          =>
        // An isolated variable e.g. "`a`", "(a)" is tracked in the AST by the Cypher5 parser.
        // This is rewrite removes the tracking.
        v.copy()(v.position, Variable.isIsolatedDefault)
      case it: IsTyped if it.withDoubleColonOnly =>
        // Type predicates with only a double column e.g. "x :: INT" are tracked in the AST by the Cypher5 parser.
        // This is rewrite removes the difference.
        it.copy()(it.position, IsTyped.withDoubleColonOnlyDefault)
      case lep: LabelExpressionPredicate if lep.isParenthesized =>
        // Label expression predicates that are parenthesized, e.g. "(n:L)", are tracked in the AST by the Cypher5 parser.
        // This is rewrite removes the difference.
        lep.copy()(
          lep.position,
          LabelExpressionPredicate.isParenthesizedDefault,
          lep.isPostfix,
          lep.hasLabeledKeyword,
          lep.hasNotKeyword
        )
    }))
  }
}

trait LogicalPlanningTestSupport2 extends AstConstructionTestSupport with LogicalPlanConstructionTestSupport
    with UsingMatcher {
  self: CypherFunSuite =>

  val pushdownPropertyReads: Boolean = LogicalPlanningTestSupport2.pushdownPropertyReads
  val deduplicateNames: Boolean = LogicalPlanningTestSupport2.deduplicateNames
  var queryGraphSolver: QueryGraphSolver = QueryGraphSolverWithIDPConnectComponents.queryGraphSolver()
  val cypherCompilerConfig: CypherPlannerConfiguration = LogicalPlanningTestSupport2.defaultCypherCompilerConfig

  val realConfig: RealLogicalPlanningConfiguration = RealLogicalPlanningConfiguration(cypherCompilerConfig)

  def createInitState(queryString: String): BaseState =
    InitialState(queryString, IDPPlannerName, new AnonymousVariableNameGenerator)

  def pipeLine(
    deduplicateNames: Boolean = deduplicateNames
  ): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    LogicalPlanningTestSupport2.pipeLine(
      parsingConfig = defaultParsingConfig,
      pushdownPropertyReads = pushdownPropertyReads,
      deduplicateNames = deduplicateNames
    )
  }

  implicit class LogicalPlanningEnvironment[C <: LogicalPlanningConfiguration](config: C) {
    lazy val semanticTable: SemanticTable = config.updateSemanticTableWithTokens(SemanticTable())

    def metricsFactory: MetricsFactory = new MetricsFactory {

      override def newCostModel(
        executionModel: ExecutionModel,
        cancellationChecker: CancellationChecker,
        databaseMode: DatabaseMode
      ): CostModel =
        (
          plan: LogicalPlan,
          input: QueryGraphSolverInput,
          semanticTable: SemanticTable,
          cardinalities: Cardinalities,
          providedOrders: ProvidedOrders,
          propertyAccess: Set[PropertyAccess],
          statistics: GraphStatistics,
          monitor: CostModelMonitor
        ) =>
          config.costModel(executionModel)((
            plan,
            input,
            semanticTable,
            cardinalities,
            providedOrders,
            propertyAccess,
            statistics,
            monitor
          ))

      override def newCardinalityEstimator(
        queryGraphCardinalityModel: QueryGraphCardinalityModel,
        selectivityCalculator: SelectivityCalculator,
        evaluator: ExpressionEvaluator
      ): CardinalityModel = {
        config.cardinalityModel(queryGraphCardinalityModel, selectivityCalculator, evaluator)
      }

      override def newQueryGraphCardinalityModel(
        planContext: PlanContext,
        selectivityCalculator: SelectivityCalculator,
        labelInferenceStrategy: LabelInferenceStrategy
      ): QueryGraphCardinalityModel =
        QueryGraphCardinalityModel.default(planContext, selectivityCalculator, labelInferenceStrategy)
    }

    def planContext: NotImplementedPlanContext = new NotImplementedPlanContext {

      private def indexesForLabel(label: String, indexType: IndexType): Iterator[IndexDescriptor] =
        config.indexes.collect {
          case (indexDef @ IndexDef(IndexDefinition.EntityType.Node(`label`), _, `indexType`), _) =>
            newIndexDescriptor(indexDef, config.indexes(indexDef))
        }.toIterator

      private def indexesForRelType(relType: String, indexType: IndexType): Iterator[IndexDescriptor] =
        config.indexes.collect {
          case (indexDef @ IndexDef(IndexDefinition.EntityType.Relationship(`relType`), _, `indexType`), _) =>
            newIndexDescriptor(indexDef, config.indexes(indexDef))
        }.toIterator

      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        config.graphStatistics,
        new MutableGraphStatisticsSnapshot()
      )

      override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        config.labelsById.get(labelId).toIterator.flatMap(label => indexesForLabel(label, IndexType.Range))
      }

      override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        config.relTypesById.get(relTypeId).toIterator.flatMap(relType => indexesForRelType(relType, IndexType.Range))
      }

      override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        config.labelsById.get(labelId).toIterator.flatMap(label => indexesForLabel(label, IndexType.Text))
      }

      override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        config.relTypesById.get(relTypeId).toIterator.flatMap(relType => indexesForRelType(relType, IndexType.Text))
      }

      override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        config.labelsById.get(labelId).toIterator.flatMap(label => indexesForLabel(label, IndexType.Point))
      }

      override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        config.relTypesById.get(relTypeId).toIterator.flatMap(relType => indexesForRelType(relType, IndexType.Point))
      }

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = config.indexes.map {
        case (indexDef: IndexDef, indexAttributes: IndexAttributes) => newIndexDescriptor(indexDef, indexAttributes)
      }.toIterator

      override def nodeVectorIndexByName(indexName: String): Either[VectorIndexError, NodeVectorIndexDescriptor] =
        config.vectorIndexes.get(indexName) match {
          case Some(NodeVectorIndexDefinition(_, labels, property, additionalProperties)) =>
            Right(NodeVectorIndexDescriptor(
              labels.map(label => semanticTable.resolvedLabelNames(label.label)),
              semanticTable.resolvedPropertyKeyNames(property),
              additionalProperties.map(semanticTable.resolvedPropertyKeyNames(_))
            ))
          case Some(_) =>
            Left(WrongEntityType(EntityType.NODE, EntityType.RELATIONSHIP))
          case None => Left(VectorIndexError.NotFound)
        }

      override def relationshipVectorIndexByName(indexName: String)
        : Either[VectorIndexError, RelationshipVectorIndexDescriptor] =
        config.vectorIndexes.get(indexName) match {
          case Some(RelationshipVectorIndexDefinition(_, relTypes, property, additionalProperties)) =>
            Right(RelationshipVectorIndexDescriptor(
              relTypes.map(relType => semanticTable.resolvedRelTypeNames(relType.relType)),
              semanticTable.resolvedPropertyKeyNames(property),
              additionalProperties.map(semanticTable.resolvedPropertyKeyNames(_))
            ))
          case Some(_) =>
            Left(WrongEntityType(EntityType.RELATIONSHIP, EntityType.NODE))
          case None => Left(VectorIndexError.NotFound)
        }

      override def procedureSignatureVersion: Long = -1

      private def newIndexDescriptor(indexDef: IndexDef, indexAttributes: IndexAttributes) = {
        val canGetValue = if (indexAttributes.withValues) CanGetValue else DoNotGetValue
        val entityType = toIndexDescriptorEntityType(indexDef.entityType)

        val indexCapability = indexDef.indexType match {
          case IndexType.Range => IndexCapabilities.range
          case IndexType.Text  => IndexCapabilities.text_2_0
          case IndexType.Point => IndexCapabilities.point
        }

        IndexDescriptor(
          indexDef.indexType,
          entityType,
          indexDef.propertyKeys.map(semanticTable.resolvedPropertyKeyNames(_)),
          valueCapability = canGetValue,
          orderCapability = indexAttributes.withOrdering,
          maybeKernelIndexCapability = Some(indexCapability),
          isUnique = indexAttributes.isUnique
        )
      }

      private def toIndexDescriptorEntityType(indexDefEntityType: IndexDefinition.EntityType)
        : IndexDescriptor.EntityType = indexDefEntityType match {
        case IndexDefinition.EntityType.Node(label) => IndexDescriptor.EntityType.Node(
            semanticTable.resolvedLabelNames(label)
          )
        case IndexDefinition.EntityType.Relationship(relType) => IndexDescriptor.EntityType.Relationship(
            semanticTable.resolvedRelTypeNames(relType)
          )
      }

      override def nodeTokenIndex: Option[TokenIndexDescriptor] =
        Some(TokenIndexDescriptor(common.EntityType.NODE, IndexOrderCapability.BOTH))

      override def relationshipTokenIndex: Option[TokenIndexDescriptor] =
        if (config.lookupRelationshipsByType.canLookupRelationshipsByType)
          Some(TokenIndexDescriptor(common.EntityType.RELATIONSHIP, IndexOrderCapability.BOTH))
        else None

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        config.nodeConstraints.filter(p => p._1 == labelName).flatMap(p => p._2)
      }

      override def getRelationshipPropertiesWithExistenceConstraint(relTypeName: String): Set[String] = {
        config.relationshipConstraints.filter(p => p._1 == relTypeName).flatMap(p => p._2)
      }

      override def getPropertiesWithExistenceConstraint: Set[String] = {
        config.relationshipConstraints.flatMap(_._2) ++ config.nodeConstraints.flatMap(_._2)
      }

      override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
        getNodePropertiesWithExistenceConstraint(labelName).contains(propertyKey)
      }

      override def hasRelationshipPropertyExistenceConstraint(relTypeName: String, propertyKey: String): Boolean = {
        getRelationshipPropertiesWithExistenceConstraint(relTypeName).contains(propertyKey)
      }

      override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[ConstrainableType]] = {
        // This trait does not support adding property type constraints
        Map.empty
      }

      override def hasNodePropertyTypeConstraint(
        labelName: String,
        propertyKey: String,
        cypherType: ConstrainableType
      ): Boolean = {
        // This trait does not support adding property type constraints
        false
      }

      override def getRelationshipPropertiesWithTypeConstraint(labelName: String)
        : Map[String, Seq[ConstrainableType]] = {
        // This trait does not support adding property type constraints
        Map.empty
      }

      override def hasRelationshipPropertyTypeConstraint(
        relTypeName: String,
        propertyKey: String,
        cypherType: ConstrainableType
      ): Boolean = {

        // This trait does not support adding property type constraints
        false
      }

      override def hasRelationshipEndpointLabelConstraint(
        relTypeName: String,
        labelName: String,
        endpointType: EndpointType
      ): Boolean = {
        // This trait does not support adding relationship endpoint label constraints
        false
      }

      override def procedureSignature(name: ProcedureName): ProcedureSignature = {
        config.procedureSignatures.find(_.name == name).get
      }

      override def indexExistsForLabel(labelId: Int): Boolean = {
        val labelName = config.labelsById(labelId)
        config.indexes.keys.exists {
          case IndexDef(IndexDefinition.EntityType.Node(`labelName`), _, _) => true
          case _                                                            => false
        }
      }

      override def getLabelName(id: Int): String = config.labelsById(id)

      override def getRelTypeName(id: Int): String = config.relTypesById(id)

      override def indexExistsForRelType(relTypeId: Int): Boolean = {
        val relTypeName = config.relTypesById(relTypeId)
        config.indexes.keys.exists {
          case IndexDef(IndexDefinition.EntityType.Relationship(`relTypeName`), _, _) => true
          case _                                                                      => false
        }
      }

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
        semanticTable.resolvedPropertyKeyNames.get(propertyKeyName).map(_.id)

      override def getPropertyKeyName(id: Int): String =
        semanticTable.resolvedPropertyKeyNames.find(_._2.id == id).map(_._1).get

      override def getOptLabelId(labelName: String): Option[Int] =
        semanticTable.resolvedLabelNames.get(labelName).map(_.id)

      override def getOptRelTypeId(relType: String): Option[Int] =
        semanticTable.resolvedRelTypeNames.get(relType).map(_.id)

      override def txStateHasChanges(): Boolean = false

      override def databaseMode: DatabaseMode = DatabaseMode.SINGLE

      override def storageHasPropertyColocation: Boolean = false

      override def storageSupportsFastExpandInto: Boolean = false

      override def storageIsMvcc: Boolean = false

      override def getNodeLabelConstraints(constrainedLabel: String): Set[String] = Set.empty

      override def getRelationshipEndpointLabelConstraints(relTypeName: String): Map[EndpointType, String] = Map.empty
    }

    // Hack to guarantee coverage across cypher versions :/.
    def getLogicalPlanFor(
      query: String,
      conf: CypherPlannerConfiguration = cypherCompilerConfig,
      solver: QueryGraphSolver = queryGraphSolver,
      stripProduceResults: Boolean = true,
      deduplicateNames: Boolean = deduplicateNames,
      debugOptions: CypherDebugOptions = CypherDebugOptions.default
    ): (LogicalPlan, SemanticTable, PlanningAttributes) =
      doGetLogicalPlanFor(randomVersion(), query, conf, solver, stripProduceResults, deduplicateNames, debugOptions)

    private def doGetLogicalPlanFor(
      version: CypherVersion,
      queryString: String,
      cypherConfig: CypherPlannerConfiguration,
      queryGraphSolver: QueryGraphSolver,
      stripProduceResults: Boolean,
      deduplicateNames: Boolean,
      debugOptions: CypherDebugOptions
    ): (LogicalPlan, SemanticTable, PlanningAttributes) = {
      val context = getContext(version, queryString, cypherConfig, queryGraphSolver, debugOptions)
      val state = createInitState(queryString)
      val output = pipeLine(deduplicateNames).transform(state, context)
      val logicalPlan = output.logicalPlan match {
        case p: ProduceResult if stripProduceResults => p.source
        case p                                       => p
      }
      (logicalPlan, output.semanticTable(), output.planningAttributes)
    }

    // Hack to guarantee coverage of all cypher versions :/
    def getLogicalPlanForAst(initialState: BaseState): (LogicalPlan, SemanticTable, PlanningAttributes) =
      getLogicalPlanForAst(randomVersion(), initialState)

    def getLogicalPlanForAst(
      version: CypherVersion,
      initialState: BaseState
    ): (LogicalPlan, SemanticTable, PlanningAttributes) = {
      // As the test only checks ast -> planning, the query string can be an empty string
      val context = getContext(version, "")

      val output = pipeLine(deduplicateNames).transform(initialState, context)
      val logicalPlan = output.logicalPlan match {
        case p: ProduceResult => p.source
        case p                => p
      }
      (logicalPlan, output.semanticTable(), output.planningAttributes)
    }

    private def getContext(
      version: CypherVersion,
      queryString: String,
      cypherConfig: CypherPlannerConfiguration = cypherCompilerConfig,
      queryGraphSolver: QueryGraphSolver = queryGraphSolver,
      debugOptions: CypherDebugOptions = CypherDebugOptions.default
    ): PlannerContext = {
      val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))

      val metrics = metricsFactory.newMetrics(
        planContext,
        simpleExpressionEvaluator,
        config.executionModel,
        CancellationChecker.neverCancelled()
      )

      ContextHelper.create(
        version,
        planContext = planContext,
        cypherExceptionFactory = exceptionFactory,
        queryGraphSolver = queryGraphSolver,
        metrics = metrics,
        config = cypherConfig,
        logicalPlanIdGen = idGen,
        debugOptions = debugOptions,
        executionModel = config.executionModel,
        statefulShortestPlanningMode = cypherConfig.statefulShortestPlanningMode()
      )
    }

    def withLogicalPlanningContext[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(
        planContext,
        simpleExpressionEvaluator,
        config.executionModel,
        CancellationChecker.neverCancelled()
      )
      val planningAttributes = PlanningAttributes.newAttributes
      val ctx = newLogicalPlanningContext(metrics, planningAttributes)
      f(config, ctx)
    }

    def withLogicalPlanningContextWithFakeAttributes[T](f: (C, LogicalPlanningContext) => T): T = {
      val metrics = metricsFactory.newMetrics(
        planContext,
        simpleExpressionEvaluator,
        config.executionModel,
        CancellationChecker.neverCancelled()
      )
      val planningAttributes = newStubbedPlanningAttributes
      val ctx = newLogicalPlanningContext(metrics, planningAttributes)
      f(config, ctx)
    }

    private def newLogicalPlanningContext(metrics: Metrics, planningAttributes: PlanningAttributes) = {
      val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen)
      val staticComponents = StaticComponents(
        planContext = planContext,
        notificationLogger = devNullLogger,
        planningAttributes = planningAttributes,
        logicalPlanProducer = logicalPlanProducer,
        queryGraphSolver = queryGraphSolver,
        metrics = metrics,
        idGen = idGen,
        anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
        cancellationChecker = CancellationChecker.NeverCancelled,
        semanticTable = semanticTable,
        costComparisonListener = devNullListener,
        readOnly = false,
        labelInferenceStrategy = NoInference
      )

      val settings = Settings(
        executionModel = config.executionModel,
        debugOptions = CypherDebugOptions.default,
        predicatesAsUnionMaxSize = cypherCompilerConfig.predicatesAsUnionMaxSize()
      )

      LogicalPlanningContext(staticComponents, settings)
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

  def planFor(
    queryString: String,
    config: CypherPlannerConfiguration = cypherCompilerConfig,
    queryGraphSolver: QueryGraphSolver = queryGraphSolver,
    stripProduceResults: Boolean = true,
    deduplicateNames: Boolean = deduplicateNames
  ): (LogicalPlan, SemanticTable, PlanningAttributes) =
    new givenConfig().getLogicalPlanFor(queryString, config, queryGraphSolver, stripProduceResults, deduplicateNames)

  class givenConfig extends StubbedLogicalPlanningConfiguration(realConfig)

  class givenPlanWithMinimumCardinalityEnabled
      extends StubbedLogicalPlanningConfiguration(RealLogicalPlanningConfiguration(cypherCompilerConfig))

  implicit def propertyKeyId(label: String)(implicit semanticTable: SemanticTable): PropertyKeyId =
    semanticTable.resolvedPropertyKeyNames(label)

}
