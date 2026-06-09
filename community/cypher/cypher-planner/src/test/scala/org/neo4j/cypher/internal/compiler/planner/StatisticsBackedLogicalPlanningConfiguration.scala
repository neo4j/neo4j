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
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.graphcounts.ApocMetaStats
import org.neo4j.cypher.graphcounts.Constraint
import org.neo4j.cypher.graphcounts.GraphCountData
import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.graphcounts.Index
import org.neo4j.cypher.graphcounts.NodeCount
import org.neo4j.cypher.graphcounts.RelationshipCount
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.randomVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.EnableWorkingScopeNamespacer
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.helpers.TokenContainer
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration.PlanningResult
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration.RecordingCostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Cardinalities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.DatabaseFormat
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.ExistenceConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType.Node
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType.Relationship
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Indexes
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Options
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.PropertyTypeDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelDef
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelationshipEndpointLabelConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.defaultSettingsOverrides
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.frontend.notification.NotificationWrapping
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.RecordingNotificationLogger
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.options.CypherParallelRepeatHeuristicOption
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption
import org.neo4j.cypher.internal.options.OptionReader
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodeVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.RelationshipVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.VectorIndexError
import org.neo4j.cypher.internal.planner.spi.VectorIndexError.NotFound
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.graphdb
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.IndexType.FULLTEXT
import org.neo4j.internal.schema.IndexType.LOOKUP
import org.neo4j.internal.schema.IndexType.POINT
import org.neo4j.internal.schema.IndexType.RANGE
import org.neo4j.internal.schema.IndexType.TEXT
import org.neo4j.internal.schema.IndexType.VECTOR
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue

import java.io.File

import scala.Console.err
import scala.jdk.CollectionConverters.MapHasAsJava

trait StatisticsBackedLogicalPlanningSupport {

  /**
   * @return an immutable builder to construct [[StatisticsBackedLogicalPlanningConfiguration]]s.
   */
  protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    StatisticsBackedLogicalPlanningConfigurationBuilder.newBuilder()
}

object StatisticsBackedLogicalPlanningConfigurationBuilder {

  private val defaultSettingsOverrides: Map[Setting[_], AnyRef] = Map(
    GraphDatabaseInternalSettings.cypher_lp_eager_analysis_fallback_enabled -> Boolean.box(false),
    GraphDatabaseSettings.db_format -> DatabaseFormat.Block.settingValue // to match DefaultDbFormatSettingMigrator
  )

  def newBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    StatisticsBackedLogicalPlanningConfigurationBuilder()

  case class Options(
    debug: CypherDebugOptions = CypherDebugOptions(Set(CypherDebugOption.verboseEagernessReasons)),
    connectComponentsPlanner: Boolean = true,
    executionModel: ExecutionModel = ExecutionModel.default,
    useMinimumGraphStatistics: Boolean = false,
    txStateHasChanges: Boolean = false,
    deduplicateNames: Boolean = true,
    semanticFeatures: Seq[SemanticFeature] = Seq(EnableWorkingScopeNamespacer),
    databaseReferenceRepository: DatabaseReferenceRepository = ContextHelper.mockDatabaseReferenceRepository,
    printNotifications: Boolean = false,
    parallelRepeatHeuristic: CypherParallelRepeatHeuristicOption = CypherParallelRepeatHeuristicOption.disabled,
    plannerVersionOption: CypherPlannerVersionOption = CypherPlannerVersionOption.default
  )

  case class Cardinalities(
    allNodes: Option[Double] = None,
    labels: Map[String, Double] = Map[String, Double](),
    relationships: Map[RelDef, Double] = Map[RelDef, Double](),
    defaultRelationshipCardinalityTo0: Boolean = false
  ) {

    def getRelCount(relDef: RelDef): Double = {
      def defaultOrThrow: Double =
        if (defaultRelationshipCardinalityTo0) 0.0
        else throw new IllegalStateException(
          s"""No cardinality set for relationship $relDef. Please specify using
             |.setRelationshipCardinality("$relDef", cardinality)""".stripMargin
        )

      def getForAllRelationshipsMatching: Option[Double] = {
        Option.when(relDef.relType.isEmpty) {
          val matchingRels = relationships.filter(r =>
            r._1.fromLabel == relDef.fromLabel && r._1.toLabel == relDef.toLabel
          )
          Option.when(matchingRels.nonEmpty)(matchingRels.values.sum)
        }.flatten
      }

      relationships.get(relDef).orElse(getForAllRelationshipsMatching).getOrElse(defaultOrThrow)
    }
  }

  object RelDef {

    implicit private class RegexHelper(val sc: StringContext) {
      def re: scala.util.matching.Regex = sc.parts.mkString.r
    }

    private def opt(s: String): Option[String] = Option(s).filter(_.nonEmpty)

    def fromString(pattern: String): Seq[RelDef] = pattern match {
      case re"""\(:?(.*?)$f\)-\[:?(.*?)$r\]->\(:?(.*?)$t\)""" =>
        Seq(RelDef(opt(f), opt(r), opt(t)))

      case re"""\(:?(.*?)$t\)<-\[:?(.*?)$r\]-\(:?(.*?)$f\)""" =>
        Seq(RelDef(opt(f), opt(r), opt(t)))

      case re"""\(:?(.*?)$t\)-\[:?(.*?)$r\]-\(:?(.*?)$b\)""" =>
        Seq(RelDef(opt(b), opt(r), opt(t)), RelDef(opt(t), opt(r), opt(b)))

      case pat =>
        throw new IllegalArgumentException(
          s"Invalid relationship pattern $pat. Expected something like ()-[]-(), (:A)-[:R]->(), (:A)<-[]-(), etc."
        )
    }

    val all: RelDef = RelDef(None, None, None)
  }

  case class RelDef(fromLabel: Option[String], relType: Option[String], toLabel: Option[String]) {

    override def toString: String = {
      val f = fromLabel.fold("")(l => ":" + l)
      val r = relType.fold("")(l => ":" + l)
      val t = toLabel.fold("")(l => ":" + l)
      s"($f)-[$r]->($t)"
    }
  }

  case class IndexDefinition(
    entityType: EntityType,
    indexType: graphdb.schema.IndexType,
    propertyKeys: Seq[String],
    uniqueValueSelectivity: Double,
    propExistsSelectivity: Double,
    isUnique: Boolean = false,
    withValues: Boolean = false,
    withOrdering: IndexOrderCapability = IndexOrderCapability.NONE,
    indexCapability: IndexCapability = NO_CAPABILITY
  )

  object IndexDefinition {
    sealed trait EntityType

    object EntityType {
      final case class Node(label: String) extends EntityType

      final case class Relationship(relType: String) extends EntityType
    }
  }

  case class Indexes(
    nodeLookupIndex: Option[TokenIndexDescriptor] =
      Some(TokenIndexDescriptor(common.EntityType.NODE, IndexOrderCapability.BOTH)),
    relationshipLookupIndex: Option[TokenIndexDescriptor] =
      Some(TokenIndexDescriptor(common.EntityType.RELATIONSHIP, IndexOrderCapability.BOTH)),
    propertyIndexes: Seq[IndexDefinition] = Seq.empty,
    vectorIndexes: Seq[VectorIndexDefinition] = Seq.empty
  ) {

    def addPropertyIndex(indexDefinition: IndexDefinition): Indexes = {
      this.copy(propertyIndexes = propertyIndexes.filterNot(sameKeys(indexDefinition, _)) :+ indexDefinition)
    }

    private def sameKeys(indexDefinition: IndexDefinition, oldDef: IndexDefinition) = {
      oldDef.entityType == indexDefinition.entityType &&
      oldDef.propertyKeys == indexDefinition.propertyKeys &&
      oldDef.indexType == indexDefinition.indexType
    }

    def addNodeLookupIndex(orderCapability: IndexOrderCapability): Indexes =
      this.copy(nodeLookupIndex = Some(TokenIndexDescriptor(common.EntityType.NODE, orderCapability)))

    def removeNodeLookupIndex(): Indexes = this.copy(nodeLookupIndex = None)

    def addRelationshipLookupIndex(orderCapability: IndexOrderCapability): Indexes =
      this.copy(relationshipLookupIndex = Some(TokenIndexDescriptor(common.EntityType.RELATIONSHIP, orderCapability)))

    def removeRelationshipLookupIndex(): Indexes = this.copy(relationshipLookupIndex = None)

    def addNodeVectorIndex(
      name: String,
      labels: Seq[String],
      propertyKey: String,
      additionalProperties: Seq[String]
    ): Indexes =
      copy(vectorIndexes =
        vectorIndexes :+ NodeVectorIndexDefinition(name, labels.map(Node.apply), propertyKey, additionalProperties)
      )

    def addRelationshipVectorIndex(
      name: String,
      types: Seq[String],
      propertyKey: String,
      additionalProperties: Seq[String]
    ): Indexes =
      copy(vectorIndexes =
        vectorIndexes :+ RelationshipVectorIndexDefinition(
          name,
          types.map(Relationship.apply),
          propertyKey,
          additionalProperties
        )
      )
  }

  case class ExistenceConstraintDefinition(entityType: IndexDefinition.EntityType, propertyKey: String)

  case class PropertyTypeDefinition(
    entityType: IndexDefinition.EntityType,
    propertyKey: String,
    propertyType: ConstrainableType
  )

  case class RelationshipEndpointLabelConstraintDefinition(
    relType: String,
    label: String,
    endPoint: EndpointType
  )

  def getProvidesOrder(indexType: IndexType): IndexOrderCapability = indexType match {
    case FULLTEXT => IndexOrderCapability.NONE
    case LOOKUP   => IndexOrderCapability.BOTH
    case TEXT     => IndexOrderCapability.NONE
    case RANGE    => IndexOrderCapability.BOTH
    case POINT    => IndexOrderCapability.NONE
    case VECTOR   => IndexOrderCapability.NONE
  }

  def getWithValues(indexType: IndexType): Boolean = indexType match {
    case FULLTEXT => false
    case LOOKUP   => true
    case TEXT     => false
    case RANGE    => true
    case POINT    => true
    case VECTOR   => false
  }

  object IndexCapabilities {
    val text_1_0: IndexCapability = org.neo4j.kernel.api.impl.schema.TextIndexCapability.text()
    val text_2_0: IndexCapability = org.neo4j.kernel.api.impl.schema.TextIndexCapability.trigram()
    val text_3_0: IndexCapability = org.neo4j.kernel.api.impl.schema.TextIndexCapability.trigram()
    val point: IndexCapability = org.neo4j.kernel.impl.index.schema.PointIndexProvider.CAPABILITY
    val range: IndexCapability = org.neo4j.kernel.impl.index.schema.RangeIndexProvider.CAPABILITY
  }

  sealed trait DatabaseFormat extends Product {
    def settingValue: String = productPrefix.toLowerCase
  }

  object DatabaseFormat {
    case object Block extends DatabaseFormat

    case object Aligned extends DatabaseFormat

    case object Mvcc extends DatabaseFormat {
      override def settingValue: String = "multiversion_block"
    }

    def default: DatabaseFormat = Block
  }
}

/**
 * @param autoResolvePropertiesDuringPlanning When true, every property encountered during planning will have an ID assigned to it.
 *                                            When false, only properties added by [[addProperty]] will have an ID.
 *
 */
case class StatisticsBackedLogicalPlanningConfigurationBuilder private (
  options: Options = Options(),
  cardinalities: Cardinalities = Cardinalities(),
  tokens: TokenContainer = TokenContainer(),
  indexes: Indexes = Indexes(),
  histograms: Set[Histogram] = Set.empty,
  existenceConstraints: Seq[ExistenceConstraintDefinition] = Seq.empty,
  propertyTypeConstraints: Seq[PropertyTypeDefinition] = Seq.empty,
  relationshipEndpointLabelConstraints: Seq[RelationshipEndpointLabelConstraintDefinition] = Seq.empty,
  nodeLabelConstraints: Map[String, Set[String]] = Map.empty,
  procedures: Set[ProcedureSignature] = Set.empty,
  functions: Set[UserFunctionSignature] = Set.empty,
  settings: Map[Setting[_], AnyRef] = Map.empty,
  dbMode: DatabaseMode = DatabaseMode.SINGLE,
  autoResolvePropertiesDuringPlanning: Boolean = true
) {

  def withSetting[T <: AnyRef](setting: Setting[T], value: T): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(settings = settings + (setting -> value))
  }

  def addLabel(label: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(tokens = tokens.addLabel(label))
  }

  def addRelType(relType: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(tokens = tokens.addRelType(relType))
  }

  def addProperty(prop: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(tokens = tokens.addProperty(prop))
  }

  def setPlannerVersion(version: CypherPlannerVersionOption): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(plannerVersionOption = version))
  }

  def setDatabaseMode(databaseMode: DatabaseMode): StatisticsBackedLogicalPlanningConfigurationBuilder =
    this.copy(dbMode = databaseMode)

  def setAllNodesCardinality(c: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(cardinalities = cardinalities.copy(allNodes = Some(c)))
  }

  def setLabelCardinality(label: String, c: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    addLabel(label)
      .copy(cardinalities = cardinalities.copy(labels = cardinalities.labels + (label -> c)))
  }

  def setLabelCardinalities(labelCardinalities: Map[String, Int])
    : StatisticsBackedLogicalPlanningConfigurationBuilder = {
    labelCardinalities.foldLeft(this) {
      case (builder, (label, c)) => builder.setLabelCardinality(label, c)
    }
  }

  def setAllRelationshipsCardinality(cardinality: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    setRelationshipCardinality(None, None, None, cardinality)
  }

  def setRelationshipCardinality(
    relDef: String,
    cardinality: Double
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    RelDef.fromString(relDef).foldLeft(this) {
      case (builder, rd) => builder.setRelationshipCardinality(rd.fromLabel, rd.relType, rd.toLabel, cardinality)
    }
  }

  def setRelationshipCardinality(
    from: Option[String] = None,
    rel: Option[String] = None,
    to: Option[String] = None,
    cardinality: Double
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val withFromLabel = from.foldLeft(this) {
      case (builder, label) => builder.addLabel(label)
    }

    val withRelType = rel.foldLeft(withFromLabel) {
      case (builder, rel) => builder.addRelType(rel)
    }

    val withToLabel = rel.foldLeft(withRelType) {
      case (builder, label) => builder.addLabel(label)
    }

    withToLabel.copy(cardinalities =
      cardinalities.copy(relationships = cardinalities.relationships + (RelDef(from, rel, to) -> cardinality))
    )
  }

  def defaultRelationshipCardinalityTo0(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(cardinalities = cardinalities.copy(defaultRelationshipCardinalityTo0 = enable))
  }

  def addNodeIndex(
    label: String,
    properties: Seq[String],
    existsSelectivity: Double,
    uniqueSelectivity: Double,
    isUnique: Boolean = false,
    indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.RANGE,
    maybeIndexCapability: Option[IndexCapability] = None
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexCapability = defaultIndexCapability(indexType, maybeIndexCapability)
    val indexDef = IndexDefinition(
      IndexDefinition.EntityType.Node(label),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = indexCapability.supportsReturningValues(),
      withOrdering =
        if (indexCapability.supportsOrdering()) IndexOrderCapability.BOTH else IndexOrderCapability.NONE,
      indexCapability = indexCapability
    )

    addLabel(label).addIndexDefAndProperties(indexDef, properties)
  }

  def addRelationshipIndex(
    relType: String,
    properties: Seq[String],
    existsSelectivity: Double,
    uniqueSelectivity: Double,
    isUnique: Boolean = false,
    indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.RANGE,
    maybeIndexCapability: Option[IndexCapability] = None
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexCapability = defaultIndexCapability(indexType, maybeIndexCapability)
    val indexDef = IndexDefinition(
      IndexDefinition.EntityType.Relationship(relType),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = indexCapability.supportsReturningValues(),
      withOrdering =
        if (indexCapability.supportsOrdering()) IndexOrderCapability.BOTH else IndexOrderCapability.NONE,
      indexCapability = indexCapability
    )

    addRelType(relType).addIndexDefAndProperties(indexDef, properties)
  }

  def addHistogram(histogram: Histogram): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(histograms = histograms + histogram)
  }

  private def defaultIndexCapability(
    indexType: graphdb.schema.IndexType,
    maybeIndexCapability: Option[IndexCapability]
  ): IndexCapability = {
    maybeIndexCapability match {
      case Some(value) => value
      case None => indexType match {
          case graphdb.schema.IndexType.TEXT  => IndexCapabilities.text_3_0
          case graphdb.schema.IndexType.RANGE => IndexCapabilities.range
          case graphdb.schema.IndexType.POINT => IndexCapabilities.point
          case graphdb.schema.IndexType.LOOKUP =>
            throw new IllegalArgumentException("Please provide a maybeIsQuerySupported for LOOKUP index")
          case graphdb.schema.IndexType.FULLTEXT =>
            throw new IllegalArgumentException("Please provide a maybeIsQuerySupported for FULLTEXT index")
          case graphdb.schema.IndexType.VECTOR =>
            throw new IllegalArgumentException("Please provide a maybeIsQuerySupported for VECTOR index")
        }

    }
  }

  def addNodeVectorIndex(
    indexName: String,
    labelNames: Seq[String],
    propertyKey: String,
    additionalProperties: Seq[String] = Seq.empty
  ): StatisticsBackedLogicalPlanningConfigurationBuilder =
    copy(indexes =
      indexes.addNodeVectorIndex(indexName, labelNames, propertyKey, additionalProperties)
    )

  def addRelationshipVectorIndex(
    indexName: String,
    relationshipTypeNames: Seq[String],
    propertyKey: String,
    additionalProperties: Seq[String] = Seq.empty
  ): StatisticsBackedLogicalPlanningConfigurationBuilder =
    copy(indexes =
      indexes.addRelationshipVectorIndex(
        indexName,
        relationshipTypeNames,
        propertyKey,
        additionalProperties
      )
    )

  def addNodeLookupIndex(orderCapability: IndexOrderCapability = IndexOrderCapability.BOTH)
    : StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.addNodeLookupIndex(orderCapability))
  }

  def removeNodeLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.removeNodeLookupIndex())
  }

  def addRelationshipLookupIndex(orderCapability: IndexOrderCapability = IndexOrderCapability.BOTH)
    : StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.addRelationshipLookupIndex(orderCapability))
  }

  def removeRelationshipLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.removeRelationshipLookupIndex())
  }

  private def addIndexDefAndProperties(
    indexDef: IndexDefinition,
    properties: Seq[String]
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val withProperties = properties.foldLeft(this) {
      case (builder, prop) => builder.addProperty(prop)
    }
    withProperties
      .copy(indexes = indexes.addPropertyIndex(indexDef))
  }

  def addNodeExistenceConstraint(
    label: String,
    property: String
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(label), property)

    addLabel(label).addProperty(property).copy(
      existenceConstraints = existenceConstraints :+ constraintDef
    )
  }

  def addRelationshipExistenceConstraint(
    relType: String,
    property: String
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(relType), property)

    addRelType(relType).addProperty(property).copy(
      existenceConstraints = existenceConstraints :+ constraintDef
    )
  }

  def addNodePropertyTypeConstraint(
    label: String,
    property: String,
    propertyType: ConstrainableType
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = PropertyTypeDefinition(IndexDefinition.EntityType.Node(label), property, propertyType)

    addLabel(label).addProperty(property).copy(
      propertyTypeConstraints = propertyTypeConstraints :+ constraintDef
    )
  }

  def addRelationshipPropertyTypeConstraint(
    relType: String,
    property: String,
    propertyType: ConstrainableType
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = PropertyTypeDefinition(IndexDefinition.EntityType.Relationship(relType), property, propertyType)

    addRelType(relType).addProperty(property).copy(
      propertyTypeConstraints = propertyTypeConstraints :+ constraintDef
    )
  }

  def addRelationshipEndpointLabelConstraint(
    relType: String,
    label: String,
    endPoint: EndpointType
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = RelationshipEndpointLabelConstraintDefinition(relType, label, endPoint)

    if (
      relationshipEndpointLabelConstraints.exists { c =>
        c.relType == relType && c.endPoint == endPoint
      }
    ) {
      err.println(
        s"""---------
           |WARNING: Duplicate relationship endpoint label constraint for `$relType` and `$endPoint`
           |---------""".stripMargin
      )
    }

    addRelType(relType)
      .addLabel(label)
      .copy(
        relationshipEndpointLabelConstraints = relationshipEndpointLabelConstraints :+ constraintDef
      )
  }

  def addRelationshipEndpointLabelConstraint(relDef: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    RelDef.fromString(relDef).foldLeft(this) {
      case (builder, RelDef(Some(startLabel), Some(relType), None)) =>
        builder.addRelationshipEndpointLabelConstraint(relType, startLabel, EndpointType.START)
      case (builder, RelDef(None, Some(relType), Some(endLabel))) =>
        builder.addRelationshipEndpointLabelConstraint(relType, endLabel, EndpointType.END)
      case (builder, RelDef(Some(startLabel), Some(relType), Some(endLabel))) =>
        builder
          .addRelationshipEndpointLabelConstraint(relType, startLabel, EndpointType.START)
          .addRelationshipEndpointLabelConstraint(relType, endLabel, EndpointType.END)

      case (_, pat) =>
        throw new IllegalArgumentException(
          s"Invalid relationship pattern `$pat` for relationship endpoint constraint. Expected relationship type and at least one of the labels to be set."
        )
    }
  }

  def addNodeLabelConstraint(
    constrainedLabel: String,
    impliedLabel: String
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    checkIsValidNodeLabelConstraint(constrainedLabel, impliedLabel)
    addLabel(constrainedLabel)
      .addLabel(impliedLabel)
      .copy(nodeLabelConstraints = nodeLabelConstraints.updatedWith(constrainedLabel) {
        case Some(labels) => Some(labels + impliedLabel)
        case None         => Some(Set(impliedLabel))
      })
  }

  private def checkIsValidNodeLabelConstraint(constrainedLabel: String, impliedLabel: String): Unit = {
    nodeLabelConstraints.foreach {
      case existing @ (label, labels) =>
        if (label == impliedLabel || labels.contains(constrainedLabel)) {
          throw new IllegalArgumentException(
            s"""Node label constraint conflict: ($constrainedLabel -> $impliedLabel) conflicts with $existing.
               |It is not allowed to have a label be the constrained label for one constraint and required label for another constraint.""".stripMargin
          )
        }
    }
  }

  def addProcedure(signature: ProcedureSignature): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(procedures = this.procedures + signature)
  }

  def addFunction(signature: UserFunctionSignature): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(functions = this.functions + signature)
  }

  private def fail(message: String): Nothing =
    throw new IllegalStateException(message)

  def enableDebugOption(
    option: CypherDebugOption,
    enable: Boolean = true
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options =
      options.copy(
        debug =
          if (enable)
            options.debug.copy(options.debug.enabledOptions + option)
          else
            options.debug.copy(options.debug.enabledOptions - option)
      )
    )
  }

  def enableIdpLogging(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    enableDebugOption(CypherDebugOption.printIDPLog, enable)
  }

  def enablePrintCostComparisons(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    enableDebugOption(CypherDebugOption.printCostComparisons, enable)
  }

  def enablePlanningStepsLogging(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    enableDebugOption(CypherDebugOption.logPlanningSteps, enable)
  }

  def enableConnectComponentsPlanner(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(connectComponentsPlanner = enable))
  }

  def enableMinimumGraphStatistics(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(useMinimumGraphStatistics = enable))
  }

  def enableDeduplicateNames(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(deduplicateNames = enable))
  }

  def setExecutionModel(executionModel: ExecutionModel): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(executionModel = executionModel))
  }

  def setParallelRepeatHeuristic(
    option: CypherParallelRepeatHeuristicOption
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(parallelRepeatHeuristic = option))
  }

  /**
   * Register all temporal functions. Assign consecutive IDs starting with the given initialId.
   */
  def registerTemporalFunctions(initialId: Int = 2): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    def functionName(namePart: String, nameParts: String*): FunctionName = {
      val parts = namePart +: nameParts
      FunctionName(Namespace(parts.dropRight(1).toList)(InputPosition.NONE), parts.last)(InputPosition.NONE)
    }

    var id = initialId - 1

    def nextId(): Int = {
      id += 1
      id
    }

    this
      .addFunction(UserFunctionSignature(
        functionName("datetime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDateTime,
        None,
        Some("Create a DateTime instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("datetime", "transaction"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDateTime,
        None,
        Some("Create a DateTime instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("datetime", "statement"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDateTime,
        None,
        Some("Create a DateTime instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("datetime", "realtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDateTime,
        None,
        Some("Create a DateTime instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("datetime", "truncate"),
        IndexedSeq(
          FieldSignature("unit", CTString),
          FieldSignature("temporalInstantValue", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT"))),
          FieldSignature("mapOfComponents", CTMap, Some(NO_VALUE))
        ),
        CTDateTime,
        None,
        Some("Create a DateTime instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("date"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDate,
        None,
        Some("Creates a `DATE` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("date", "transaction"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDate,
        None,
        Some("Creates a `DATE` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("date", "statement"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDate,
        None,
        Some("Creates a `DATE` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("date", "realtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTDate,
        None,
        Some("Creates a `DATE` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("date", "truncate"),
        IndexedSeq(
          FieldSignature("unit", CTString),
          FieldSignature("temporalInstantValue", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT"))),
          FieldSignature("mapOfComponents", CTMap, Some(NO_VALUE))
        ),
        CTDateTime,
        None,
        Some("Create a Date instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("time"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTTime,
        None,
        Some("Creates a `ZONED TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("time", "transaction"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTTime,
        None,
        Some("Creates a `ZONED TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("time", "statement"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTTime,
        None,
        Some("Creates a `ZONED TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("time", "realtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTTime,
        None,
        Some("Creates a `ZONED TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("time", "truncate"),
        IndexedSeq(
          FieldSignature("unit", CTString),
          FieldSignature("temporalInstantValue", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT"))),
          FieldSignature("mapOfComponents", CTMap, Some(NO_VALUE))
        ),
        CTDateTime,
        None,
        Some("Create a `ZONED TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localdatetime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalDateTime,
        None,
        Some("Creates a `LOCAL DATETIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localdatetime", "transaction"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalDateTime,
        None,
        Some("Creates a `LOCAL DATETIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localdatetime", "statement"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalDateTime,
        None,
        Some("Creates a `LOCAL DATETIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localdatetime", "realtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalDateTime,
        None,
        Some("Creates a `LOCAL DATETIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localdatetime", "truncate"),
        IndexedSeq(
          FieldSignature("unit", CTString),
          FieldSignature("temporalInstantValue", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT"))),
          FieldSignature("mapOfComponents", CTMap, Some(NO_VALUE))
        ),
        CTDateTime,
        None,
        Some("Create a `LOCAL DATETIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalTime,
        None,
        Some("Creates a `LOCAL TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localtime", "transaction"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalTime,
        None,
        Some("Creates a `LOCAL TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localtime", "statement"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalTime,
        None,
        Some("Creates a `LOCAL TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localtime", "realtime"),
        IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
        CTLocalTime,
        None,
        Some("Creates a `LOCAL TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("localtime", "truncate"),
        IndexedSeq(
          FieldSignature("unit", CTString),
          FieldSignature("temporalInstantValue", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT"))),
          FieldSignature("mapOfComponents", CTMap, Some(NO_VALUE))
        ),
        CTDateTime,
        None,
        Some("Create a `LOCAL TIME` instant"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("duration"),
        IndexedSeq(FieldSignature("Input", CTAny)),
        CTDuration,
        None,
        Some("Creates a `DURATION` value"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("duration", "between"),
        IndexedSeq(FieldSignature("from", CTAny), FieldSignature("to", CTAny)),
        CTDuration,
        None,
        Some("Creates a `DURATION` value"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("duration", "inMonths"),
        IndexedSeq(FieldSignature("from", CTAny), FieldSignature("to", CTAny)),
        CTDuration,
        None,
        Some("Creates a `DURATION` value"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("duration", "inDays"),
        IndexedSeq(FieldSignature("from", CTAny), FieldSignature("to", CTAny)),
        CTDuration,
        None,
        Some("Creates a `DURATION` value"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
      .addFunction(UserFunctionSignature(
        functionName("duration", "inSeconds"),
        IndexedSeq(FieldSignature("from", CTAny), FieldSignature("to", CTAny)),
        CTDuration,
        None,
        Some("Creates a `DURATION` value"),
        isAggregate = false,
        id = nextId(),
        builtIn = true
      ))
  }

  def processGraphCountFile(fileName: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val graphCountData = GraphCountsJson.getGraphCounts(new File(fileName))
    processGraphCounts(graphCountData)
  }

  /**
   * Process graph count data and return a builder with updated constraints, indexes and counts.
   */
  def processGraphCounts(graphCountData: GraphCountData): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val bare = this
      // Lookup indexes are present in the Graph counts, if they exist.
      .removeNodeLookupIndex()
      .removeRelationshipLookupIndex()
      // Graph counts do not capture relationship counts with both from and to label, because they are not actually present in the count store.
      // So when using graphCountData, we should always use MinimumGraphStatistics, just like in the real product.
      .enableMinimumGraphStatistics()
      // Graph counts may lack relationship counts if they are 0
      .defaultRelationshipCardinalityTo0()
      // Temporal functions are registered as UDFs. Adding them here to make it easier to reproduce support cases.
      .registerTemporalFunctions()

    val withNodes = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.nodes.foldLeft(builder) {
        case (builder, NodeCount(count, None))        => builder.setAllNodesCardinality(count)
        case (builder, NodeCount(count, Some(label))) => builder.setLabelCardinality(label, count)
      }

    val withRelationships = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.relationships.foldLeft(builder) {
        case (builder, RelationshipCount(count, relationshipType, startLabel, endLabel)) =>
          builder.setRelationshipCardinality(startLabel, relationshipType, endLabel, count)
      }

    val withConstraints = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.constraints.foldLeft(builder) {
        case (builder, Constraint(Some(label), None, Seq(property), None, ConstraintType.EXISTS, Seq(), _)) =>
          builder.addNodeExistenceConstraint(label, property)
        case (builder, Constraint(None, Some(relType), Seq(property), None, ConstraintType.EXISTS, Seq(), _)) =>
          builder.addRelationshipExistenceConstraint(relType, property)
        case (builder, Constraint(_, _, _, _, ConstraintType.UNIQUE, Seq(), _)) =>
          builder // Will get found by matchingUniquenessConstraintExists
        case (builder, Constraint(Some(label), None, properties, None, ConstraintType.UNIQUE_EXISTS, Seq(), _)) =>
          properties.foldLeft(builder)(_.addNodeExistenceConstraint(label, _))
        case (builder, Constraint(None, Some(relType), properties, None, ConstraintType.UNIQUE_EXISTS, Seq(), _)) =>
          properties.foldLeft(builder)(_.addRelationshipExistenceConstraint(relType, _))
        case (
            builder,
            Constraint(Some(label), None, Nil, Some(enforcedLabel), ConstraintType.NODE_LABEL_EXISTENCE, _, _)
          ) =>
          builder.addNodeLabelConstraint(label, enforcedLabel)
        case (
            builder,
            Constraint(
              None,
              Some(relType),
              Nil,
              Some(enforcedLabel),
              ConstraintType.RELATIONSHIP_ENDPOINT_LABEL,
              _,
              Some(endpointType)
            )
          ) =>
          builder.addRelationshipEndpointLabelConstraint(relType, enforcedLabel, endpointType)
        case (_, constraint) => throw new IllegalArgumentException(s"Unsupported constraint: $constraint")
      }

    val withIndexes = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.indexes.foldLeft(builder) {
        case (_, index @ Index(_, _, FULLTEXT, _, _, _, _, _)) =>
          throw new IllegalArgumentException(s"Unsupported index of type FULLTEXT: $index")
        case (builder, Index(Some(Seq()), None, LOOKUP, Seq(), _, _, _, _)) =>
          builder.addNodeLookupIndex()
        case (builder, Index(None, Some(Seq()), LOOKUP, Seq(), _, _, _, _)) =>
          builder.addRelationshipLookupIndex()
        case (
            builder,
            i @ Index(Some(Seq(label)), None, indexType, properties, totalSize, estimatedUniqueSize, _, indexProvider)
          ) =>
          val existsSelectivity = math.min(1.0, totalSize / builder.cardinalities.labels(label))
          val uniqueSelectivity = 1.0 / estimatedUniqueSize
          val isUnique = graphCountData.matchingUniquenessConstraintExists(i)
          val maybeIndexCapability = indexProvider.name() match {
            case "text-1.0" => Some(IndexCapabilities.text_1_0)
            case "text-2.0" => Some(IndexCapabilities.text_2_0)
            case "text-3.0" => Some(IndexCapabilities.text_3_0)
            case _          => None
          }
          builder.addNodeIndex(
            label,
            properties,
            existsSelectivity,
            uniqueSelectivity,
            isUnique = isUnique,
            indexType.toPublicApi,
            maybeIndexCapability = maybeIndexCapability
          )
        case (
            builder,
            i @ Index(None, Some(Seq(relType)), indexType, properties, totalSize, estimatedUniqueSize, _, indexProvider)
          ) =>
          val existsSelectivity = totalSize / builder.cardinalities.getRelCount(RelDef(None, Some(relType), None))
          val uniqueSelectivity = 1.0 / estimatedUniqueSize
          val isUnique = graphCountData.matchingUniquenessConstraintExists(i)
          val maybeIndexCapability = indexProvider.name() match {
            case "text-1.0" => Some(IndexCapabilities.text_1_0)
            case "text-2.0" => Some(IndexCapabilities.text_2_0)
            case "text-3.0" => Some(IndexCapabilities.text_3_0)
            case _          => None
          }
          builder.addRelationshipIndex(
            relType,
            properties,
            existsSelectivity,
            uniqueSelectivity,
            isUnique = isUnique,
            indexType.toPublicApi,
            maybeIndexCapability = maybeIndexCapability
          )
        case (_, index) => throw new IllegalArgumentException(s"Unsupported index: $index")
      }

    (withNodes
      andThen withRelationships
      andThen withConstraints
      andThen withIndexes)(bare)
  }

  def processApocMeta(stats: ApocMetaStats): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val base =
      this
        // Graph counts may lack relationship counts if they are 0
        .defaultRelationshipCardinalityTo0()
        .setAllNodesCardinality(stats.nodeCount)
        .setLabelCardinalities(stats.labels)
        .setAllRelationshipsCardinality(stats.relCount)

    stats.relTypes.foldLeft(base) {
      case (builder, (relDef, cardinality)) =>
        builder.setRelationshipCardinality(relDef, cardinality)
    }
  }

  def setTxStateHasChanges(hasChanges: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(txStateHasChanges = hasChanges))
  }

  def addSemanticFeature(sf: SemanticFeature): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(semanticFeatures = options.semanticFeatures :+ sf))
  }

  def enablePlanningIntersectionScans(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseInternalSettings.planning_intersection_scans_enabled, Boolean.box(enabled))
  }

  def enablePlanningDynamicLabelScans(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder =
    withSetting(GraphDatabaseInternalSettings.cypher_enable_dynamic_label_scan, Boolean.box(enabled))

  def enablePlanningDynamicLabelIndexUse(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder =
    withSetting(GraphDatabaseInternalSettings.cypher_enable_dynamic_label_index_use, Boolean.box(enabled))

  def enableGraphSchemaOptimizations(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseInternalSettings.planning_graph_schema_optimizations_enabled, Boolean.box(enabled))
  }

  def enableSchemaInference(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val strategy =
      if (enabled)
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      else
        GraphDatabaseSettings.InferSchemaPartsStrategy.OFF
    withSetting(GraphDatabaseSettings.cypher_infer_schema_parts_strategy, strategy)
  }

  def enableGraphTypes(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseInternalSettings.graph_type_enabled, Boolean.box(enabled))
  }

  def setDatabaseReferenceRepository(
    databaseReferenceRepository: DatabaseReferenceRepository
  ): StatisticsBackedLogicalPlanningConfigurationBuilder =
    this.copy(options = options.copy(databaseReferenceRepository = databaseReferenceRepository))

  def enablePrintNotifications(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder =
    this.copy(options = options.copy(printNotifications = enabled))

  def setDatabaseFormat(dbFormat: DatabaseFormat): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseSettings.db_format, dbFormat.settingValue)
  }

  def setAutoResolvePropertiesDuringPlanning(enabled: Boolean): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    copy(autoResolvePropertiesDuringPlanning = enabled)
  }

  def build(): StatisticsBackedLogicalPlanningConfiguration = {
    require(cardinalities.allNodes.isDefined, "Please specify allNodesCardinality using `setAllNodesCardinality`.")
    cardinalities.allNodes.foreach(anc =>
      cardinalities.labels.values.foreach(lc =>
        require(anc >= lc, s"Label cardinality ($lc) was greater than all nodes cardinality ($anc)")
      )
    )

    cardinalities.relationships.get(RelDef.all).foreach(arc =>
      cardinalities.relationships.values.foreach(rc =>
        require(arc >= rc, s"Relationship cardinality ($rc) was greater than all relationships cardinality ($arc)")
      )
    )

    nodeLabelConstraints.foreach { case (constrainedLabel, impliedLabels) =>
      impliedLabels.foreach { impliedLabel =>
        cardinalities.labels.get(impliedLabel).foreach { impliedLabelCardinality =>
          cardinalities.labels.get(constrainedLabel).foreach { constrainedLabelCardinality =>
            require(
              impliedLabelCardinality >= constrainedLabelCardinality,
              s"""---------
                 |Inconsistent cardinality for node label constraint:
                 |$impliedLabelCardinality (`$impliedLabel`) < $constrainedLabelCardinality (`$constrainedLabel`)
                 |---------
                 |""".stripMargin
            )
          }
        }
      }
    }

    relationshipEndpointLabelConstraints.foreach { constraint =>
      val specificString =
        if (constraint.endPoint == EndpointType.START) s"(:${constraint.label})-[:${constraint.relType}]->()"
        else s"()-[:${constraint.relType}]->(:${constraint.label})"
      cardinalities.relationships.get(RelDef.fromString(specificString).head).foreach { specificCardinality =>
        val genericString = s"()-[:${constraint.relType}]->()"
        cardinalities.relationships.get(RelDef.fromString(genericString).head).foreach { generalCardinality =>
          require(
            // Generally, we know that the specific cardinality should be less than or equal to the general one.
            // With the constraint, we know that all general relationship are of the specific kind.
            // Thus, from <= and >= we can conclude ==.
            specificCardinality == generalCardinality,
            s"""---------
               |Inconsistent cardinality for relationship with endpoint label constraint:
               |$specificCardinality (`$specificString`) != $generalCardinality (`$genericString`)
               |---------""".stripMargin
          )
        }
      }
    }

    val resolver = tokens.getResolver(procedures, functions, autoResolvePropertiesDuringPlanning)

    // Get the parsed histograms from the config
    val plannerConfiguration = CypherPlannerConfiguration.withSettings(settings)
    val histogramsFromConfig = plannerConfiguration.histograms

    val internalGraphStatistics = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = cardinalities.allNodes.get

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.map(_.id)
          .map(resolver.getLabelName)
          .map(label =>
            cardinalities.labels.getOrElse(
              label,
              fail(
                s"""No cardinality set for label $label. Please specify using
                   |.setLabelCardinality("$label", cardinality)""".stripMargin
              )
            )
          )
          .map(Cardinality.apply)
          .getOrElse(Cardinality.EMPTY)
      }

      override def patternStepCardinality(
        fromLabelId: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabelId: Option[LabelId]
      ): Cardinality = {
        val relDef = RelDef(
          fromLabel = fromLabelId.map(_.id).map(resolver.getLabelName),
          relType = relTypeId.map(_.id).map(resolver.getRelTypeName),
          toLabel = toLabelId.map(_.id).map(resolver.getLabelName)
        )
        cardinalities.getRelCount(relDef)
      }

      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.propertyIndexes.find { indexDef =>
          indexDef.indexType == index.indexType.toPublicApi &&
          indexDef.entityType == resolveEntityType(index) &&
          indexDef.propertyKeys == index.properties.map(_.id).map(resolver.getPropertyKeyName)
        }.flatMap(indexDef => Selectivity.of(indexDef.uniqueValueSelectivity))
      }

      override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.propertyIndexes.find { indexDef =>
          indexDef.indexType == index.indexType.toPublicApi &&
          indexDef.entityType == resolveEntityType(index) &&
          indexDef.propertyKeys == index.properties.map(_.id).map(resolver.getPropertyKeyName)
        }.flatMap(indexDef => Selectivity.of(indexDef.propExistsSelectivity))
      }

      private def resolveEntityType(index: IndexDescriptor): IndexDefinition.EntityType = index.entityType match {
        case IndexDescriptor.EntityType.Node(label) =>
          IndexDefinition.EntityType.Node(resolver.getLabelName(label.id))
        case IndexDescriptor.EntityType.Relationship(relType) =>
          IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relType.id))
      }

      override def mostCommonLabelGivenRelationshipType(typ: Int): Seq[Int] = {
        val givenRelationshipTypeName = resolver.getRelTypeName(typ)
        cardinalities.relationships.view.collect {
          case (RelDef(Some(fromLabelName), Some(relationshipTypeName), None), cardinality)
            if relationshipTypeName == givenRelationshipTypeName =>
            (cardinality, resolver.getLabelId(fromLabelName))
          case (RelDef(None, Some(relationshipTypeName), Some(toLabelName)), cardinality)
            if relationshipTypeName == givenRelationshipTypeName =>
            (cardinality, resolver.getLabelId(toLabelName))
        }.groupMap(_._1)(_._2).maxByOption(_._1).map(_._2.toSeq).getOrElse(Seq.empty)
      }

      override def getHistograms(labels: Set[LabelId], propertyKey: PropertyKeyId): Set[Histogram] = {
        val resolver = tokens.getResolver(procedures, functions, autoResolvePropertiesDuringPlanning)
        histograms.filter(histogram =>
          histogram.nodeOrRelationship == NODE_TYPE &&
            resolver.getOptPropertyKeyId(
              histogram.property
            ).map(PropertyKeyId.apply).contains(propertyKey) && labels.contains(
              LabelId(resolver.getLabelId(histogram.labelOrTypeName))
            )
        ) ++ histogramsFromConfig.filter(histogram =>
          histogram.nodeOrRelationship == NODE_TYPE &&
            resolver.getOptPropertyKeyId(histogram.property).map(PropertyKeyId.apply).contains(propertyKey) &&
            labels.contains(LabelId(resolver.getLabelId(histogram.labelOrTypeName)))
        )
      }

      override def getHistograms(typeId: RelTypeId, propertyKey: PropertyKeyId): Set[Histogram] = {
        val resolver = tokens.getResolver(procedures, functions, autoResolvePropertiesDuringPlanning)

        histograms.filter(histogram =>
          histogram.nodeOrRelationship == RELATIONSHIP_TYPE &&
            resolver.getOptPropertyKeyId(histogram.property).map(PropertyKeyId.apply).contains(propertyKey) &&
            resolver.getOptRelTypeId(histogram.labelOrTypeName).map(RelTypeId.apply).contains(typeId)
        ) ++ histogramsFromConfig.filter(histogram =>
          histogram.nodeOrRelationship == RELATIONSHIP_TYPE &&
            resolver.getOptPropertyKeyId(histogram.property).map(PropertyKeyId.apply).contains(propertyKey) &&
            resolver.getOptRelTypeId(histogram.labelOrTypeName).map(RelTypeId.apply).contains(typeId)
        )
      }
    }

    val graphStatistics =
      if (options.useMinimumGraphStatistics) {
        new MinimumGraphStatistics(internalGraphStatistics)
      } else {
        internalGraphStatistics
      }
    val planContext: PlanContext = new NotImplementedPlanContext() {

      override def databaseMode: DatabaseMode = dbMode

      override def storageHasPropertyColocation: Boolean =
        Set[DatabaseFormat](DatabaseFormat.Block, DatabaseFormat.Mvcc).contains(dbFormatFromSettings)

      override def storageSupportsFastExpandInto: Boolean =
        Set[DatabaseFormat](DatabaseFormat.Block, DatabaseFormat.Mvcc).contains(dbFormatFromSettings)

      override def storageIsMvcc: Boolean = dbFormatFromSettings == DatabaseFormat.Mvcc

      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(graphStatistics, new MutableGraphStatisticsSnapshot())

      override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.RANGE)
      }

      override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.RANGE)
      }

      override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.TEXT)
      }

      override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.TEXT)
      }

      override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.POINT)
      }

      override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.POINT)
      }

      override def procedureSignatureVersion: Long = -1

      private def indexesGetForEntityAndIndexType(
        entityType: IndexDefinition.EntityType,
        indexType: graphdb.schema.IndexType
      ): Iterator[IndexDescriptor] = {
        indexes.propertyIndexes.collect {
          case indexDef @ IndexDefinition(`entityType`, `indexType`, _, _, _, _, _, _, _) =>
            newIndexDescriptor(indexDef)
        }.flatten
      }.iterator

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = {
        indexes.propertyIndexes.toIterator.flatMap(newIndexDescriptor)
      }

      override def nodeTokenIndex: Option[TokenIndexDescriptor] = indexes.nodeLookupIndex

      override def relationshipTokenIndex: Option[TokenIndexDescriptor] = indexes.relationshipLookupIndex

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        existenceConstraints.collect {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(`labelName`), property) => property
        }.toSet
      }

      override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
        existenceConstraints.exists {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(`labelName`), `propertyKey`) => true
          case _                                                                                          => false
        }
      }

      override def getRelationshipPropertiesWithExistenceConstraint(relTypeName: String): Set[String] = {
        existenceConstraints.collect {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), property) =>
            property
        }.toSet
      }

      override def getPropertiesWithExistenceConstraint: Set[String] = {
        existenceConstraints.collect {
          case ExistenceConstraintDefinition(_, property) => property
        }.toSet
      }

      override def hasRelationshipPropertyExistenceConstraint(relTypeName: String, propertyKey: String): Boolean = {
        existenceConstraints.exists {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), `propertyKey`) =>
            true
          case _ => false
        }
      }

      override def hasNodePropertyTypeConstraint(
        labelName: String,
        propertyKey: String,
        cypherType: ConstrainableType
      ): Boolean = {
        propertyTypeConstraints.exists {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Node(`labelName`), `propertyKey`, `cypherType`) => true
          case _ => false
        }
      }

      override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[ConstrainableType]] = {
        propertyTypeConstraints.collect {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Node(`labelName`), property, cypherType) =>
            property -> Seq(cypherType)
        }.toMap
      }

      override def hasRelationshipPropertyTypeConstraint(
        relTypeName: String,
        propertyKey: String,
        cypherType: ConstrainableType
      ): Boolean = {
        propertyTypeConstraints.exists {
          case PropertyTypeDefinition(
              IndexDefinition.EntityType.Relationship(`relTypeName`),
              `propertyKey`,
              `cypherType`
            ) => true
          case _ => false
        }
      }

      override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String)
        : Map[String, Seq[ConstrainableType]] = {
        propertyTypeConstraints.collect {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), property, cypherType) =>
            property -> Seq(cypherType)
        }.toMap
      }

      override def hasRelationshipEndpointLabelConstraint(
        relTypeName: String,
        labelName: String,
        endpointType: EndpointType
      ): Boolean =
        relationshipEndpointLabelConstraints.contains(RelationshipEndpointLabelConstraintDefinition(
          relTypeName,
          labelName,
          endpointType
        ))

      override def getRelationshipEndpointLabelConstraints(relTypeName: String): Map[EndpointType, String] =
        relationshipEndpointLabelConstraints
          .filter(_.relType == relTypeName)
          .map { constraint =>
            constraint.endPoint -> constraint.label
          }
          .toMap

      override def hasNodeLabelConstraint(constrainedLabel: String, impliedLabel: String): Boolean =
        nodeLabelConstraints.get(constrainedLabel).exists(_.contains(impliedLabel))

      override def getNodeLabelConstraints(constrainedLabel: String): Set[String] =
        nodeLabelConstraints.getOrElse(constrainedLabel, Set.empty)

      override def procedureSignature(name: ProcedureName): ProcedureSignature = {
        procedures.find(_.name == name).getOrElse(fail(s"No procedure signature for $name"))
      }

      override def functionSignature(name: FunctionName): Option[UserFunctionSignature] = {
        functions.find(_.name == name)
      }

      override def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        textIndexGetForLabelAndProperties(labelName, propertyKeys).nonEmpty
      }

      override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        rangeIndexGetForLabelAndProperties(labelName, propertyKeys).nonEmpty
      }

      override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        pointIndexGetForLabelAndProperties(labelName, propertyKeys).nonEmpty
      }

      override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = {
        textIndexGetForRelTypeAndProperties(relTypeName, propertyKeys).nonEmpty
      }

      override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = {
        rangeIndexGetForRelTypeAndProperties(relTypeName, propertyKeys).nonEmpty
      }

      override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = {
        pointIndexGetForRelTypeAndProperties(relTypeName, propertyKeys).nonEmpty
      }

      override def textIndexGetForLabelAndProperties(
        labelName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.TEXT)
      }

      override def rangeIndexGetForLabelAndProperties(
        labelName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.RANGE)
      }

      override def pointIndexGetForLabelAndProperties(
        labelName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.POINT)
      }

      override def textIndexGetForRelTypeAndProperties(
        relTypeName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(relTypeName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.TEXT)
      }

      override def rangeIndexGetForRelTypeAndProperties(
        relTypeName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(relTypeName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.RANGE)
      }

      override def pointIndexGetForRelTypeAndProperties(
        relTypeName: String,
        propertyKeys: Seq[String]
      ): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(relTypeName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.POINT)
      }

      private def indexGetForEntityTypePropertiesAndIndexType(
        entityType: IndexDefinition.EntityType,
        propertyKeys: Seq[String],
        indexType: graphdb.schema.IndexType
      ): Option[IndexDescriptor] = {
        indexes.propertyIndexes.collect {
          case indexDef @ IndexDefinition(`entityType`, `indexType`, `propertyKeys`, _, _, _, _, _, _) =>
            newIndexDescriptor(indexDef)
        }.flatten.headOption
      }

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
        resolver.getOptPropertyKeyId(propertyKeyName)
      }

      override def getOptLabelId(labelName: String): Option[Int] =
        resolver.getOptLabelId(labelName)

      override def getOptRelTypeId(relType: String): Option[Int] =
        resolver.getOptRelTypeId(relType)

      override def txStateHasChanges(): Boolean = options.txStateHasChanges

      override def getLabelName(id: Int): String = resolver.getLabelName(id)

      override def getRelTypeName(id: Int): String = resolver.getRelTypeName(id)

      override def getPropertyKeyName(id: Int): String = resolver.getPropertyKeyName(id)

      override def getPropertyKeyId(propertyKeyName: String): Int = resolver.getPropertyKeyId(propertyKeyName)

      private def newIndexDescriptor(indexDef: IndexDefinition): Option[IndexDescriptor] = {
        val canGetValue = if (indexDef.withValues) CanGetValue else DoNotGetValue

        val props = indexDef.propertyKeys.map(p => PropertyKeyId(resolver.getPropertyKeyId(p)))

        val entityType = indexDef.entityType match {
          case EntityType.Node(label) =>
            IndexDescriptor.EntityType.Node(LabelId(resolver.getLabelId(label)))
          case EntityType.Relationship(relType) =>
            IndexDescriptor.EntityType.Relationship(RelTypeId(resolver.getRelTypeId(relType)))
        }

        IndexDescriptor.IndexType.fromPublicApi(indexDef.indexType) map { indexType =>
          IndexDescriptor(
            indexType,
            entityType,
            props,
            valueCapability = canGetValue,
            orderCapability = indexDef.withOrdering,
            maybeKernelIndexCapability = Some(indexDef.indexCapability),
            isUnique = indexDef.isUnique
          )
        }
      }

      override def nodeVectorIndexByName(indexName: String): Either[VectorIndexError, NodeVectorIndexDescriptor] =
        indexes.vectorIndexes.collectFirst {
          case NodeVectorIndexDefinition(name, labels, property, additionalProperties) if name == indexName =>
            Right(NodeVectorIndexDescriptor(
              labelIds = labels.map {
                case EntityType.Node(label) => LabelId(resolver.getLabelId(label))
              },
              property = PropertyKeyId(resolver.getPropertyKeyId(property)),
              additionalProperties = additionalProperties.map(prop => PropertyKeyId(resolver.getPropertyKeyId(prop)))
            ))
          case RelationshipVectorIndexDefinition(name, _, _, _) if name == indexName =>
            Left(VectorIndexError.WrongEntityType(common.EntityType.NODE, common.EntityType.RELATIONSHIP))
        }.getOrElse(Left(VectorIndexError.NotFound))

      override def relationshipVectorIndexByName(indexName: String)
        : Either[VectorIndexError, RelationshipVectorIndexDescriptor] =
        indexes.vectorIndexes.collectFirst {
          case NodeVectorIndexDefinition(name, _, _, _) if name == indexName =>
            Left(VectorIndexError.WrongEntityType(common.EntityType.RELATIONSHIP, common.EntityType.NODE))
          case RelationshipVectorIndexDefinition(name, relTypes, property, additionalProperties) if name == indexName =>
            Right(RelationshipVectorIndexDescriptor(
              relTypeIds = relTypes.map(relType => RelTypeId(resolver.getRelTypeId(relType.relType))),
              property = PropertyKeyId(resolver.getPropertyKeyId(property)),
              additionalProperties = additionalProperties.map(prop => PropertyKeyId(resolver.getPropertyKeyId(prop)))
            ))
        }.getOrElse(Left(NotFound))
    }
    new StatisticsBackedLogicalPlanningConfiguration(
      resolver,
      planContext,
      options,
      allSettings
    )
  }

  private def allSettings: Map[Setting[_], AnyRef] = defaultSettingsOverrides ++ settings

  private def dbFormatFromSettings: DatabaseFormat = {
    allSettings.get(GraphDatabaseSettings.db_format).fold(DatabaseFormat.default) { dbFormatSetting =>
      Seq(DatabaseFormat.Block, DatabaseFormat.Aligned, DatabaseFormat.Mvcc)
        .find(_.settingValue == dbFormatSetting)
        .getOrElse(throw new IllegalArgumentException(s"Unknown database format: $dbFormatSetting"))
    }
  }
}

class StatisticsBackedLogicalPlanningConfiguration(
  resolver: LogicalPlanResolver,
  // We want to be able to inspect the planContext in tests, which is why it is public
  val planContext: PlanContext,
  options: StatisticsBackedLogicalPlanningConfigurationBuilder.Options,
  settings: Map[Setting[_], AnyRef]
) extends LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {

  // Hack to guarantee coverage in all versions
  def plan(query: String): LogicalPlan = plan(randomVersion(), query)

  def plan(version: CypherVersion, query: String): LogicalPlan = planState(version, query).logicalPlan

  // Hack to guarantee coverage in all versions
  def planState(query: String): LogicalPlanState = planState(randomVersion(), query)

  def planState(
    version: CypherVersion,
    queryString: String
  ): LogicalPlanState = {
    doPlan(version, queryString, recordCostComparisonCandidates = false).logicalPlanState
  }

  def planAndRecordCostComparisonCandidates(
    version: CypherVersion,
    query: String
  ): (LogicalPlan, ListSet[LogicalPlan]) = {
    val result = doPlan(version, query, recordCostComparisonCandidates = true)
    (result.logicalPlanState.logicalPlan, result.recordedCostComparisonCandidates)
  }

  private def doPlan(
    version: CypherVersion,
    queryString: String,
    recordCostComparisonCandidates: Boolean
  ): PlanningResult = {
    val plannerConfiguration = CypherPlannerConfiguration.withSettings(settings)

    val cfg = Config.defaults(settings.asJava)
    val cc = CypherConfiguration.fromConfig(cfg)
    val labelInference = CypherInferSchemaPartsOption.reader.read(OptionReader.Input(
      cc,
      Set.empty
    )).result

    val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
    val labelInferenceStrategy = LabelInferenceStrategy.fromConfig(
      planContext,
      labelInference,
      CypherPlannerVersionWithOptimisations.allSupportedOptimisations(options.plannerVersionOption)
    )
    val metrics = SimpleMetricsFactory.newMetrics(
      planContext,
      simpleExpressionEvaluator,
      options.executionModel,
      CancellationChecker.neverCancelled(),
      labelInferenceStrategy
    )

    val notificationLogger = new RecordingNotificationLogger()
    val context = ContextHelper.create(
      version = version,
      planContext = planContext,
      cypherExceptionFactory = exceptionFactory,
      queryGraphSolver = queryGraphSolver(plannerConfiguration),
      metrics = metrics,
      config = plannerConfiguration,
      logicalPlanIdGen = idGen,
      debugOptions = options.debug,
      executionModel = options.executionModel,
      statefulShortestPlanningMode = plannerConfiguration.statefulShortestPlanningMode(),
      planVarExpandInto = plannerConfiguration.planVarExpandInto(),
      parallelRepeatHeuristic = options.parallelRepeatHeuristic,
      databaseReferenceRepository = options.databaseReferenceRepository,
      labelInferenceStrategy = labelInferenceStrategy,
      notificationLogger = notificationLogger,
      semanticFeatures = options.semanticFeatures
    )
    val state = InitialState(queryString, IDPPlannerName, new AnonymousVariableNameGenerator)
    val parsingConfig = {
      val cfg = LogicalPlanningTestSupport2.defaultParsingConfig
      cfg.copy(
        resolveSimpleDynamicExpressions = cc.resolveSimpleDynamicExpressions
      )
    }

    val recordingCostComparisonListener = new RecordingCostComparisonListener()
    val finalState = {
      val costComparisonListener =
        if (recordCostComparisonCandidates) recordingCostComparisonListener
        else devNullListener

      CostComparisonListener.withScopedTestListener(costComparisonListener) {
        LogicalPlanningTestSupport2
          .pipeLine(
            parsingConfig = parsingConfig,
            deduplicateNames = options.deduplicateNames,
            allowSubqueryDuplicationInCnfNormalizer = cc.allowDuplicatingSubqueryExpressionsInCnfNormalizer
          )
          .transform(state, context)
      }
    }

    if (options.printNotifications) {
      printOutNotifications(notificationLogger.notifications)
    }

    PlanningResult(finalState, recordingCostComparisonListener.result())
  }

  private def printOutNotifications(notifications: Set[InternalNotification]): Unit =
    notifications match {
      case notifications if notifications.isEmpty => ()
      case notifications =>
        println("Notifications occurred:")
        notifications
          .map(NotificationWrapping.asKernelNotification(None))
          .map(_.getDescription)
          .foreach(println)
    }

  def planBuilder(language: CypherVersion = CypherVersion.Legacy.legacyVersion()): LogicalPlanBuilder =
    new LogicalPlanBuilder(wholePlan = true, resolver, language = language)

  def subPlanBuilder(language: CypherVersion = CypherVersion.Legacy.legacyVersion()): LogicalPlanBuilder =
    new LogicalPlanBuilder(wholePlan = false, resolver, language = language)

  def queryGraphSolver(): QueryGraphSolver = {
    queryGraphSolver(CypherPlannerConfiguration.withSettings(settings))
  }

  def queryGraphSolver(plannerConfiguration: CypherPlannerConfiguration): QueryGraphSolver = {
    val iDPSolverConfig =
      new ConfigurableIDPSolverConfig(
        plannerConfiguration.idpMaxTableSize(),
        plannerConfiguration.idpIterationDuration()
      )

    if (options.connectComponentsPlanner) {
      LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents.queryGraphSolver(
        iDPSolverConfig,
        options.debug.disableExistsSubqueryCaching
      )
    } else {
      LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents.queryGraphSolver(
        iDPSolverConfig,
        options.debug.disableExistsSubqueryCaching
      )
    }
  }
}

object StatisticsBackedLogicalPlanningConfiguration {

  case class PlanningResult(logicalPlanState: LogicalPlanState, recordedCostComparisonCandidates: ListSet[LogicalPlan])

  class RecordingCostComparisonListener() extends CostComparisonListener {

    private val recordedPlans = ListSet.newBuilder[LogicalPlan]

    def result(): ListSet[LogicalPlan] = recordedPlans.result()

    override def report[X, Score: Ordering](
      projector: X => LogicalPlan,
      input: Iterable[X],
      calculateScore: X => Score,
      context: LogicalPlanningContext,
      resolved: => String,
      resolvedPerPlan: LogicalPlan => String,
      heuristic: SelectorHeuristic,
      planDescriptor: X => Option[String]
    ): Unit = {
      recordedPlans.addAll(input.map(projector))
    }
  }
}
