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
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.graphcounts.Constraint
import org.neo4j.cypher.graphcounts.GraphCountData
import org.neo4j.cypher.graphcounts.Index
import org.neo4j.cypher.graphcounts.NodeCount
import org.neo4j.cypher.graphcounts.RelationshipCount
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.helpers.TokenContainer
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Cardinalities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.ExistenceConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Indexes
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Options
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.PropertyTypeDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelDef
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getProvidesOrder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getWithValues
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.LabelInferenceOption
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.graphdb
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.IndexType.FULLTEXT
import org.neo4j.internal.schema.IndexType.LOOKUP
import org.neo4j.internal.schema.IndexType.POINT
import org.neo4j.internal.schema.IndexType.RANGE
import org.neo4j.internal.schema.IndexType.TEXT
import org.neo4j.internal.schema.IndexType.VECTOR
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.kernel.database.DatabaseReferenceRepository

trait StatisticsBackedLogicalPlanningSupport {

  /**
   * @return an immutable builder to construct [[StatisticsBackedLogicalPlanningConfiguration]]s.
   */
  protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    StatisticsBackedLogicalPlanningConfigurationBuilder.newBuilder()
}

object StatisticsBackedLogicalPlanningConfigurationBuilder {

  def newBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    StatisticsBackedLogicalPlanningConfigurationBuilder()

  case class Options(
    debug: CypherDebugOptions = CypherDebugOptions(Set(CypherDebugOption.verboseEagernessReasons)),
    connectComponentsPlanner: Boolean = true,
    useLabelInference: LabelInferenceOption = LabelInferenceOption.default,
    executionModel: ExecutionModel = ExecutionModel.default,
    useMinimumGraphStatistics: Boolean = false,
    txStateHasChanges: Boolean = false,
    deduplicateNames: Boolean = true,
    semanticFeatures: Seq[SemanticFeature] = Seq.empty,
    databaseReferenceRepository: DatabaseReferenceRepository = ContextHelper.mockDatabaseReferenceRepository
  )

  case class Cardinalities(
    allNodes: Option[Double] = None,
    labels: Map[String, Double] = Map[String, Double](),
    relationships: Map[RelDef, Double] = Map[RelDef, Double](),
    defaultRelationshipCardinalityTo0: Boolean = true
  ) {

    def getRelCount(relDef: RelDef): Double = {
      def orElse: Double =
        if (defaultRelationshipCardinalityTo0) 0.0
        else throw new IllegalStateException(
          s"""No cardinality set for relationship $relDef. Please specify using
             |.setRelationshipCardinality("$relDef", cardinality)""".stripMargin
        )

      relationships.getOrElse(relDef, orElse)
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
    propertyIndexes: Seq[IndexDefinition] = Seq.empty
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
  }

  case class ExistenceConstraintDefinition(entityType: IndexDefinition.EntityType, propertyKey: String)

  case class PropertyTypeDefinition(
    entityType: IndexDefinition.EntityType,
    propertyKey: String,
    propertyType: SchemaValueType
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
    val point: IndexCapability = org.neo4j.kernel.impl.index.schema.PointIndexProvider.CAPABILITY
    val range: IndexCapability = org.neo4j.kernel.impl.index.schema.RangeIndexProvider.CAPABILITY
  }
}

case class StatisticsBackedLogicalPlanningConfigurationBuilder private (
  options: Options = Options(),
  cardinalities: Cardinalities = Cardinalities(),
  tokens: TokenContainer = TokenContainer(),
  indexes: Indexes = Indexes(),
  existenceConstraints: Seq[ExistenceConstraintDefinition] = Seq.empty,
  propertyTypeConstraints: Seq[PropertyTypeDefinition] = Seq.empty,
  procedures: Set[ProcedureSignature] = Set.empty,
  functions: Set[UserFunctionSignature] = Set.empty,
  settings: Map[Setting[_], AnyRef] = Map.empty
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

  def setAllNodesCardinality(c: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(cardinalities = cardinalities.copy(allNodes = Some(c)))
  }

  def setLabelCardinality(label: String, c: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    addLabel(label)
      .copy(cardinalities = cardinalities.copy(labels = cardinalities.labels + (label -> c)))
  }

  def setLabelCardinalities(labelCardinalities: Map[String, Double])
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
    withValues: Boolean = false,
    providesOrder: IndexOrderCapability = IndexOrderCapability.NONE,
    indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.RANGE,
    maybeIndexCapability: Option[IndexCapability] = None
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexDef = IndexDefinition(
      IndexDefinition.EntityType.Node(label),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = withValues,
      withOrdering = providesOrder,
      indexCapability = defaultIndexCapability(indexType, maybeIndexCapability)
    )

    addLabel(label).addIndexDefAndProperties(indexDef, properties)
  }

  def addRelationshipIndex(
    relType: String,
    properties: Seq[String],
    existsSelectivity: Double,
    uniqueSelectivity: Double,
    isUnique: Boolean = false,
    withValues: Boolean = false,
    providesOrder: IndexOrderCapability = IndexOrderCapability.NONE,
    indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.RANGE,
    maybeIndexCapability: Option[IndexCapability] = None
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexDef = IndexDefinition(
      IndexDefinition.EntityType.Relationship(relType),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = withValues,
      withOrdering = providesOrder,
      indexCapability = defaultIndexCapability(indexType, maybeIndexCapability)
    )

    addRelType(relType).addIndexDefAndProperties(indexDef, properties)
  }

  private def defaultIndexCapability(
    indexType: graphdb.schema.IndexType,
    maybeIndexCapability: Option[IndexCapability]
  ): IndexCapability = {
    maybeIndexCapability match {
      case Some(value) => value
      case None => indexType match {
          case graphdb.schema.IndexType.TEXT  => IndexCapabilities.text_2_0
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
    propertyType: SchemaValueType
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = PropertyTypeDefinition(IndexDefinition.EntityType.Node(label), property, propertyType)

    addLabel(label).addProperty(property).copy(
      propertyTypeConstraints = propertyTypeConstraints :+ constraintDef
    )
  }

  def addRelationshipPropertyTypeConstraint(
    relType: String,
    property: String,
    propertyType: SchemaValueType
  ): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = PropertyTypeDefinition(IndexDefinition.EntityType.Relationship(relType), property, propertyType)

    addRelType(relType).addProperty(property).copy(
      propertyTypeConstraints = propertyTypeConstraints :+ constraintDef
    )
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

  def enablePrintCostComparisons(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    enableDebugOption(CypherDebugOption.printCostComparisons, enable)
  }

  def enableConnectComponentsPlanner(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(connectComponentsPlanner = enable))
  }

  def enableMinimumGraphStatistics(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(useMinimumGraphStatistics = enable))
  }

  def enableLabelInference(option: LabelInferenceOption = LabelInferenceOption.enabled)
    : StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(useLabelInference = option))
  }

  def enableDeduplicateNames(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(deduplicateNames = enable))
  }

  def setExecutionModel(executionModel: ExecutionModel): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(executionModel = executionModel))
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
        case (builder, Constraint(Some(label), None, Seq(property), ConstraintType.EXISTS, Seq())) =>
          builder.addNodeExistenceConstraint(label, property)
        case (builder, Constraint(None, Some(relType), Seq(property), ConstraintType.EXISTS, Seq())) =>
          builder.addRelationshipExistenceConstraint(relType, property)
        case (builder, Constraint(_, _, _, ConstraintType.UNIQUE, Seq())) =>
          builder // Will get found by matchingUniquenessConstraintExists
        case (builder, Constraint(Some(label), None, properties, ConstraintType.UNIQUE_EXISTS, Seq())) =>
          properties.foldLeft(builder)(_.addNodeExistenceConstraint(label, _))
        case (builder, Constraint(None, Some(relType), properties, ConstraintType.UNIQUE_EXISTS, Seq())) =>
          properties.foldLeft(builder)(_.addRelationshipExistenceConstraint(relType, _))
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
          val existsSelectivity = totalSize / builder.cardinalities.labels(label)
          val uniqueSelectivity = 1.0 / estimatedUniqueSize
          val isUnique = graphCountData.matchingUniquenessConstraintExists(i)
          val maybeIndexCapability = indexProvider.name() match {
            case "text-1.0" => Some(IndexCapabilities.text_1_0)
            case "text-2.0" => Some(IndexCapabilities.text_2_0)
            case _          => None
          }
          builder.addNodeIndex(
            label,
            properties,
            existsSelectivity,
            uniqueSelectivity,
            isUnique = isUnique,
            withValues = getWithValues(indexType),
            providesOrder = getProvidesOrder(indexType),
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
            case _          => None
          }
          builder.addRelationshipIndex(
            relType,
            properties,
            existsSelectivity,
            uniqueSelectivity,
            isUnique = isUnique,
            withValues = getWithValues(indexType),
            providesOrder = getProvidesOrder(indexType),
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

  def setTxStateHasChanges(hasChanges: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(txStateHasChanges = hasChanges))
  }

  def addSemanticFeature(sf: SemanticFeature): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(semanticFeatures = options.semanticFeatures :+ sf))
  }

  def enablePlanningIntersectionScans(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseInternalSettings.planning_intersection_scans_enabled, Boolean.box(enabled))
  }

  def setDatabaseReferenceRepository(
    databaseReferenceRepository: DatabaseReferenceRepository
  ): StatisticsBackedLogicalPlanningConfigurationBuilder =
    this.copy(options = options.copy(databaseReferenceRepository = databaseReferenceRepository))

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

    val resolver = tokens.getResolver(procedures)

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
        val labelRelTypeCardinalities = cardinalities.labels.keys.flatMap { label =>
          val labelId = resolver.getLabelId(label)
          Seq(
            (labelId, patternStepCardinality(Some(LabelId(labelId)), Some(RelTypeId(typ)), None)),
            (labelId, patternStepCardinality(None, Some(RelTypeId(typ)), Some(LabelId(labelId))))
          )
        }.groupBy(_._2)
        if (labelRelTypeCardinalities.nonEmpty) {
          labelRelTypeCardinalities.maxBy(_._1.amount)._2.map(_._1).toSeq
        } else {
          Seq.empty
        }
      }
    }

    val graphStatistics =
      if (options.useMinimumGraphStatistics) {
        new MinimumGraphStatistics(internalGraphStatistics)
      } else {
        internalGraphStatistics
      }
    val planContext: PlanContext = new NotImplementedPlanContext() {
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
        cypherType: SchemaValueType
      ): Boolean = {
        propertyTypeConstraints.exists {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Node(`labelName`), `propertyKey`, `cypherType`) => true
          case _ => false
        }
      }

      override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = {
        propertyTypeConstraints.collect {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Node(`labelName`), property, cypherType) =>
            property -> Seq(cypherType)
        }.toMap
      }

      override def hasRelationshipPropertyTypeConstraint(
        relTypeName: String,
        propertyKey: String,
        cypherType: SchemaValueType
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
        : Map[String, Seq[SchemaValueType]] = {
        propertyTypeConstraints.collect {
          case PropertyTypeDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), property, cypherType) =>
            property -> Seq(cypherType)
        }.toMap
      }

      override def procedureSignature(name: QualifiedName): ProcedureSignature = {
        procedures.find(_.name == name).getOrElse(fail(s"No procedure signature for $name"))
      }

      override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
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
    }
    new StatisticsBackedLogicalPlanningConfiguration(
      resolver,
      planContext,
      options,
      settings
    )
  }
}

class StatisticsBackedLogicalPlanningConfiguration(
  resolver: LogicalPlanResolver,
  planContext: PlanContext,
  options: StatisticsBackedLogicalPlanningConfigurationBuilder.Options,
  settings: Map[Setting[_], AnyRef]
) extends LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {

  def plan(queryString: String): LogicalPlan = {
    planState(queryString).logicalPlan
  }

  def planState(queryString: String): LogicalPlanState = {
    val plannerConfiguration = CypherPlannerConfiguration.withSettings(settings)

    val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
    val metrics = SimpleMetricsFactory.newMetrics(
      planContext,
      simpleExpressionEvaluator,
      options.executionModel,
      LabelInferenceStrategy.fromConfig(planContext, options.useLabelInference)
    )

    val context = ContextHelper.create(
      planContext = planContext,
      cypherExceptionFactory = exceptionFactory,
      queryGraphSolver = queryGraphSolver(plannerConfiguration),
      metrics = metrics,
      config = plannerConfiguration,
      logicalPlanIdGen = idGen,
      debugOptions = options.debug,
      executionModel = options.executionModel,
      eagerAnalyzer = plannerConfiguration.eagerAnalyzer(),
      statefulShortestPlanningMode = plannerConfiguration.statefulShortestPlanningMode(),
      databaseReferenceRepository = options.databaseReferenceRepository
    )
    val state = InitialState(queryString, None, IDPPlannerName, new AnonymousVariableNameGenerator)
    val parsingConfig = {
      val cypherCompilerConfig = LogicalPlanningTestSupport2.defaultCypherCompilerConfig
      val cfg = LogicalPlanningTestSupport2.defaultParsingConfig(cypherCompilerConfig)
      cfg.copy(semanticFeatures = cfg.semanticFeatures ++ options.semanticFeatures)
    }

    LogicalPlanningTestSupport2.pipeLine(parsingConfig, deduplicateNames = options.deduplicateNames).transform(
      state,
      context
    )
  }

  def planBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = true, resolver)
  def subPlanBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = false, resolver)

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
