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
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Indexes
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Options
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelDef
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.OrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.ValueCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.graphdb
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexType.BTREE
import org.neo4j.internal.schema.IndexType.FULLTEXT
import org.neo4j.internal.schema.IndexType.LOOKUP

trait StatisticsBackedLogicalPlanningSupport {

  /**
   * @return an immutable builder to construct [[StatisticsBackedLogicalPlanningConfiguration]]s.
   */
  protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder = StatisticsBackedLogicalPlanningConfigurationBuilder.newBuilder()
}

object StatisticsBackedLogicalPlanningConfigurationBuilder {

  def newBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder = StatisticsBackedLogicalPlanningConfigurationBuilder()

  case class Options(
                      debug: CypherDebugOptions = CypherDebugOptions(Set.empty),
                      connectComponentsPlanner: Boolean = true,
                      executionModel: ExecutionModel = ExecutionModel.default,
                      useMinimumGraphStatistics: Boolean = false,
                      txStateHasChanges: Boolean = false,
                      semanticFeatures: Seq[SemanticFeature] = Seq.empty,
                    )
  case class Cardinalities(
                            allNodes: Option[Double] = None,
                            labels: Map[String, Double] = Map[String, Double](),
                            relationships: Map[RelDef, Double] = Map[RelDef, Double](),
                            defaultRelationshipCardinalityTo0: Boolean = false
                          ) {
    def getRelCount(relDef: RelDef): Double = {
      def orElse: Double =
        if (defaultRelationshipCardinalityTo0) 0.0
        else throw new IllegalStateException(
          s"""No cardinality set for relationship $relDef. Please specify using
             |.setRelationshipCardinality("$relDef", cardinality)""".stripMargin)

        relationships.getOrElse(relDef, orElse)
    }
  }
  object RelDef {

    private implicit class RegexHelper(val sc: StringContext) {
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
        throw new IllegalArgumentException(s"Invalid relationship pattern $pat. Expected something like ()-[]-(), (:A)-[:R]->(), (:A)<-[]-(), etc.")
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

  case class IndexDefinition(entityType: IndexDefinition.EntityType,
                             indexType: graphdb.schema.IndexType,
                             propertyKeys: Seq[String],
                             uniqueValueSelectivity: Double,
                             propExistsSelectivity: Double,
                             isUnique: Boolean = false,
                             withValues: Boolean = false,
                             withOrdering: IndexOrderCapability = IndexOrderCapability.NONE)

  object IndexDefinition {
    sealed trait EntityType
    object EntityType {
      final case class Node(label: String) extends EntityType
      final case class Relationship(relType: String) extends EntityType
    }
  }
  case class Indexes(
                      nodeLookupIndex: Boolean = true,
                      relationshipLookupIndex: Boolean = true,
                      propertyIndexes: Seq[IndexDefinition] = Seq.empty,
                    ) {
    def addPropertyIndex(indexDefinition: IndexDefinition): Indexes = this.copy(propertyIndexes = propertyIndexes :+ indexDefinition)
    def addNodeLookupIndex(): Indexes = this.copy(nodeLookupIndex = true)
    def removeNodeLookupIndex(): Indexes = this.copy(nodeLookupIndex = false)
    def addRelationshipLookupIndex(): Indexes = this.copy(relationshipLookupIndex = true)
    def removeRelationshipLookupIndex(): Indexes = this.copy(relationshipLookupIndex = false)
  }

  case class ExistenceConstraintDefinition(entityType: IndexDefinition.EntityType,
                                           propertyKey: String)
}

case class StatisticsBackedLogicalPlanningConfigurationBuilder private(
                                                                        options: Options = Options(),
                                                                        cardinalities: Cardinalities = Cardinalities(),
                                                                        tokens: TokenContainer = TokenContainer(),
                                                                        indexes: Indexes = Indexes(),
                                                                        constraints: Seq[ExistenceConstraintDefinition] = Seq.empty,
                                                                        procedures: Set[ProcedureSignature] = Set.empty,
                                                                        settings: Map[Setting[_], AnyRef] = Map.empty,
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

  def setLabelCardinalities(labelCardinalities: Map[String, Double]): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    labelCardinalities.foldLeft(this) {
      case (builder, (label, c)) => builder.setLabelCardinality(label, c)
    }
  }

  def setAllRelationshipsCardinality(cardinality: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    setRelationshipCardinality(None, None, None, cardinality)
  }

  def setRelationshipCardinality(relDef: String, cardinality: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    RelDef.fromString(relDef).foldLeft(this) {
      case (builder, rd) => builder.setRelationshipCardinality(rd.fromLabel, rd.relType, rd.toLabel, cardinality)
    }
  }

  def setRelationshipCardinality(from: Option[String] = None, rel: Option[String] = None, to: Option[String] = None, cardinality: Double): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val withFromLabel = from.foldLeft(this) {
      case (builder, label) => builder.addLabel(label)
    }

    val withRelType = rel.foldLeft(withFromLabel) {
      case (builder, rel) => builder.addRelType(rel)
    }

    val withToLabel = rel.foldLeft(withRelType) {
      case (builder, label) => builder.addLabel(label)
    }

    withToLabel.copy(cardinalities = cardinalities.copy(relationships = cardinalities.relationships + (RelDef(from, rel, to) -> cardinality)))
  }

  def defaultRelationshipCardinalityTo0(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(cardinalities = cardinalities.copy(defaultRelationshipCardinalityTo0 = enable))
  }

  def addNodeIndex(label: String,
                   properties: Seq[String],
                   existsSelectivity: Double,
                   uniqueSelectivity: Double,
                   isUnique: Boolean = false,
                   withValues: Boolean = false,
                   providesOrder: IndexOrderCapability = IndexOrderCapability.NONE,
                   indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.BTREE): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexDef = IndexDefinition(IndexDefinition.EntityType.Node(label),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = withValues,
      withOrdering = providesOrder)

    addLabel(label).addIndexDefAndProperties(indexDef, properties)
  }

  def addRelationshipIndex(relType: String,
                           properties: Seq[String],
                           existsSelectivity: Double,
                           uniqueSelectivity: Double,
                           isUnique: Boolean = false,
                           withValues: Boolean = false,
                           providesOrder: IndexOrderCapability = IndexOrderCapability.NONE,
                           indexType: graphdb.schema.IndexType = graphdb.schema.IndexType.BTREE): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val indexDef = IndexDefinition(IndexDefinition.EntityType.Relationship(relType),
      indexType = indexType,
      propertyKeys = properties,
      propExistsSelectivity = existsSelectivity,
      uniqueValueSelectivity = uniqueSelectivity,
      isUnique = isUnique,
      withValues = withValues,
      withOrdering = providesOrder)

    addRelType(relType).addIndexDefAndProperties(indexDef, properties)
  }

  def addNodeLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.addNodeLookupIndex())
  }

  def removeNodeLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.removeNodeLookupIndex())
  }

  def addRelationshipLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.addRelationshipLookupIndex())
  }

  def removeRelationshipLookupIndex(): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(indexes = indexes.removeRelationshipLookupIndex())
  }

  private def addIndexDefAndProperties(indexDef: IndexDefinition, properties: Seq[String]): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val withProperties = properties.foldLeft(this) {
      case (builder, prop) => builder.addProperty(prop)
    }
    withProperties
      .copy(indexes = indexes.addPropertyIndex(indexDef))
  }

  def addNodeExistenceConstraint(label: String,
                                 property: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(label), property)

    addLabel(label).addProperty(property).copy(
      constraints = constraints :+ constraintDef
    )
  }

  def addRelationshipExistenceConstraint(relType: String,
                                         property: String): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    val constraintDef = ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(relType), property)

    addRelType(relType).addProperty(property).copy(
      constraints = constraints :+ constraintDef
    )
  }

  def addProcedure(signature: ProcedureSignature): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(procedures = this.procedures + signature)
  }

  private def fail(message: String): Nothing =
    throw new IllegalStateException(message)

  def enableDebugOption(option: CypherDebugOption, enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(
      debug = if (enable)
        options.debug.copy(options.debug.enabledOptions + option)
      else
        options.debug.copy(options.debug.enabledOptions - option))
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

  def setExecutionModel(executionModel: ExecutionModel): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(executionModel = executionModel))
  }

  /**
   * Process graph count data and return a builder with updated constraints, indexes and counts.
   */
  def processGraphCounts(graphCountData: GraphCountData): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    def matchingUniquenessConstraintExists(index: Index): Boolean = {
      index match {
        case Index(Some(Seq(label)), None, BTREE, properties, _, _, _) => graphCountData.constraints.exists {
          case Constraint(Some(`label`), None, `properties`, ConstraintType.UNIQUE) => true
          case Constraint(Some(`label`), None, `properties`, ConstraintType.UNIQUE_EXISTS) => true
          case _ => false
        }
        case Index(None, Some(Seq(relType)), BTREE, properties, _, _, _) => graphCountData.constraints.exists {
          case Constraint(None, Some(`relType`), `properties`, ConstraintType.UNIQUE) => true
          case Constraint(None, Some(`relType`), `properties`, ConstraintType.UNIQUE_EXISTS) => true
          case _ => false
        }
        case _ => false
      }
    }

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
        case (builder, NodeCount(count, None)) => builder.setAllNodesCardinality(count)
        case (builder, NodeCount(count, Some(label))) => builder.setLabelCardinality(label, count)
      }

    val withRelationships = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.relationships.foldLeft(builder) {
        case (builder, RelationshipCount(count, relationshipType, startLabel, endLabel)) =>
          builder.setRelationshipCardinality(startLabel, relationshipType, endLabel, count)
      }

    val withConstraints = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.constraints.foldLeft(builder) {
        case (builder, Constraint(Some(label), None, Seq(property), ConstraintType.EXISTS)) => builder.addNodeExistenceConstraint(label, property)
        case (builder, Constraint(None, Some(relType), Seq(property), ConstraintType.EXISTS)) => builder.addRelationshipExistenceConstraint(relType, property)
        case (builder, Constraint(_, _, _, ConstraintType.UNIQUE)) => builder // Will get found by matchingUniquenessConstraintExists
        case (builder, Constraint(_, _, _, ConstraintType.UNIQUE_EXISTS)) => builder // Will get found by matchingUniquenessConstraintExists
        case (_, constraint) => throw new IllegalArgumentException(s"Unsupported constraint: $constraint")
      }

    val withIndexes = (builder: StatisticsBackedLogicalPlanningConfigurationBuilder) =>
      graphCountData.indexes.foldLeft(builder) {
        case (_, index@Index(_, _, FULLTEXT, _, _, _, _)) => throw new IllegalArgumentException(s"Unsupported index of type FULLTEXT: $index")
        case (builder, Index(Some(Seq()), None, LOOKUP, Seq(), _, _, _)) =>
          builder.addNodeLookupIndex()
        case (builder, Index(None, Some(Seq()), LOOKUP, Seq(), _, _, _)) =>
          builder.addRelationshipLookupIndex()
        case (builder, i@Index(Some(Seq(label)), None, indexType, properties, totalSize, estimatedUniqueSize, _)) =>
          val existsSelectivity = totalSize / builder.cardinalities.labels(label)
          val uniqueSelectivity = 1.0 / estimatedUniqueSize
          val isUnique = matchingUniquenessConstraintExists(i)
          builder.addNodeIndex(label, properties, existsSelectivity, uniqueSelectivity, isUnique = isUnique, withValues = true, providesOrder = IndexOrderCapability.BOTH, indexType.toPublicApi)
        case (builder, i@Index(None, Some(Seq(relType)), indexType, properties, totalSize, estimatedUniqueSize, _)) =>
          val existsSelectivity = totalSize / builder.cardinalities.getRelCount(RelDef(None, Some(relType), None))
          val uniqueSelectivity = 1.0 / estimatedUniqueSize
          val isUnique = matchingUniquenessConstraintExists(i)
          builder.addRelationshipIndex(relType, properties, existsSelectivity, uniqueSelectivity, isUnique = isUnique, withValues = true, providesOrder = IndexOrderCapability.BOTH, indexType.toPublicApi)
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

  def enablePlanningTextIndexes(enabled: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    withSetting(GraphDatabaseInternalSettings.planning_text_indexes_enabled, Boolean.box(enabled))
  }

  def build(): StatisticsBackedLogicalPlanningConfiguration = {
    require(cardinalities.allNodes.isDefined, "Please specify allNodesCardinality using `setAllNodesCardinality`.")
    cardinalities.allNodes.foreach(anc =>
      cardinalities.labels.values.foreach(lc =>
        require(anc >= lc, s"Label cardinality ($lc) was greater than all nodes cardinality ($anc)")
      ))

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
               .map(label => cardinalities.labels.getOrElse(label, fail(
                 s"""No cardinality set for label $label. Please specify using
                    |.setLabelCardinality("$label", cardinality)""".stripMargin)))
               .map(Cardinality.apply)
               .getOrElse(Cardinality.EMPTY)
      }

      override def patternStepCardinality(fromLabelId: Option[LabelId], relTypeId: Option[RelTypeId], toLabelId: Option[LabelId]): Cardinality = {
        val relDef = RelDef(
          fromLabel = fromLabelId.map(_.id).map(resolver.getLabelName),
          relType = relTypeId.map(_.id).map(resolver.getRelTypeName),
          toLabel = toLabelId.map(_.id).map(resolver.getLabelName),
        )
        cardinalities.getRelCount(relDef)
      }

      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.propertyIndexes.find { indexDef =>
          indexDef.entityType == resolveEntityType(index) &&
            indexDef.propertyKeys == index.properties.map(_.id).map(resolver.getPropertyKeyName)
        }.flatMap(indexDef => Selectivity.of(indexDef.uniqueValueSelectivity))
      }

      override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.propertyIndexes.find { indexDef =>
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
    }

    val graphStatistics =
      if (options.useMinimumGraphStatistics) new MinimumGraphStatistics(internalGraphStatistics)
      else internalGraphStatistics

    val planContext: PlanContext = new NotImplementedPlanContext() {
      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(graphStatistics, new MutableGraphStatisticsSnapshot())

      override def btreeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.BTREE)
      }

      override def btreeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.BTREE)
      }

      override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.TEXT)
      }

      override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityAndIndexType(entityType, graphdb.schema.IndexType.TEXT)
      }

      private def indexesGetForEntityAndIndexType(entityType: IndexDefinition.EntityType, indexType: graphdb.schema.IndexType): Iterator[IndexDescriptor] = {
        indexes.propertyIndexes.collect {
          case indexDef@IndexDefinition(`entityType`, `indexType`, _, _, _, _, _, _) =>
            newIndexDescriptor(indexDef)
        }.flatten
      }.iterator

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = {
        indexes.propertyIndexes.toIterator.flatMap(newIndexDescriptor)
      }

      override def canLookupNodesByLabel: Boolean = indexes.nodeLookupIndex

      override def canLookupRelationshipsByType: Boolean = indexes.relationshipLookupIndex

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        constraints.collect {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(`labelName`), property) => property
        }.toSet
      }

      override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
        constraints.exists {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Node(`labelName`), `propertyKey`) => true
          case _ => false
        }
      }

      override def getRelationshipPropertiesWithExistenceConstraint(relTypeName: String): Set[String] = {
        constraints.collect {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), property) => property
        }.toSet
      }

      override def getPropertiesWithExistenceConstraint: Set[String] = {
        constraints.collect {
          case ExistenceConstraintDefinition(_, property) => property
        }.toSet
      }

      override def hasRelationshipPropertyExistenceConstraint(relTypeName: String, propertyKey: String): Boolean = {
        constraints.exists {
          case ExistenceConstraintDefinition(IndexDefinition.EntityType.Relationship(`relTypeName`), `propertyKey`) => true
          case _ => false
        }
      }

      override def procedureSignature(name: QualifiedName): ProcedureSignature = {
        procedures.find(_.name == name).getOrElse(fail(s"No procedure signature for $name"))
      }

      override def btreeIndexExistsForLabel(labelId: Int): Boolean = {
        btreeIndexesGetForLabel(labelId).nonEmpty
      }

      override def btreeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        btreeIndexGetForLabelAndProperties(labelName, propertyKeys).nonEmpty
      }

      override def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        textIndexGetForLabelAndProperties(labelName, propertyKeys).nonEmpty
      }

      override def btreeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = {
        btreeIndexGetForRelTypeAndProperties(relTypeName, propertyKeys).nonEmpty
      }

      override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = {
        textIndexGetForRelTypeAndProperties(relTypeName, propertyKeys).nonEmpty
      }

      override def btreeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.BTREE)
      }

      override def textIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.TEXT)
      }

      override def btreeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(relTypeName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.BTREE)
      }

      override def textIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(relTypeName)
        indexGetForEntityTypePropertiesAndIndexType(entityType, propertyKeys, graphdb.schema.IndexType.TEXT)
      }

      private def indexGetForEntityTypePropertiesAndIndexType(entityType: IndexDefinition.EntityType,
                                                              propertyKeys: Seq[String],
                                                              indexType: graphdb.schema.IndexType): Option[IndexDescriptor] = {
        indexes.propertyIndexes.collect {
          case indexDef@IndexDefinition(`entityType`, `indexType`, `propertyKeys`, _, _, _, _, _) => newIndexDescriptor(indexDef)
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

      private def newIndexDescriptor(indexDef: IndexDefinition): Option[IndexDescriptor] = {
        // Our fake index either can always or never return property values
        val canGetValue = if (indexDef.withValues) CanGetValue else DoNotGetValue
        val valueCapability: ValueCapability = _ => indexDef.propertyKeys.map(_ => canGetValue)
        val orderCapability: OrderCapability = _ => indexDef.withOrdering

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
            valueCapability = valueCapability,
            orderCapability = orderCapability,
            isUnique = indexDef.isUnique
          )
        }
      }
    }
    new StatisticsBackedLogicalPlanningConfiguration(
      resolver,
      planContext,
      options,
      settings,
    )
  }
}

class StatisticsBackedLogicalPlanningConfiguration(
  resolver: LogicalPlanResolver,
  planContext: PlanContext,
  options: StatisticsBackedLogicalPlanningConfigurationBuilder.Options,
  settings: Map[Setting[_], AnyRef],
) extends LogicalPlanConstructionTestSupport
  with AstConstructionTestSupport {

  def plan(queryString: String): LogicalPlan = {
    planState(queryString).logicalPlan
  }

  def planState(queryString: String): LogicalPlanState = {
    val plannerConfiguration = CypherPlannerConfiguration.withSettings(settings)

    val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
    val metrics = SimpleMetricsFactory.newMetrics(planContext, simpleExpressionEvaluator, options.executionModel, plannerConfiguration.planningTextIndexesEnabled)

    val iDPSolverConfig = new ConfigurableIDPSolverConfig(plannerConfiguration.idpMaxTableSize, plannerConfiguration.idpIterationDuration)

    val context = ContextHelper.create(
      planContext = planContext,
      cypherExceptionFactory = exceptionFactory,
      queryGraphSolver =
        if (options.connectComponentsPlanner) {
          LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents.queryGraphSolver(iDPSolverConfig)
        } else {
          LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents.queryGraphSolver(iDPSolverConfig)
        },
      metrics = metrics,
      config = plannerConfiguration,
      logicalPlanIdGen = idGen,
      debugOptions = options.debug,
      executionModel = options.executionModel
    )
    val state = InitialState(queryString, IDPPlannerName, new AnonymousVariableNameGenerator)
    val parsingConfig = {
      val cypherCompilerConfig = LogicalPlanningTestSupport2.defaultCypherCompilerConfig
      val cfg = LogicalPlanningTestSupport2.defaultParsingConfig(cypherCompilerConfig)
      cfg.copy(semanticFeatures = cfg.semanticFeatures ++ options.semanticFeatures)
    }

    LogicalPlanningTestSupport2.pipeLine(parsingConfig).transform(state, context)
  }

  def planBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = true, resolver)
  def subPlanBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = false, resolver)
}
