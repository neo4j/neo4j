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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.helpers.TokenContainer
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.cypherCompilerConfig
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Cardinalities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition.EntityType
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.Options
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelDef
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
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
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

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
                      relationshipTypeScanStoreEnabled: Boolean = false,
                    )
  case class Cardinalities(
                            allNodes: Option[Double] = None,
                            labels: Map[String, Double] = Map[String, Double](),
                            relationships: Map[RelDef, Double] = Map[RelDef, Double]()
                          )
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
}

case class StatisticsBackedLogicalPlanningConfigurationBuilder private(
                                                                        options: Options = Options(),
                                                                        cardinalities: Cardinalities = Cardinalities(),
                                                                        tokens: TokenContainer = TokenContainer(),
                                                                        indexes: Seq[IndexDefinition] = Seq.empty,
                                                                        procedures: Set[ProcedureSignature] = Set.empty
                                                                      ) {

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

  def addIndex(
    label: String,
    properties: Seq[String],
    existsSelectivity: Double,
    uniqueSelectivity: Double,
    isUnique: Boolean = false,
    withValues: Boolean = false,
    providesOrder: IndexOrderCapability = IndexOrderCapability.NONE): StatisticsBackedLogicalPlanningConfigurationBuilder = {

    val withLabel = addLabel(label)
    val withProperties = properties.foldLeft(withLabel) {
      case (builder, prop) => builder.addProperty(prop)
    }
    val indexDef = IndexDefinition(IndexDefinition.EntityType.Node(label), properties, uniqueSelectivity, existsSelectivity, isUnique, withValues, providesOrder)
    withProperties
      .copy(indexes = indexes :+ indexDef)
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

  def setExecutionModel(executionModel: ExecutionModel): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(executionModel = executionModel))
  }

  def enableRelationshipTypeScanStore(enable: Boolean = true): StatisticsBackedLogicalPlanningConfigurationBuilder = {
    this.copy(options = options.copy(relationshipTypeScanStoreEnabled = enable))
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

    val graphStatistics = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = cardinalities.allNodes.get

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.map(_.id)
               .map(resolver.getLabelName)
               .map(label => cardinalities.labels.getOrElse(label, fail(s"No cardinality set for label $label")))
               .map(Cardinality.apply)
               .getOrElse(Cardinality.EMPTY)
      }

      override def patternStepCardinality(fromLabelId: Option[LabelId], relTypeId: Option[RelTypeId], toLabelId: Option[LabelId]): Cardinality = {
        val relDef = RelDef(
          fromLabel = fromLabelId.map(_.id).map(resolver.getLabelName),
          relType = relTypeId.map(_.id).map(resolver.getRelTypeName),
          toLabel = toLabelId.map(_.id).map(resolver.getLabelName),
        )
        cardinalities.relationships.getOrElse(relDef, fail(s"No cardinality set for relationship $relDef"))
      }

      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.find { indexDef =>
          indexDef.entityType == resolveEntityType(index) &&
            indexDef.propertyKeys == index.properties.map(_.id).map(resolver.getPropertyKeyName)
        }.flatMap(indexDef => Selectivity.of(indexDef.uniqueValueSelectivity))
      }

      override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        indexes.find { indexDef =>
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

    val planContext: PlanContext = new NotImplementedPlanContext() {
      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(graphStatistics, new MutableGraphStatisticsSnapshot())

      override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexesGetForEntityType(entityType)
      }

      override def indexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Relationship(resolver.getRelTypeName(relTypeId))
        indexesGetForEntityType(entityType)
      }

      private def indexesGetForEntityType(entityType: IndexDefinition.EntityType): Iterator[IndexDescriptor] = {
        indexes.collect {
          case indexDef if entityType == indexDef.entityType =>
            newIndexDescriptor(indexDef)
        }
      }.iterator

      override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val labelName = IndexDefinition.EntityType.Node(resolver.getLabelName(labelId))
        indexes.collect {
          case indexDef if labelName == indexDef.entityType && indexDef.isUnique =>
            newIndexDescriptor(indexDef)
        }
      }.iterator

      override def getPropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        // Existence and node-key constraints are not yet supported by this class.
        Set.empty
      }

      override def procedureSignature(name: QualifiedName): ProcedureSignature = {
        procedures.find(_.name == name).getOrElse(fail(s"No procedure signature for $name"))
      }

      override def indexExistsForLabel(labelId: Int): Boolean = {
        indexesGetForRelType(labelId).nonEmpty
      }

      override def indexExistsForRelType(relTypeId: Int): Boolean = {
        indexesGetForRelType(relTypeId).nonEmpty
      }

      override def indexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexes.exists {
          case indexDef if indexDef.entityType == entityType && indexDef.propertyKeys == propertyKeys => true
          case _ => false
        }
      }

      override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val entityType = IndexDefinition.EntityType.Node(labelName)
        indexes.collectFirst {
          case indexDef if indexDef.entityType == entityType && indexDef.propertyKeys == propertyKeys => newIndexDescriptor(indexDef)
        }
      }

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
        resolver.getOptPropertyKeyId(propertyKeyName)
      }

      override def getOptLabelId(labelName: String): Option[Int] =
        resolver.getOptLabelId(labelName)

      override def getOptRelTypeId(relType: String): Option[Int] =
        resolver.getOptRelTypeId(relType)

      override def relationshipTypeScanStoreEnabled: Boolean =
        options.relationshipTypeScanStoreEnabled

      private def newIndexDescriptor(indexDef: IndexDefinition): IndexDescriptor = {
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

        IndexDescriptor(
          entityType,
          props,
          valueCapability = valueCapability,
          orderCapability = orderCapability,
          isUnique = indexDef.isUnique
        )
      }
    }
    new StatisticsBackedLogicalPlanningConfiguration(
      resolver,
      planContext,
      options,
    )
  }
}

class StatisticsBackedLogicalPlanningConfiguration(
  resolver: LogicalPlanResolver,
  planContext: PlanContext,
  options: StatisticsBackedLogicalPlanningConfigurationBuilder.Options,
) extends LogicalPlanConstructionTestSupport
  with AstConstructionTestSupport {

  def plan(queryString: String): LogicalPlan = {
    planState(queryString).logicalPlan
  }

  def planState(queryString: String): LogicalPlanState = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
    val metrics = SimpleMetricsFactory.newMetrics(planContext.statistics, simpleExpressionEvaluator, cypherCompilerConfig, options.executionModel)

    val context = ContextHelper.create(
      planContext = planContext,
      cypherExceptionFactory = exceptionFactory,
      queryGraphSolver =
        if (options.connectComponentsPlanner) LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents.queryGraphSolver()
        else LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents.queryGraphSolver(),
      metrics = metrics,
      config = cypherCompilerConfig,
      logicalPlanIdGen = idGen,
      debugOptions = options.debug,
      executionModel = options.executionModel
    )
    val state = InitialState(queryString, None, IDPPlannerName)
    LogicalPlanningTestSupport2.pipeLine().transform(state, context)
  }

  def planBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = true, resolver)
  def subPlanBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = false, resolver)
}
