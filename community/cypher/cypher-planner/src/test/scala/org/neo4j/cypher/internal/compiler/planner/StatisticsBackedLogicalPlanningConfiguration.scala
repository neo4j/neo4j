/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.cypherCompilerConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
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
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable

trait StatisticsBackedLogicalPlanningSupport {
  protected def plannerBuilder() = new StatisticsBackedLogicalPlanningConfigurationBuilder()
}

object StatisticsBackedLogicalPlanningConfigurationBuilder {
  case class Options(
    debug: Set[String] = Set(),
  )
}

class StatisticsBackedLogicalPlanningConfigurationBuilder() {

  private val tokens = new LogicalPlanResolver()
  private object indexes extends FakeIndexAndConstraintManagement {
    def getIndexes: Map[IndexDef, IndexType] = indexes
    def getConstraints: Set[(String, Set[String])] = constraints
    def getProcedureSignatures: Set[ProcedureSignature] = procedureSignatures
  }
  private object cardinalities {
    var allNodes: Option[Double] = None
    val labels: mutable.Map[String, Double] = mutable.Map[String, Double]()
    val relationships: mutable.Map[RelDef, Double] = mutable.Map[RelDef, Double]()
  }
  private object selectivities {
    val uniqueValue: mutable.Map[IndexDef, Double] = mutable.Map[IndexDef, Double]()
    val propExists: mutable.Map[IndexDef, Double] = mutable.Map[IndexDef, Double]()
  }
  private var options = StatisticsBackedLogicalPlanningConfigurationBuilder.Options()

  def addLabel(label: String): this.type = {
    tokens.getLabelId(label)
    this
  }

  def addRelType(relType: String): this.type = {
    tokens.getRelTypeId(relType)
    this
  }

  def addProperty(prop: String): this.type = {
    tokens.getPropertyKeyId(prop)
    this
  }

  def setAllNodesCardinality(c: Double): this.type = {
    cardinalities.allNodes = Some(c)
    this
  }

  def setLabelCardinality(label: String, c: Double): this.type = {
    addLabel(label)
    cardinalities.labels(label) = c
    this
  }

  def setLabelCardinalities(labelCardinalities: Map[String, Double]): this.type = {
    for {(label, c) <- labelCardinalities} setLabelCardinality(label, c)
    this
  }

  def setAllRelationshipsCardinality(cardinality: Double): this.type = {
    setRelationshipCardinality(None, None, None, cardinality)
  }

  def setRelationshipCardinality(relDef: String, cardinality: Double): this.type = {
    RelDef.fromString(relDef)
          .foreach(rd => setRelationshipCardinality(rd.fromLabel, rd.relType, rd.toLabel, cardinality))
    this
  }

  def setRelationshipCardinality(from: Option[String] = None, rel: Option[String] = None, to: Option[String] = None, cardinality: Double): this.type = {
    from.foreach(addLabel)
    rel.foreach(addRelType)
    to.foreach(addLabel)
    cardinalities.relationships(RelDef(from, rel, to)) = cardinality
    this
  }

  def addIndex(
    label: String,
    properties: Seq[String],
    existsSelectivity: Double,
    uniqueSelectivity: Double,
    isUnique: Boolean = false,
    withValues: Boolean = false,
    providesOrder: IndexOrderCapability = IndexOrderCapability.NONE): this.type = {
    addLabel(label)
    properties.foreach(addProperty)
    val indexDef = indexes.indexOn(label, properties, isUnique, withValues, providesOrder)
    selectivities.propExists(indexDef) = existsSelectivity
    selectivities.uniqueValue(indexDef) = uniqueSelectivity
    this
  }

  def addProcedure(signature: ProcedureSignature): this.type = {
    indexes.procedure(signature)
    this
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
    override def toString(): String = {
      val f = fromLabel.fold("")(l => ":" + l)
      val r = relType.fold("")(l => ":" + l)
      val t = toLabel.fold("")(l => ":" + l)
      s"($f)-[$r]->($t)"
    }
  }

  private def fail(message: String): Nothing =
    throw new IllegalStateException(message)


  def enableDebugOption(option: String, enable: Boolean = true): this.type = {
    options = options.copy(debug = if (enable) options.debug + option else options.debug - option)
    this
  }

  def enablePrintCostComparisons(enable: Boolean = true): this.type = enableDebugOption("printCostComparisons", enable)

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

    val graphStatistics = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = cardinalities.allNodes.get

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.map(_.id)
               .map(tokens.getLabelName)
               .map(label => cardinalities.labels.getOrElse(label, fail(s"No cardinality set for label $label")))
               .map(Cardinality.apply)
               .getOrElse(Cardinality.EMPTY)
      }

      override def patternStepCardinality(fromLabelId: Option[LabelId], relTypeId: Option[RelTypeId], toLabelId: Option[LabelId]): Cardinality = {
        val relDef = RelDef(
          fromLabel = fromLabelId.map(_.id).map(tokens.getLabelName),
          relType = relTypeId.map(_.id).map(tokens.getRelTypeName),
          toLabel = toLabelId.map(_.id).map(tokens.getLabelName),
        )
        cardinalities.relationships.getOrElse(relDef, fail(s"No cardinality set for relationship $relDef"))
      }

      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        selectivities.uniqueValue
          .get(IndexDef(label = tokens.getLabelName(index.label), propertyKeys = index.properties.map(_.id).map(tokens.getPropertyKeyName)))
          .flatMap(Selectivity.of)
      }

      override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] = {
        selectivities.propExists
          .get(IndexDef(label = tokens.getLabelName(index.label), propertyKeys = index.properties.map(_.id).map(tokens.getPropertyKeyName)))
          .flatMap(Selectivity.of)
      }
    }

    val planContext: PlanContext = new NotImplementedPlanContext() {
      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(graphStatistics, new MutableGraphStatisticsSnapshot())

      override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val labelName = tokens.getLabelName(labelId)
        indexes.getIndexes.collect {
          case (indexDef, indexType) if labelName == indexDef.label =>
            newIndexDescriptor(indexDef, indexType)
        }
      }.iterator

      override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
        val labelName = tokens.getLabelName(labelId)
        indexes.getIndexes.collect {
          case (indexDef, indexType) if labelName == indexDef.label && indexType.isUnique =>
            newIndexDescriptor(indexDef, indexType)
        }
      }.iterator

      override def getPropertiesWithExistenceConstraint(labelName: String): Set[String] = {
        indexes.getConstraints.collect { case (`labelName`, properties) => properties }.flatten
      }

      override def procedureSignature(name: QualifiedName): ProcedureSignature = {
        indexes.getProcedureSignatures.find(_.name == name).getOrElse(fail(s"No procedure signature for $name"))
      }

      override def indexExistsForLabel(labelId: Int): Boolean = {
        val labelName = tokens.getLabelName(labelId)
        indexes.getIndexes.keys.exists(_.label == labelName)
      }

      override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
        indexes.getIndexes.contains(IndexDef(labelName, propertyKey))

      override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
        val indexDef = IndexDef(labelName, propertyKeys)
        indexes.getIndexes.get(indexDef).map(indexType => newIndexDescriptor(indexDef, indexType))
      }

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
        tokens.getOptPropertyKeyId(propertyKeyName)
      }

      override def getOptLabelId(labelName: String): Option[Int] =
        tokens.getOptLabelId(labelName)

      override def getOptRelTypeId(relType: String): Option[Int] =
        tokens.getOptRelTypeId(relType)

      private def newIndexDescriptor(indexDef: IndexDef, indexType: IndexType): IndexDescriptor = {
        // Our fake index either can always or never return property values
        val canGetValue = if (indexType.withValues) CanGetValue else DoNotGetValue
        val valueCapability: ValueCapability = _ => indexDef.propertyKeys.map(_ => canGetValue)
        val orderCapability: OrderCapability = _ => indexType.withOrdering

        val props = indexDef.propertyKeys.map(p => PropertyKeyId(tokens.getPropertyKeyId(p)))
        val label = LabelId(tokens.getLabelId(indexDef.label))

        IndexDescriptor(
          label,
          props,
          valueCapability = valueCapability,
          orderCapability = orderCapability,
          isUnique = indexType.isUnique
        )
      }
    }
    new StatisticsBackedLogicalPlanningConfiguration(
      tokens,
      planContext,
      options = options,
    )
  }
}

class StatisticsBackedLogicalPlanningConfiguration(
  resolver: LogicalPlanResolver,
  planContext: PlanContext,
  options: StatisticsBackedLogicalPlanningConfigurationBuilder.Options,
) extends LogicalPlanConstructionTestSupport
  with AstConstructionTestSupport
  with MockitoSugar {

  def plan(queryString: String): LogicalPlan = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryString, Some(pos))
    val metrics = SimpleMetricsFactory.newMetrics(planContext.statistics, mock[ExpressionEvaluator], cypherCompilerConfig)

    val context = ContextHelper.create(
      planContext = planContext,
      cypherExceptionFactory = exceptionFactory,
      queryGraphSolver = LogicalPlanningTestSupport2.createQueryGraphSolver(),
      metrics = metrics,
      config = cypherCompilerConfig,
      logicalPlanIdGen = idGen,
      debugOptions = options.debug,
    )
    val state = InitialState(queryString, None, IDPPlannerName)
    LogicalPlanningTestSupport2.pipeLine().transform(state, context).logicalPlan
  }

  def planBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = true, resolver)
  def subPlanBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder(wholePlan = false, resolver)
}
