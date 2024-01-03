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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.LabelId

case class IndexModifier(indexAttributes: IndexAttributes) {

  def providesValues(): IndexModifier = {
    indexAttributes.withValues = true
    this
  }

  def providesOrder(order: IndexOrderCapability): IndexModifier = {
    indexAttributes.withOrdering = order
    this
  }
}

trait FakeIndexAndConstraintManagement {
  var indexes: Map[IndexDef, IndexAttributes] = Map.empty

  var nodeConstraints: Set[(String, Set[String])] = Set.empty

  var relationshipConstraints: Set[(String, Set[String])] = Set.empty

  var procedureSignatures: Set[ProcedureSignature] = Set.empty

  def indexOn(label: String, properties: String*): IndexModifier = {
    val indexDef =
      indexOn(label, properties, isUnique = false, withValues = false, IndexOrderCapability.NONE, IndexType.Range)
    IndexModifier(indexes(indexDef))
  }

  def pointIndexOn(label: String, properties: String*): IndexModifier = {
    val indexDef =
      indexOn(label, properties, isUnique = false, withValues = false, IndexOrderCapability.NONE, IndexType.Point)
    IndexModifier(indexes(indexDef))
  }

  def textIndexOn(label: String, properties: String*): IndexModifier = {
    val indexDef =
      indexOn(label, properties, isUnique = false, withValues = false, IndexOrderCapability.NONE, IndexType.Text)
    IndexModifier(indexes(indexDef))
  }

  def relationshipIndexOn(relationshipType: String, properties: String*): IndexModifier = {
    val indexDef = relationshipIndexOn(
      relationshipType,
      properties,
      isUnique = false,
      withValues = false,
      IndexOrderCapability.NONE,
      IndexType.Range
    )
    IndexModifier(indexes(indexDef))
  }

  def uniqueRelationshipIndexOn(relationshipType: String, properties: String*): IndexModifier = {
    val indexDef = relationshipIndexOn(
      relationshipType,
      properties,
      isUnique = true,
      withValues = false,
      IndexOrderCapability.NONE,
      IndexType.Range
    )
    IndexModifier(indexes(indexDef))
  }

  def relationshipTextIndexOn(relationshipType: String, properties: String*): IndexModifier = {
    val indexDef = relationshipIndexOn(
      relationshipType,
      properties,
      isUnique = false,
      withValues = false,
      IndexOrderCapability.NONE,
      IndexType.Text
    )
    IndexModifier(indexes(indexDef))
  }

  def relationshipPointIndexOn(relationshipType: String, properties: String*): IndexModifier = {
    val indexDef = relationshipIndexOn(
      relationshipType,
      properties,
      isUnique = false,
      withValues = false,
      IndexOrderCapability.NONE,
      IndexType.Point
    )
    IndexModifier(indexes(indexDef))
  }

  def uniqueIndexOn(label: String, properties: String*): IndexModifier = {
    val indexDef =
      indexOn(label, properties, isUnique = true, withValues = false, IndexOrderCapability.NONE, IndexType.Range)
    IndexModifier(indexes(indexDef))
  }

  def indexOn(
    label: String,
    properties: Seq[String],
    isUnique: Boolean,
    withValues: Boolean,
    providesOrder: IndexOrderCapability,
    indexType: IndexType
  ): IndexDef = {
    val indexAttributes = new IndexAttributes(isUnique, withValues, providesOrder)
    val indexDef = IndexDef(IndexDefinition.EntityType.Node(label), properties, indexType)
    indexes += indexDef -> indexAttributes
    indexDef
  }

  def relationshipIndexOn(
    relationshipType: String,
    properties: Seq[String],
    isUnique: Boolean,
    withValues: Boolean,
    providesOrder: IndexOrderCapability,
    indexType: IndexType
  ): IndexDef = {
    val indexAttributes = new IndexAttributes(isUnique, withValues, providesOrder)
    val indexDef = IndexDef(IndexDefinition.EntityType.Relationship(relationshipType), properties, indexType)
    indexes += indexDef -> indexAttributes
    indexDef
  }

  /**
   * Adds an existence constraint from the given label to the properties. This could also be a node key constraint.
   */
  def nodePropertyExistenceConstraintOn(label: String, properties: Set[String]): Unit = {
    nodeConstraints = nodeConstraints + (label -> properties)
  }

  /**
   * Adds an existence constraint from the given relationship type to the properties.
   *
   * This could later on also be a relationship key constraint, which is not implemented at the moment.
   */
  def relationshipPropertyExistenceConstraintOn(relType: String, properties: Set[String]): Unit = {
    relationshipConstraints = relationshipConstraints + (relType -> properties)
  }

  def procedure(signature: ProcedureSignature): Unit = {
    procedureSignatures += signature
  }
}

class StubbedLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration)
    extends LogicalPlanningConfiguration
    with LogicalPlanningConfigurationAdHocSemanticTable
    with FakeIndexAndConstraintManagement {

  self =>

  var knownLabels: Set[String] = Set.empty
  var knownRelationships: Set[String] = Set.empty
  var cardinality: PartialFunction[PlannerQuery, Cardinality] = PartialFunction.empty

  var cost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] =
    PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics: GraphStatistics = _
  var qg: QueryGraph = _

  var executionModel: ExecutionModel = parent.executionModel
  var lookupRelationshipsByType: LookupRelationshipsByType = LookupRelationshipsByType.default

  lazy val labelsById: Map[Int, String] = {
    val indexed = indexes.keys.collect {
      case IndexDef(IndexDefinition.EntityType.Node(label), _, _) => label
    }.toSeq
    val known = knownLabels.toSeq
    val indexedThenKnown = (indexed ++ known).distinct
    indexedThenKnown.zipWithIndex.map(_.swap).toMap
  }

  lazy val relTypesById: Map[Int, String] = {
    val indexed = indexes.keys.collect {
      case IndexDef(IndexDefinition.EntityType.Relationship(relationshipType), _, _) => relationshipType
    }.toSeq
    val known = knownRelationships.toSeq
    val indexedThenKnown = (indexed ++ known).distinct
    indexedThenKnown.zipWithIndex.map(_.swap).toMap
  }

  override def costModel(executionModel: ExecutionModel = executionModel): PartialFunction[
    (
      LogicalPlan,
      QueryGraphSolverInput,
      SemanticTable,
      Cardinalities,
      ProvidedOrders,
      Set[PropertyAccess],
      GraphStatistics,
      CostModelMonitor
    ),
    Cost
  ] = {
    case (lp, input, semanticTable, cardinalities, providedOrders, propertyAccess, statistics, monitor) =>
      // Calling this in any case has the benefit of having the monitor passed down to the real cost model
      val realCost =
        parent.costModel(executionModel)((
          lp,
          input,
          semanticTable,
          cardinalities,
          providedOrders,
          propertyAccess,
          statistics,
          monitor
        ))
      if (cost.isDefinedAt((lp, input, cardinalities, providedOrders))) {
        cost((lp, input, cardinalities, providedOrders))
      } else {
        realCost
      }
  }

  override def cardinalityModel(
    queryGraphCardinalityModel: QueryGraphCardinalityModel,
    selectivityCalculator: SelectivityCalculator,
    evaluator: ExpressionEvaluator
  ): CardinalityModel = {
    // noinspection ConvertExpressionToSAM
    new CardinalityModel {
      override def apply(
        pq: PlannerQuery,
        labelInfo: LabelInfo,
        relTypeInfo: RelTypeInfo,
        semanticTable: SemanticTable,
        indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
        cardinalityModel: CardinalityModel
      ): Cardinality = {
        val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
          case (name: String, cardinality: Cardinality) =>
            semanticTable.resolvedLabelNames(name) -> cardinality
        }
        val labelScanCardinality: PartialFunction[PlannerQuery, Cardinality] = {
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _)
            if queryGraph.patternNodes.size == 1 &&
              computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).isDefined =>
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).get
        }

        val r: PartialFunction[PlannerQuery, Cardinality] = labelScanCardinality.orElse(cardinality)
        if (r.isDefinedAt(pq)) r.apply(pq)
        else parent.cardinalityModel(queryGraphCardinalityModel, selectivityCalculator, evaluator)(
          pq,
          labelInfo,
          relTypeInfo,
          semanticTable,
          indexPredicateProviderContext,
          cardinalityModel
        )
      }
    }
  }

  private def computeOptionCardinality(
    queryGraph: QueryGraph,
    semanticTable: SemanticTable,
    labelIdCardinality: Map[LabelId, Cardinality]
  ) = {
    val labelMap: Map[LogicalVariable, Set[HasLabels]] = queryGraph.selections.labelPredicates
    val labels = queryGraph.patternNodes.flatMap(labelMap.get).flatten.flatMap(_.labels)
    val results = labels.collect {
      case label
        if semanticTable.id(label).isDefined &&
          labelIdCardinality.contains(semanticTable.id(label).get) =>
        labelIdCardinality(semanticTable.id(label).get)
    }
    results.headOption
  }

  override def graphStatistics: GraphStatistics =
    Option(statistics).getOrElse(parent.graphStatistics)

}
