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

import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.expressions.Expression
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
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.collection.mutable

trait LogicalPlanningConfiguration {
  def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable

  def cardinalityModel(
    queryGraphCardinalityModel: QueryGraphCardinalityModel,
    selectivityCalculator: SelectivityCalculator,
    expressionEvaluator: ExpressionEvaluator
  ): CardinalityModel

  def costModel(executionModel: ExecutionModel = executionModel): PartialFunction[
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
  ]
  def graphStatistics: GraphStatistics
  def indexes: Map[IndexDef, IndexAttributes]
  def nodeConstraints: Set[(String, Set[String])]
  def relationshipConstraints: Set[(String, Set[String])]
  def procedureSignatures: Set[ProcedureSignature]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def knownRelationships: Set[String]
  def labelsById: Map[Int, String]
  def relTypesById: Map[Int, String]
  def qg: QueryGraph
  def executionModel: ExecutionModel
  def lookupRelationshipsByType: LookupRelationshipsByType

  protected def mapCardinality(pf: PartialFunction[PlannerQuery, Double]): PartialFunction[PlannerQuery, Cardinality] =
    pf.andThen(Cardinality.apply(_))

  protected def selectivitiesCardinality(
    selectivities: Map[Expression, Double],
    baseCardinality: QueryGraph => Double
  ): PartialFunction[PlannerQuery, Cardinality] = mapCardinality {
    case RegularSinglePlannerQuery(queryGraph, _, _, _, _) =>
      queryGraph.selections.predicates.foldLeft(baseCardinality(queryGraph)) { case (rows, predicate) =>
        rows * selectivities(predicate.expr)
      }
  }
}

case class IndexDef(entityType: IndexDefinition.EntityType, propertyKeys: Seq[String], indexType: IndexType)

class IndexAttributes(
  var isUnique: Boolean = false,
  var withValues: Boolean = false,
  var withOrdering: IndexOrderCapability = IndexOrderCapability.NONE
)

trait LogicalPlanningConfigurationAdHocSemanticTable {
  self: LogicalPlanningConfiguration =>

  private val mappings = mutable.Map.empty[Expression, TypeSpec]

  def addTypeToSemanticTable(expr: Expression, typ: TypeSpec): Unit = {
    mappings += ((expr, typ))
  }

  override def updateSemanticTableWithTokens(table: SemanticTable): SemanticTable = {
    def addLabelIfUnknown(labelName: String) =
      if (!table.resolvedLabelNames.contains(labelName))
        table.resolvedLabelNames.put(labelName, LabelId(table.resolvedLabelNames.size))
    def addPropertyKeyIfUnknown(property: String) =
      if (!table.resolvedPropertyKeyNames.contains(property))
        table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
    def addRelationshipTypeIfUnknown(relationType: String) =
      if (!table.resolvedRelTypeNames.contains(relationType)) {
        table.resolvedRelTypeNames.put(relationType, RelTypeId(table.resolvedRelTypeNames.size))
      }
    indexes.keys.foreach {
      case IndexDef(IndexDefinition.EntityType.Node(label), properties, _) =>
        addLabelIfUnknown(label)
        properties.foreach(addPropertyKeyIfUnknown)
      case IndexDef(IndexDefinition.EntityType.Relationship(relationshipType), properties, _) =>
        addRelationshipTypeIfUnknown(relationshipType)
        properties.foreach(addPropertyKeyIfUnknown)
    }

    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)
    knownRelationships.foreach(addRelationshipTypeIfUnknown)

    var theTable = table
    for ((expr, typ) <- mappings) {
      theTable = theTable.copy(types = theTable.types + ((expr, ExpressionTypeInfo(typ, None))))
    }

    theTable
  }
}

sealed trait LookupRelationshipsByType {

  def canLookupRelationshipsByType: Boolean = this match {
    case LookupRelationshipsByTypeEnabled  => true
    case LookupRelationshipsByTypeDisabled => false
  }
}

object LookupRelationshipsByType {
  def default: LookupRelationshipsByType = LookupRelationshipsByTypeEnabled
}
case object LookupRelationshipsByTypeEnabled extends LookupRelationshipsByType
case object LookupRelationshipsByTypeDisabled extends LookupRelationshipsByType
