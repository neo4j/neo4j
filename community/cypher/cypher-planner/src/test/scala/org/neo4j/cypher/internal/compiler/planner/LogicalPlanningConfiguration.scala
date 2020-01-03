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

import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics._
import org.neo4j.cypher.internal.ir.{PlannerQueryPart, QueryGraph}
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, ProcedureSignature}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.{GraphStatistics, IndexOrderCapability}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.symbols.TypeSpec
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Cost, LabelId, PropertyKeyId}

import scala.collection.mutable

trait LogicalPlanningConfiguration {
  def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel
  def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost]
  def graphStatistics: GraphStatistics
  def indexes: Map[IndexDef, IndexType]
  def constraints: Set[(String, Set[String])]
  def procedureSignatures: Set[ProcedureSignature]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def labelsById: Map[Int, String]
  def qg: QueryGraph

  protected def mapCardinality(pf: PartialFunction[PlannerQueryPart, Double]): PartialFunction[PlannerQueryPart, Cardinality] = pf.andThen(Cardinality.apply)
}

case class IndexDef(label: String, propertyKeys: Seq[String])
class IndexType(var isUnique: Boolean = false,
                var withValues: Boolean = false,
                var withOrdering: IndexOrderCapability = IndexOrderCapability.NONE)

class DelegatingLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration) extends LogicalPlanningConfiguration {
  override def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable = parent.updateSemanticTableWithTokens(in)
  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel =
    parent.cardinalityModel(queryGraphCardinalityModel, expressionEvaluator)
  override def costModel() = parent.costModel()
  override def graphStatistics = parent.graphStatistics
  override def indexes = parent.indexes
  override def constraints: Set[(String, Set[String])] = parent.constraints
  override def labelCardinality = parent.labelCardinality
  override def knownLabels = parent.knownLabels
  override def labelsById = parent.labelsById
  override def qg = parent.qg
  override def procedureSignatures: Set[ProcedureSignature] = parent.procedureSignatures
}

trait LogicalPlanningConfigurationAdHocSemanticTable {
  self: LogicalPlanningConfiguration =>

  private var mappings = mutable.Map.empty[Expression, TypeSpec]

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

    indexes.keys.foreach { case IndexDef(label, properties) =>
      addLabelIfUnknown(label)
      properties.foreach(addPropertyKeyIfUnknown)
    }

    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)

    var theTable = table
    for((expr, typ) <- mappings) {
      theTable = theTable.copy(types = theTable.types + ((expr, ExpressionTypeInfo(typ, None))))
    }

    theTable
  }
}
