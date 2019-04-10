/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics._
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, IndexOrderCapability}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, ProcedureSignature}
import org.neo4j.cypher.internal.v3_5.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.symbols.TypeSpec
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, Cost, LabelId, PropertyKeyId}

import scala.collection.mutable

trait LogicalPlanningConfiguration {
  def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel
  def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost]
  def graphStatistics: GraphStatistics
  def indexes: Map[IndexDef, IndexType]
  // A subset of indexes
  def procedureSignatures: Set[ProcedureSignature]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def labelsById: Map[Int, String]
  def qg: QueryGraph

  protected def mapCardinality(pf: PartialFunction[PlannerQuery, Double]): PartialFunction[PlannerQuery, Cardinality] = pf.andThen(Cardinality.apply)
}

case class IndexDef(label: String, propertyKeys: Seq[String])
class IndexType(var isUnique: Boolean = false,
                var withValues: Boolean = false,
                var withOrdering: IndexOrderCapability = IndexOrderCapability.NONE)

class DelegatingLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration) extends LogicalPlanningConfiguration {
  override def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable = parent.updateSemanticTableWithTokens(in)
  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, expressionEvaluator: ExpressionEvaluator): CardinalityModel =
    parent.cardinalityModel(queryGraphCardinalityModel, expressionEvaluator)
  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = parent.costModel()
  override def graphStatistics: GraphStatistics = parent.graphStatistics
  override def indexes: Map[IndexDef, IndexType] = parent.indexes
  override def labelCardinality: Map[String, Cardinality] = parent.labelCardinality
  override def knownLabels: Set[String] = parent.knownLabels
  override def labelsById: Map[Int, String] = parent.labelsById
  override def qg: QueryGraph = parent.qg
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
