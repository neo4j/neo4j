/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, PropertyKeyId, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, Cost, PlannerQuery, QueryGraph}

trait LogicalPlanningConfiguration {
  def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel
  def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost]
  def graphStatistics: GraphStatistics
  def indexes: Set[(String, Seq[String])]
  def uniqueIndexes: Set[(String, Seq[String])]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def labelsById: Map[Int, String]
  def qg: QueryGraph

  protected def mapCardinality(pf: PartialFunction[PlannerQuery, Double]): PartialFunction[PlannerQuery, Cardinality] = pf.andThen(Cardinality.apply)
}

class DelegatingLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration) extends LogicalPlanningConfiguration {
  override def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable = parent.updateSemanticTableWithTokens(in)
  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel =
    parent.cardinalityModel(queryGraphCardinalityModel)
  override def costModel() = parent.costModel()
  override def graphStatistics = parent.graphStatistics
  override def indexes = parent.indexes
  override def uniqueIndexes = parent.uniqueIndexes
  override def labelCardinality = parent.labelCardinality
  override def knownLabels = parent.knownLabels
  override def labelsById = parent.labelsById
  override def qg = parent.qg
}

trait LogicalPlanningConfigurationAdHocSemanticTable {
  self: LogicalPlanningConfiguration =>

  override def updateSemanticTableWithTokens(table: SemanticTable): SemanticTable = {
    def addLabelIfUnknown(labelName: String) =
      if (!table.resolvedLabelIds.contains(labelName))
        table.resolvedLabelIds.put(labelName, LabelId(table.resolvedLabelIds.size))
    def addPropertyKeyIfUnknown(property: String) =
      if (!table.resolvedPropertyKeyNames.contains(property))
        table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))

    indexes.foreach { case (label, properties) =>
      addLabelIfUnknown(label)
      properties.foreach(addPropertyKeyIfUnknown(_))
    }
    uniqueIndexes.foreach { case (label, properties) =>
      addLabelIfUnknown(label)
      properties.foreach(addPropertyKeyIfUnknown(_))
    }
    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)
    table
  }
}
