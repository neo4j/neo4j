/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cost, _}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, PropertyKeyId, LabelId}

trait LogicalPlanningConfiguration {
  def updateSemanticTableWithTokens(in: SemanticTable): SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel
  def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost]
  def graphStatistics: GraphStatistics
  def indexes: Set[(String, String)]
  def uniqueIndexes: Set[(String, String)]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
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

    indexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      addPropertyKeyIfUnknown(property)
    }
    uniqueIndexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      addPropertyKeyIfUnknown(property)
    }
    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)
    table
  }
}
