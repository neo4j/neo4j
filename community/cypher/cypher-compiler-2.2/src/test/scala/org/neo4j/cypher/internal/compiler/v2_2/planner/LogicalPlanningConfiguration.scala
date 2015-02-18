/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, PropertyKeyId, LabelId, HardcodedGraphStatistics}
import org.neo4j.cypher.internal.compiler.v2_2.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{NodeByLabelScan, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.helpers.collection.Visitable
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor

import scala.collection.mutable

trait LogicalPlanningConfiguration {
  def computeSemanticTable: SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable): Metrics.CardinalityModel
  def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost]
  def graphStatistics: GraphStatistics
  def indexes: Set[(String, String)]
  def uniqueIndexes: Set[(String, String)]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def qg: QueryGraph

  protected def mapCardinality(pf: PartialFunction[LogicalPlan, Double]): PartialFunction[LogicalPlan, Cardinality] = pf.andThen(Cardinality.apply)
}

trait LogicalPlanningConfigurationAdHocSemanticTable {
  self: LogicalPlanningConfiguration =>

  override def computeSemanticTable: SemanticTable = {
    val table = SemanticTable()
    def addLabelIfUnknown(labelName: String) =
      if (!table.resolvedLabelIds.contains(labelName))
        table.resolvedLabelIds.put(labelName, LabelId(table.resolvedLabelIds.size))

    indexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
    }
    uniqueIndexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
    }
    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)
    table
  }
}
case class RealLogicalPlanningConfiguration()
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable) = {
    val model: Metrics.CardinalityModel = new StatisticsBackedCardinalityModel(queryGraphCardinalityModel)
    ({ case (plan: LogicalPlan, card: QueryGraphCardinalityInput) => model(plan, card) })
  }

  def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost] = {
    val model: Metrics.CostModel = new CardinalityCostModel(cardinality)
    ({ case (plan: LogicalPlan) => model(plan, QueryGraphCardinalityInput.empty) })
  }

  def graphStatistics: GraphStatistics = HardcodedGraphStatistics

  def indexes = Set.empty
  def uniqueIndexes = Set.empty
  def labelCardinality = Map.empty
  def knownLabels = Set.empty

  def qg: QueryGraph = ???
}

class StubbedLogicalPlanningConfiguration(parent: LogicalPlanningConfiguration)
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  self =>

  var knownLabels: Set[String] = Set.empty
  var cardinality: PartialFunction[LogicalPlan, Cardinality] = PartialFunction.empty
  var cost: PartialFunction[LogicalPlan, Cost] = PartialFunction.empty
  var selectivity: PartialFunction[Expression, Selectivity] = PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics = null
  var qg: QueryGraph = null

  var indexes: Set[(String, String)] = Set.empty
  var uniqueIndexes: Set[(String, String)] = Set.empty
  def indexOn(label: String, property: String) {
    indexes = indexes + (label -> property)
  }
  def uniqueIndexOn(label: String, property: String) {
    uniqueIndexes = uniqueIndexes + (label -> property)
  }

  def costModel(cardinality: Metrics.CardinalityModel) =
    cost.orElse(parent.costModel(cardinality))

  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable): Metrics.CardinalityModel = {
    val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
      case (name: String, cardinality: Cardinality) =>
        semanticTable.resolvedLabelIds(name) -> cardinality
    }
    val labelScanCardinality: PartialFunction[LogicalPlan, Cardinality] = {
      case NodeByLabelScan(_, label, _) if label.id(semanticTable).isDefined &&
        labelIdCardinality.contains(label.id(semanticTable).get) =>
        labelIdCardinality(label.id(semanticTable).get)
    }

    val r: PartialFunction[LogicalPlan, Cardinality] =
      labelScanCardinality.orElse(cardinality)

    (p: LogicalPlan, c: QueryGraphCardinalityInput) =>
      if(r.isDefinedAt(p)) r.apply(p) else parent.cardinalityModel(queryGraphCardinalityModel, semanticTable)(p, c)
  }

  def graphStatistics: GraphStatistics =
    Option(statistics).getOrElse(parent.graphStatistics)
}
