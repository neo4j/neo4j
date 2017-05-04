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

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, HasLabels}
import org.neo4j.cypher.internal.frontend.v3_2.{LabelId, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2._

class StubbedLogicalPlanningConfiguration(parent: LogicalPlanningConfiguration)
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  self =>

  var knownLabels: Set[String] = Set.empty
  var cardinality: PartialFunction[PlannerQuery, Cardinality] = PartialFunction.empty
  var cost: PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost] = PartialFunction.empty
  var selectivity: PartialFunction[Expression, Selectivity] = PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics = null
  var qg: QueryGraph = null

  var indexes: Set[(String, Seq[String])] = Set.empty
  var uniqueIndexes: Set[(String, Seq[String])] = Set.empty

  lazy val labelsById: Map[Int, String] = (indexes ++ uniqueIndexes).map(_._1).zipWithIndex.map(_.swap).toMap

  def indexOn(label: String, properties: String*) {
    indexes = indexes + (label -> properties)
  }

  def uniqueIndexOn(label: String, properties: String*) {
    uniqueIndexes = uniqueIndexes + (label -> properties)
  }

  def costModel() = cost.orElse(parent.costModel())

  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel = {
    (pq: PlannerQuery, input: QueryGraphSolverInput, semanticTable: SemanticTable) => {
      val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
        case (name: String, cardinality: Cardinality) =>
          semanticTable.resolvedLabelIds(name) -> cardinality
      }
      val labelScanCardinality: PartialFunction[PlannerQuery, Cardinality] = {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 &&
          computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).isDefined =>
          computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).get
      }

      val r: PartialFunction[PlannerQuery, Cardinality] = labelScanCardinality.orElse(cardinality)
      if (r.isDefinedAt(pq)) r.apply(pq) else parent.cardinalityModel(queryGraphCardinalityModel)(pq, input, semanticTable)
    }
  }

  private def computeOptionCardinality(queryGraph: QueryGraph, semanticTable: SemanticTable,
                                       labelIdCardinality: Map[LabelId, Cardinality]) = {
    val labelMap: Map[IdName, Set[HasLabels]] = queryGraph.selections.labelPredicates
    val labels = queryGraph.patternNodes.flatMap(labelMap.get).flatten.flatMap(_.labels)
    val results = labels.collect {
      case label if label.id(semanticTable).isDefined && labelIdCardinality.contains(label.id(semanticTable).get) =>
        labelIdCardinality(label.id(semanticTable).get)
    }
    results.headOption
  }

  def graphStatistics: GraphStatistics =
    Option(statistics).getOrElse(parent.graphStatistics)
}
