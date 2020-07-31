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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, IndexOrderCapability}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, ProcedureSignature}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.{Expression, HasLabels}
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, Cost, LabelId}

class StubbedLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration)
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  self =>

  var knownLabels: Set[String] = Set.empty
  var knownRelationships: Set[String] = Set.empty
  var cardinality: PartialFunction[PlannerQuery, Cardinality] = PartialFunction.empty
  var cost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics: GraphStatistics = null
  var qg: QueryGraph = null
  var expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluator {
    override def evaluateExpression(expr: Expression): Option[Any] = ???

    override def isDeterministic(expr: Expression): Boolean = ???

    override def hasParameters(expr: Expression): Boolean = ???
  }

  var indexes: Map[IndexDef, IndexType] = Map.empty

  var procedureSignatures: Set[ProcedureSignature] = Set.empty

  lazy val labelsById: Map[Int, String] = indexes.keys.map(_.label).zipWithIndex.map(_.swap).toMap

  case class IndexModifier(indexType: IndexType) {
    def providesValues(): IndexModifier = {
      indexType.withValues = true
      this
    }
    def providesOrder(order: IndexOrderCapability): IndexModifier = {
      indexType.withOrdering = order
      this
    }
  }

  def indexOn(label: String, properties: String*): IndexModifier = {
    val indexType = new IndexType()
    indexes += IndexDef(label, properties) -> indexType
    IndexModifier(indexType)
  }

  def uniqueIndexOn(label: String, properties: String*): IndexModifier = {
    val indexType = new IndexType(isUnique = true)
    indexes += IndexDef(label, properties) -> indexType
    IndexModifier(indexType)
  }

  def procedure(signature: ProcedureSignature): Unit = {
    procedureSignatures += signature
  }

  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = cost.orElse(parent.costModel())

  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator): CardinalityModel = {
    new CardinalityModel {
      override def apply(pq: PlannerQuery, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
        val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
          case (name: String, cardinality: Cardinality) =>
            semanticTable.resolvedLabelNames(name) -> cardinality
        }
        val labelScanCardinality: PartialFunction[PlannerQuery, Cardinality] = {
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes.size == 1 &&
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).isDefined =>
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).get
        }

        val r: PartialFunction[PlannerQuery, Cardinality] = labelScanCardinality.orElse(cardinality)
        if (r.isDefinedAt(pq)) r.apply(pq) else parent.cardinalityModel(queryGraphCardinalityModel, evaluator)(pq, input, semanticTable)
      }
    }
  }

  private def computeOptionCardinality(queryGraph: QueryGraph, semanticTable: SemanticTable,
                                       labelIdCardinality: Map[LabelId, Cardinality]) = {
    val labelMap: Map[String, Set[HasLabels]] = queryGraph.selections.labelPredicates
    val labels = queryGraph.patternNodes.flatMap(labelMap.get).flatten.flatMap(_.labels)
    val results = labels.collect {
      case label if semanticTable.id(label).isDefined &&
                    labelIdCardinality.contains(semanticTable.id(label).get) =>
        labelIdCardinality(semanticTable.id(label).get)
    }
    results.headOption
  }

  override def graphStatistics: GraphStatistics =
    Option(statistics).getOrElse(parent.graphStatistics)

}
