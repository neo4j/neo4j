/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.LabelId

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

trait FakeIndexAndConstraintManagement {
  var indexes: Map[IndexDef, IndexType] = Map.empty

  var nodeConstraints: Set[(String, Set[String])] = Set.empty

  var relationshipConstraints: Set[(String, Set[String])] = Set.empty

  var procedureSignatures: Set[ProcedureSignature] = Set.empty

  def indexOn(label: String, properties: String*): IndexModifier = {
    val indexDef = indexOn(label, properties, isUnique = false, withValues = false, IndexOrderCapability.NONE)
    IndexModifier(indexes(indexDef))
  }

  def relationshipIndexOn(relationshipType: String, properties: String*): IndexModifier = {
    val indexDef = relationshipIndexOn(relationshipType, properties, isUnique = false, withValues = false, IndexOrderCapability.NONE)
    IndexModifier(indexes(indexDef))
  }

  def uniqueIndexOn(label: String, properties: String*): IndexModifier = {
    val indexDef = indexOn(label, properties, isUnique = true, withValues = false, IndexOrderCapability.NONE)
    IndexModifier(indexes(indexDef))
  }

  def indexOn(label: String, properties: Seq[String], isUnique: Boolean, withValues: Boolean, providesOrder: IndexOrderCapability): IndexDef = {
    val indexType = new IndexType(isUnique, withValues, providesOrder)
    val indexDef = IndexDef(IndexDefinition.EntityType.Node(label), properties)
    indexes += indexDef -> indexType
    indexDef
  }

  def relationshipIndexOn(relationshipType: String, properties: Seq[String], isUnique: Boolean, withValues: Boolean, providesOrder: IndexOrderCapability): IndexDef = {
    val indexType = new IndexType(isUnique, withValues, providesOrder)
    val indexDef = IndexDef(IndexDefinition.EntityType.Relationship(relationshipType), properties)
    indexes += indexDef -> indexType
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
  var cardinality: PartialFunction[PlannerQueryPart, Cardinality] = PartialFunction.empty
  var cost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] = PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics: GraphStatistics = _
  var qg: QueryGraph = _
  var expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluator {
    override def evaluateExpression(expr: Expression): Option[Any] = ???

    override def isDeterministic(expr: Expression): Boolean = ???

    override def hasParameters(expr: Expression): Boolean = ???
  }
  var lookupRelationshipsByType: LookupRelationshipsByType = LookupRelationshipsByType.default

  lazy val labelsById: Map[Int, String] = {
    val indexed = indexes.keys.collect {
      case IndexDef(IndexDefinition.EntityType.Node(label), _) => label
    }.toSeq
    val known = knownLabels.toSeq
    val indexedThenKnown = (indexed ++ known).distinct
    indexedThenKnown.zipWithIndex.map(_.swap).toMap
  }

  lazy val relTypesById: Map[Int, String] = {
    val indexed = indexes.keys.collect {
      case IndexDef(IndexDefinition.EntityType.Relationship(relationshipType), _) => relationshipType
    }.toSeq
    val known = knownRelationships.toSeq
    val indexedThenKnown = (indexed ++ known).distinct
    indexedThenKnown.zipWithIndex.map(_.swap).toMap
  }

  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, SemanticTable, Cardinalities, ProvidedOrders, CostModelMonitor), Cost] = {
    case (lp, input, semanticTable, cardinalities, providedOrders, monitor) =>
      // Calling this in any case has the benefit of having the monitor passed down to the real cost model
      val realCost = parent.costModel()((lp, input, semanticTable, cardinalities, providedOrders, monitor))
      if (cost.isDefinedAt((lp, input, cardinalities, providedOrders))) {
        cost((lp, input, cardinalities, providedOrders))
      } else {
        realCost
      }
  }

  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator): CardinalityModel = {
    new CardinalityModel {
      override def apply(pq: PlannerQueryPart, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
        val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
          case (name: String, cardinality: Cardinality) =>
            semanticTable.resolvedLabelNames(name) -> cardinality
        }
        val labelScanCardinality: PartialFunction[PlannerQueryPart, Cardinality] = {
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes.size == 1 &&
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).isDefined =>
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).get
        }

        val r: PartialFunction[PlannerQueryPart, Cardinality] = labelScanCardinality.orElse(cardinality)
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
