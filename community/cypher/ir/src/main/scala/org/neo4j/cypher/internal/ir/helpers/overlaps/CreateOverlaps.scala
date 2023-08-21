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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.label_expressions.SolvableLabelExpression

object CreateOverlaps {

  /**
   * Checks whether the labels and the property keys on the node being created might overlap with the predicates of the node being matched on.
   * @param predicates Predicates of the node being matched on. All predicates must apply exclusively to the node at hand, there is no checks for variable name.
   * @param labelsToCreate Labels on the node being created.
   * @param propertiesToCreate Keys of the properties being created.
   * @return Whether or not the existing node and the node to create might overlap.
   */
  def overlap(
    predicates: Seq[Expression],
    labelsToCreate: Set[String],
    propertiesToCreate: CreatesPropertyKeys
  ): Result = {
    val (unsupportedExpressions, nodePredicates) = extractNodePredicates(predicates)

    val propertiesOverlap = propertyOverlap(propertiesToCreate, nodePredicates.properties)

    propertiesOverlap match {
      case None => NoPropertyOverlap
      case Some(propOverlap) =>
        if (nodePredicates.labelExpressions.forall(_.matchesLabels(labelsToCreate)))
          Overlap(unsupportedExpressions, propOverlap, KnownLabels(labelsToCreate))
        else
          NoLabelOverlap
    }
  }

  sealed trait Result

  /**
   * Proves that there is no overlap because of properties.
   */
  case object NoPropertyOverlap extends Result

  /**
   * Proves that there is no overlap because of labels.
   */
  case object NoLabelOverlap extends Result

  /**
   * Details of the potential overlap on create.
   * @param unprocessedExpressions The expressions that were not processed by the evaluator.
   * @param propertiesOverlap Set of properties responsible for the overlap.
   * @param labelsOverlap Node labels responsible for the overlap.
   */
  case class Overlap(
    unprocessedExpressions: Seq[Expression],
    propertiesOverlap: PropertiesOverlap,
    labelsOverlap: NodeLabels
  ) extends Result

  /**
   * Details of the overlap between a node being created and an existing node.
   */
  sealed trait PropertiesOverlap

  object PropertiesOverlap {

    /**
     * Known set of properties causing the overlap.
     */
    case class Overlap(properties: Set[PropertyKeyName]) extends PropertiesOverlap

    /**
     * Some unknown properties are being created, and so an overlap cannot be ruled out.
     */
    case object UnknownOverlap extends PropertiesOverlap
  }

  /**
   * Node predicates that are relevant for calculating the overlap.
   * @param labelExpressions Conjoint list of label expressions, empty is equivalent to (:%|!%) also known as ().
   * @param properties Name of the properties that must exist on the node.
   */
  private case class NodePredicates(
    labelExpressions: Seq[SolvableLabelExpression],
    properties: Set[PropertyKeyName]
  ) {

    def combine(other: NodePredicates): NodePredicates =
      NodePredicates(
        labelExpressions = labelExpressions ++ other.labelExpressions,
        properties = properties.union(other.properties)
      )
  }

  private object NodePredicates {

    def empty: NodePredicates =
      NodePredicates(labelExpressions = Vector.empty, properties = Set.empty)

    def fold(nodePredicates: Seq[NodePredicates]): NodePredicates =
      nodePredicates.reduceLeftOption(_.combine(_)).getOrElse(empty)

    def withLabelExpression(labelExpression: SolvableLabelExpression): NodePredicates =
      empty.copy(labelExpressions = Vector(labelExpression))

    def withProperty(propertyKeyName: PropertyKeyName): NodePredicates =
      empty.copy(properties = Set(propertyKeyName))
  }

  private def extractNodePredicates(expressions: Seq[Expression]): (Seq[Expression], NodePredicates) =
    expressions.flatMap(Expressions.splitExpression).partitionMap(expression =>
      Expressions.extractPropertyExpression(expression).map(NodePredicates.withProperty)
        .orElse(Expressions.extractLabelExpression(expression).map(NodePredicates.withLabelExpression))
        .toRight(expression)
    ) match {
      case (unprocessedExpressions, values) => (unprocessedExpressions, NodePredicates.fold(values))
    }

  /**
   * Checks whether there might be an overlap between the properties of the node being created and the ones referred to in the predicates of an existing node.
   * @param propertiesToCreate Properties of the node being created.
   * @param nodeProperties Properties that must be present on an existing node based on its predicates.
   * @return None if there can be no overlap, details of the overlap otherwise.
   */
  private def propertyOverlap(
    propertiesToCreate: CreatesPropertyKeys,
    nodeProperties: Set[PropertyKeyName]
  ): Option[PropertiesOverlap] = {
    propertiesToCreate match {
      case CreatesNoPropertyKeys =>
        if (nodeProperties.isEmpty)
          Some(PropertiesOverlap.Overlap(Set.empty))
        else
          None // If the existing node has at least one property, and the created node has no properties, then there can be no overlap.
      case CreatesKnownPropertyKeys(keys) =>
        if (nodeProperties.subsetOf(keys)) // Note that the empty set is a subset of any other set.
          Some(PropertiesOverlap.Overlap(nodeProperties))
        else
          None // If the existing node has at least one property that the node being created does not have, then there can be no overlap.
      case CreatesUnknownPropertyKeys =>
        Some(PropertiesOverlap.UnknownOverlap)
    }
  }
}
