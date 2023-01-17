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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan

/**
 * Finds all writes for a single plan.
 */
object WriteFinder {

  /**
   * Set write operations of a single plan
   * Labels written on the same node for sets does not need to differentiated
   * since there can be a situation where another label already exists on the node.
   *
   * @param writtenProperties       all properties written by this plan
   * @param writesUnknownProperties `true` if this plan writes unknown properties, e.g. by doing SET m = n
   */
  private[eager] case class PlanSets(
    writtenProperties: Seq[PropertyKeyName] = Seq.empty,
    writesUnknownProperties: Boolean = false,
    writtenLabels: Set[LabelName] = Set.empty
  ) {

    def withPropertyWritten(property: PropertyKeyName): PlanSets =
      copy(writtenProperties = writtenProperties :+ property)

    def withUnknownPropertiesWritten: PlanSets = copy(writesUnknownProperties = true)

    /**
     * Call this to signal that this plan writes some labels.
     * The method should be called once for each node, with all the labels that are written for that node.
     * If a plan writes labels on multiple nodes, it should call this method multiple times.
     *
     * @param labels labels written on the same node for each write
     */
    def withLabelsWritten(labels: Set[LabelName]): PlanSets = copy(writtenLabels = writtenLabels ++ labels)
  }

  /**
   * A single node from a CREATE or MERGE
   *
   * @param createdLabels     the created labels on that node
   * @param createdProperties the properties that get created
   */
  private[eager] case class CreatedNode(createdLabels: Set[LabelName], createdProperties: CreatesPropertyKeys)

  /**
   * Create write operations of a single plan
   */
  private[eager] case class PlanCreates(createdNodes: Set[CreatedNode] = Set.empty) {
    def withCreatedNode(createdNode: CreatedNode): PlanCreates = copy(createdNodes = createdNodes + createdNode)
  }

  /**
   * Writes of a single plan.
   * The Seqs nested in this class may contain duplicates. These are filtered out later in [[ConflictFinder]].
   */
  private[eager] case class PlanWrites(
    sets: PlanSets = PlanSets(),
    creates: PlanCreates = PlanCreates()
  )

  /**
   * Collects the writes of a single plan, not traversing into child plans.
   */
  private[eager] def collectWrites(plan: LogicalPlan): PlanWrites = plan match {
    case SetNodeProperty(_, _, propertyName, _) =>
      PlanWrites(sets = PlanSets(writtenProperties = Seq(propertyName)))

    case SetNodeProperties(_, _, assignments) =>
      PlanWrites(sets = PlanSets(writtenProperties = assignments.map(_._1)))

    // Set concrete properties, do not delete any other properties
    case SetNodePropertiesFromMap(_, _, properties: MapExpression, false) =>
      PlanWrites(sets = PlanSets(properties.items.map(_._1)))

    // Set unknown properties (i.e. not a MapExpression) or delete any other properties
    case _: SetNodePropertiesFromMap =>
      PlanWrites(sets = PlanSets(writesUnknownProperties = true))

    case SetLabels(_, _, labelNames) =>
      PlanWrites(sets = PlanSets(writtenLabels = labelNames))

    case RemoveLabels(_, _, labelNames) =>
      PlanWrites(sets = PlanSets(writtenLabels = labelNames))

    case Create(_, nodes, relationships) =>
      val creates = processCreateNodes(PlanCreates(), nodes)
      PlanWrites(creates = creates)

    case Merge(_, nodes, relationships, onMatch, onCreate, _) =>
      val setPart = Function.chain[PlanSets](Seq(
        processSetMutatingPatterns(_, onMatch),
        processSetMutatingPatterns(_, onCreate)
      ))(PlanSets())

      val createPart = processCreateNodes(PlanCreates(), nodes)

      PlanWrites(setPart, createPart)

    case Foreach(_, _, _, mutations) =>
      val (setMutatingPatterns: Seq[SetMutatingPattern], otherMutatingPatterns) =
        mutations.toSeq.partition(mutation => mutation.isInstanceOf[SetMutatingPattern])
      val setPart = processSetMutatingPatterns(PlanSets(), setMutatingPatterns)
      val createPart = processCreateMutatingPatterns(PlanCreates(), otherMutatingPatterns)

      PlanWrites(setPart, createPart)

    case plan: UpdatingPlan =>
      throw new UnsupportedOperationException(
        s"Eagerness support for operator ${plan.productPrefix} not implemented yet."
      )
    case _ => PlanWrites()
  }

  private def processPropertyMap(acc: PlanSets, properties: MapExpression): PlanSets = {
    properties.items.foldLeft[PlanSets](acc) {
      case (acc, (propertyName, _)) => acc.withPropertyWritten(propertyName)
    }
  }

  private def processCreateNodes(acc: PlanCreates, nodes: Seq[CreateNode]): PlanCreates = {
    nodes.foldLeft(acc) {
      case (acc, CreateNode(_, labels, maybeProperties)) =>
        Option(acc)
          .map(acc =>
            maybeProperties match {
              case None => acc.withCreatedNode(CreatedNode(labels, CreatesNoPropertyKeys))
              case Some(MapExpression(properties)) =>
                acc.withCreatedNode(CreatedNode(labels, CreatesKnownPropertyKeys(properties.map(_._1).toSet)))
              case Some(_) => acc.withCreatedNode(CreatedNode(labels, CreatesUnknownPropertyKeys))
            }
          )
          .get
    }
  }

  private def processSetMutatingPatterns(
    acc: PlanSets,
    setMutatingPatterns: Seq[SetMutatingPattern]
  ): PlanSets = {
    setMutatingPatterns.foldLeft[PlanSets](acc) {
      case (acc, SetNodePropertyPattern(_, propertyName, _)) => acc.withPropertyWritten(propertyName)
      case (acc, SetNodePropertiesPattern(_, assignments)) =>
        assignments.foldLeft(acc) {
          case (acc, (propertyName, _)) => acc.withPropertyWritten(propertyName)
        }
      // Set concrete properties, do not delete any other properties
      case (acc, SetNodePropertiesFromMapPattern(_, properties: MapExpression, false)) =>
        processPropertyMap(acc, properties)

      // Set unknown properties (i.e. not a MapExpression) or delete any other properties
      case (acc, _: SetNodePropertiesFromMapPattern) =>
        acc.withUnknownPropertiesWritten

      case (acc, SetLabelPattern(_, labels)) =>
        acc.withLabelsWritten(labels.toSet)

      case (_, mutatingPattern) =>
        throw new UnsupportedOperationException(
          s"Eagerness support for mutating pattern ${mutatingPattern.productPrefix} not implemented yet."
        )
    }
  }

  private def processCreateMutatingPatterns(
    acc: PlanCreates,
    setMutatingPatterns: Seq[SimpleMutatingPattern]
  ): PlanCreates = {
    val res = setMutatingPatterns.foldLeft[PlanCreates](acc) {
      case (acc, CreatePattern(nodes, relationships)) =>
        processCreateNodes(acc, nodes)

      case (_, mutatingPattern) =>
        throw new UnsupportedOperationException(
          s"Eagerness support for mutating pattern ${mutatingPattern.productPrefix} not implemented yet."
        )
    }
    res
  }
}
