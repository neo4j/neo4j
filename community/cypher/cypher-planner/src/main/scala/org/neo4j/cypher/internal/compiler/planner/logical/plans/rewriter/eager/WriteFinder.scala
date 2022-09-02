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
   * Write operations of a certain kind of a single plan.
   */
  abstract private[eager] class PlanWriteOperations {

    /**
     * @return all properties written by this plan
     */
    def writtenProperties: Seq[PropertyKeyName]

    /**
     * @return `true` if this plan writes unknown properties, e.g. by doing SET m = n
     */
    def writesUnknownProperties: Boolean

    def withPropertyWritten(property: PropertyKeyName): PlanWriteOperations
    def withUnknownPropertiesWritten: PlanWriteOperations

    /**
     *
     * @param labels labels written on the same node for each write
     * @return
     */
    def withLabelsWritten(labels: Set[LabelName]): PlanWriteOperations

    /**
     * @return A `Seq` of all writtem properties.
     *         For a known `PropertyKeyName` (`Some`) and for an unknown `PropertyKeyName` (`None`).
     */
    def writtenPropertiesIncludingUnknown: Seq[Option[PropertyKeyName]] = {
      val concrete = writtenProperties.map(Some(_))
      if (writesUnknownProperties) concrete :+ None else concrete
    }
  }

  /**
   * Set write operations of a single plan
   * Labels written on the same node for sets does not need to differentiated
   * since there can be a situation where another label already exists on the node.
   */
  private[eager] case class PlanSets(
    override val writtenProperties: Seq[PropertyKeyName] = Seq.empty,
    override val writesUnknownProperties: Boolean = false,
    writtenLabels: Set[LabelName] = Set.empty
  ) extends PlanWriteOperations {

    override def withPropertyWritten(property: PropertyKeyName): PlanSets =
      copy(writtenProperties = writtenProperties :+ property)

    override def withUnknownPropertiesWritten: PlanSets = copy(writesUnknownProperties = true)

    override def withLabelsWritten(labels: Set[LabelName]): PlanSets = copy(writtenLabels = writtenLabels ++ labels)
  }

  /**
   * Create write operations of a single plan
   * Labels needs to be differentiated based on each create
   * since we can then assume that no other label is on that node since it was just created.
   *
   * @param createsNodes `true` if this plan creates nodes
   */
  private[eager] case class PlanCreates(
    override val writtenProperties: Seq[PropertyKeyName] = Seq.empty,
    override val writesUnknownProperties: Boolean = false,
    writtenLabels: Set[Set[LabelName]] = Set.empty,
    createsNodes: Boolean = false
  ) extends PlanWriteOperations {

    def withCreatesNodes: PlanCreates = copy(createsNodes = true)

    override def withPropertyWritten(property: PropertyKeyName): PlanCreates =
      copy(writtenProperties = writtenProperties :+ property)

    override def withUnknownPropertiesWritten: PlanCreates = copy(writesUnknownProperties = true)

    override def withLabelsWritten(labels: Set[LabelName]): PlanCreates = copy(writtenLabels = writtenLabels + labels)
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
      val setPart = processSetMutatingPatterns(PlanSets(), onMatch)

      val createPart = Option(PlanCreates())
        .map(acc => processCreateNodes(acc, nodes))
        .map(acc => processSetMutatingPatterns(acc, onCreate))
        .get

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

  private def processPropertyMap[OP <: PlanWriteOperations](acc: OP, properties: MapExpression): OP = {
    val res = properties.items.foldLeft[PlanWriteOperations](acc) {
      case (acc, (propertyName, _)) => acc.withPropertyWritten(propertyName)
    }
    res.asInstanceOf[OP]
  }

  private def processCreateNodes(acc: PlanCreates, nodes: Seq[CreateNode]): PlanCreates = {
    nodes.foldLeft(acc) {
      case (acc, CreateNode(_, labels, maybeProperties)) =>
        Option(acc)
          .map(acc => acc.withCreatesNodes)
          .map(acc => acc.withLabelsWritten(labels))
          .map(acc =>
            maybeProperties match {
              case None                            => acc
              case Some(properties: MapExpression) => processPropertyMap(acc, properties)
              case Some(_)                         => acc.withUnknownPropertiesWritten
            }
          )
          .get
    }
  }

  private def processSetMutatingPatterns[OP <: PlanWriteOperations](
    acc: OP,
    setMutatingPatterns: Seq[SetMutatingPattern]
  ): OP = {
    val res = setMutatingPatterns.foldLeft[PlanWriteOperations](acc) {
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
    res.asInstanceOf[OP]
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
