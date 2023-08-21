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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.logical.plans.create.CreateNode
import org.neo4j.cypher.internal.logical.plans.create.CreateRelationship
import org.neo4j.cypher.internal.logical.plans.set.CreatePattern
import org.neo4j.cypher.internal.logical.plans.set.DeleteMutatingPattern
import org.neo4j.cypher.internal.logical.plans.set.RemoveLabelPattern
import org.neo4j.cypher.internal.logical.plans.set.SetLabelPattern
import org.neo4j.cypher.internal.logical.plans.set.SetMutatingPattern
import org.neo4j.cypher.internal.logical.plans.set.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.logical.plans.set.SetNodePropertiesPattern
import org.neo4j.cypher.internal.logical.plans.set.SetNodePropertyPattern
import org.neo4j.cypher.internal.logical.plans.set.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.logical.plans.set.SetPropertiesPattern
import org.neo4j.cypher.internal.logical.plans.set.SetPropertyPattern
import org.neo4j.cypher.internal.logical.plans.set.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.logical.plans.set.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.logical.plans.set.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.logical.plans.set.SimpleMutatingPattern

/**
 * Finds all writes for a single plan.
 */
object WriteFinder {

  /**
   * Set write operations of a single plan
   * Labels written on the same node for sets does not need to differentiated
   * since there can be a situation where another label already exists on the node.
   *
   * @param writtenNodeProperties       all node properties written by this plan
   * @param writesUnknownNodeProperties `true` if this plan writes unknown node properties, e.g. by doing SET m = n
   * @param writtenLabels all labels written by this plan
   * @param writtenRelProperties       all relationship properties written by this plan
   * @param writesUnknownRelProperties `true` if this plan writes unknown relationship properties, e.g. by doing SET m = n
   */
  private[eager] case class PlanSets(
    writtenNodeProperties: Seq[PropertyKeyName] = Seq.empty,
    writtenRelProperties: Seq[PropertyKeyName] = Seq.empty,
    writesUnknownNodeProperties: Boolean = false,
    writesUnknownRelProperties: Boolean = false,
    writtenLabels: Set[LabelName] = Set.empty
  ) {

    def withNodePropertyWritten(property: PropertyKeyName): PlanSets =
      copy(writtenNodeProperties = writtenNodeProperties :+ property)

    def withRelPropertyWritten(property: PropertyKeyName): PlanSets =
      copy(writtenRelProperties = writtenRelProperties :+ property)

    def withUnknownNodePropertiesWritten: PlanSets = copy(writesUnknownNodeProperties = true)

    def withUnknownRelPropertiesWritten: PlanSets = copy(writesUnknownRelProperties = true)

    /**
     * Call this to signal that this plan writes some labels.
     * The method should be called once for each node, with all the labels that are written for that node.
     * If a plan writes labels on multiple nodes, it should call this method multiple times.
     *
     * @param labels labels written on the same node for each write
     */
    def withLabelsWritten(labels: Set[LabelName]): PlanSets = copy(writtenLabels = writtenLabels ++ labels)
  }

  sealed private[eager] trait CreatedEntity[A] {
    def getCreatedProperties: CreatesPropertyKeys
    def getCreatedLabelsOrTypes: Set[A]
  }

  /**
   * A single node from a CREATE or MERGE
   *
   * @param createdLabels     the created labels on that node
   * @param createdProperties the properties that get created
   */
  private[eager] case class CreatedNode(createdLabels: Set[LabelName], createdProperties: CreatesPropertyKeys)
      extends CreatedEntity[LabelName] {
    override def getCreatedLabelsOrTypes: Set[LabelName] = createdLabels
    override def getCreatedProperties: CreatesPropertyKeys = createdProperties
  }

  /**
   * A single relationship from a CREATE or MERGE
   *
   * @param createdType  the created types on that relationship
   * @param createdProperties the relationship properties that get created
   */
  private[eager] case class CreatedRelationship(createdType: RelTypeName, createdProperties: CreatesPropertyKeys)
      extends CreatedEntity[RelTypeName] {
    override def getCreatedLabelsOrTypes: Set[RelTypeName] = Set(createdType)
    override def getCreatedProperties: CreatesPropertyKeys = createdProperties
  }

  /**
   * Create write operations of a single plan
   */
  private[eager] case class PlanCreates(
    createdNodes: Set[CreatedNode] = Set.empty,
    createdRelationships: Set[CreatedRelationship] = Set.empty
  ) {
    def withCreatedNode(createdNode: CreatedNode): PlanCreates = copy(createdNodes = createdNodes + createdNode)

    def withCreatedRelationship(createdRelationship: CreatedRelationship): PlanCreates =
      copy(createdRelationships = createdRelationships + createdRelationship)
  }

  /**
   * Delete write operations of a single plan
   *
   * @param deletedNodeVariables nodes that are deleted by variable name
   * @param deletesNodeExpressions non-variable expressions of type node are deleted
   * @param deletedRelationshipVariables relationships that are deleted by variable name
   * @param deletesRelationshipExpressions non-variable expressions of type relationship are deleted
   * @param deletesUnknownTypeExpressions expressions that are not nodes or relationships are deleted
   */
  private[eager] case class PlanDeletes(
    deletedNodeVariables: Set[Variable] = Set.empty,
    deletedRelationshipVariables: Set[Variable] = Set.empty,
    deletesNodeExpressions: Boolean = false,
    deletesRelationshipExpressions: Boolean = false,
    deletesUnknownTypeExpressions: Boolean = false
  ) {

    def withDeletedNodeVariable(deletedNode: Variable): PlanDeletes =
      copy(deletedNodeVariables = deletedNodeVariables + deletedNode)

    def withDeletedRelationshipVariable(deletedRelationship: Variable): PlanDeletes =
      copy(deletedRelationshipVariables = deletedRelationshipVariables + deletedRelationship)
    def withDeletedNodeExpression: PlanDeletes = copy(deletesNodeExpressions = true)
    def withDeletedRelationshipExpression: PlanDeletes = copy(deletesRelationshipExpressions = true)
    def withDeletedUnknownTypeExpression: PlanDeletes = copy(deletesUnknownTypeExpressions = true)

    def isEmpty: Boolean =
      deletedNodeVariables.isEmpty && deletedRelationshipVariables.isEmpty && !deletesNodeExpressions && !deletesUnknownTypeExpressions && !deletesRelationshipExpressions
  }

  /**
   * Writes of a single plan.
   * The Seqs nested in this class may contain duplicates. These are filtered out later in [[ConflictFinder]].
   */
  private[eager] case class PlanWrites(
    sets: PlanSets = PlanSets(),
    creates: PlanCreates = PlanCreates(),
    deletes: PlanDeletes = PlanDeletes()
  )

  /**
   * Collects the writes of a single plan, not traversing into child plans.
   */
  private[eager] def collectWrites(plan: LogicalPlan): PlanWrites = plan match {
    case up: UpdatingPlan => up match {
        case SetNodeProperty(_, _, propertyName, _) =>
          PlanWrites(sets = PlanSets(writtenNodeProperties = Seq(propertyName)))

        case SetRelationshipProperty(_, _, propertyName, _) =>
          PlanWrites(sets = PlanSets(writtenRelProperties = Seq(propertyName)))

        case SetNodeProperties(_, _, assignments) =>
          PlanWrites(sets = PlanSets(writtenNodeProperties = assignments.map(_._1)))

        case SetRelationshipProperties(_, _, assignments) =>
          PlanWrites(sets = PlanSets(writtenRelProperties = assignments.map(_._1)))

        // Set concrete properties, do not delete any other properties
        case SetPropertiesFromMap(_, _, properties: MapExpression, false) =>
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = properties.items.map(_._1),
              writtenRelProperties = properties.items.map(_._1)
            )
          )

        case _: SetPropertiesFromMap =>
          PlanWrites(sets = PlanSets(writesUnknownNodeProperties = true, writesUnknownRelProperties = true))

        case SetProperty(_, _, propertyName, _) =>
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = Seq(propertyName),
              writtenRelProperties = Seq(propertyName)
            )
          )

        case SetProperties(_, _, assignments) =>
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = assignments.map(_._1),
              writtenRelProperties = assignments.map(_._1)
            )
          )

        // Set concrete relationship properties, do not delete any other properties
        case SetRelationshipPropertiesFromMap(_, _, properties: MapExpression, false) =>
          PlanWrites(sets = PlanSets(writtenRelProperties = properties.items.map(_._1)))

        case _: SetRelationshipPropertiesFromMap =>
          PlanWrites(sets = PlanSets(writesUnknownRelProperties = true))

        // Set concrete node properties, do not delete any other properties
        case SetNodePropertiesFromMap(_, _, properties: MapExpression, false) =>
          PlanWrites(sets = PlanSets(writtenNodeProperties = properties.items.map(_._1)))

        // Set unknown properties (i.e. not a MapExpression) or delete any other properties
        case _: SetNodePropertiesFromMap =>
          PlanWrites(sets = PlanSets(writesUnknownNodeProperties = true))

        case SetLabels(_, _, labelNames) =>
          PlanWrites(sets = PlanSets(writtenLabels = labelNames))

        case RemoveLabels(_, _, labelNames) =>
          PlanWrites(sets = PlanSets(writtenLabels = labelNames))

        case c: Create =>
          val nodeCreates = processCreateNodes(PlanCreates(), c.nodes)
          val creates = processCreateRelationships(nodeCreates, c.relationships)
          PlanWrites(creates = creates)

        case Merge(_, nodes, relationships, onMatch, onCreate, _) =>
          val setPart = Function.chain[PlanSets](Seq(
            processSetMutatingPatterns(_, onMatch),
            processSetMutatingPatterns(_, onCreate)
          ))(PlanSets())

          val nodeCreatePart = processCreateNodes(PlanCreates(), nodes)
          val createPart = processCreateRelationships(nodeCreatePart, relationships)

          PlanWrites(setPart, createPart)

        case Foreach(_, _, _, mutations) =>
          val allMutations = mutations.toSet
          val setMutatingPatterns = allMutations.collect {
            case pattern: SetMutatingPattern => pattern
          }
          val deleteMutatingPatterns = allMutations.collect {
            case pattern: DeleteMutatingPattern => pattern
          }
          val restMutatingPatterns = allMutations -- setMutatingPatterns -- deleteMutatingPatterns

          val setPart = processSetMutatingPatterns(PlanSets(), setMutatingPatterns.toSeq)
          val createPart = processCreateMutatingPatterns(PlanCreates(), restMutatingPatterns.toSeq)
          val deletePart = processDeleteMutatingPatterns(PlanDeletes(), deleteMutatingPatterns.toSeq)

          PlanWrites(setPart, createPart, deletePart)

        case DeleteNode(_, v: Variable) =>
          val deletes = PlanDeletes().withDeletedNodeVariable(v)
          PlanWrites(deletes = deletes)

        case DeleteRelationship(_, v: Variable) =>
          val deletes = PlanDeletes().withDeletedRelationshipVariable(v)
          PlanWrites(deletes = deletes)

        case _: DeleteNode =>
          val deletes = PlanDeletes().withDeletedNodeExpression
          PlanWrites(deletes = deletes)

        case _: DeleteRelationship =>
          val deletes = PlanDeletes().withDeletedRelationshipExpression
          PlanWrites(deletes = deletes)

        case _: DeleteExpression =>
          val deletes = PlanDeletes().withDeletedUnknownTypeExpression
          PlanWrites(deletes = deletes)

        case DetachDeleteNode(_, v: Variable) =>
          val deletes = PlanDeletes().withDeletedNodeVariable(v)
            .withDeletedRelationshipExpression
          PlanWrites(deletes = deletes)

        case _: DetachDeleteNode =>
          val deletes = PlanDeletes().withDeletedNodeExpression
            .withDeletedRelationshipExpression
          PlanWrites(deletes = deletes)

        case _: DetachDeleteExpression =>
          val deletes = PlanDeletes().withDeletedUnknownTypeExpression
            .withDeletedRelationshipExpression
          PlanWrites(deletes = deletes)

        case _: DeletePath =>
          val deletes = PlanDeletes().withDeletedUnknownTypeExpression
          PlanWrites(deletes = deletes)

        case _: DetachDeletePath =>
          val deletes = PlanDeletes().withDeletedUnknownTypeExpression
          PlanWrites(deletes = deletes)
      }
    case _ => PlanWrites()
  }

  private def processNodePropertyMap(acc: PlanSets, properties: MapExpression): PlanSets = {
    properties.items.foldLeft[PlanSets](acc) {
      case (acc, (propertyName, _)) => acc.withNodePropertyWritten(propertyName)
    }
  }

  private def processRelationshipPropertyMap(acc: PlanSets, properties: MapExpression): PlanSets = {
    properties.items.foldLeft[PlanSets](acc) {
      case (acc, (propertyName, _)) => acc.withRelPropertyWritten(propertyName)
    }
  }

  private def processCreateNodes(acc: PlanCreates, nodes: Iterable[CreateNode]): PlanCreates = {
    nodes.foldLeft(acc) {
      case (acc, CreateNode(_, labels, maybeProperties)) =>
        maybeProperties match {
          case None => acc.withCreatedNode(CreatedNode(labels, CreatesNoPropertyKeys))
          case Some(MapExpression(properties)) =>
            acc.withCreatedNode(CreatedNode(labels, CreatesKnownPropertyKeys(properties.map(_._1).toSet)))
          case Some(_) => acc.withCreatedNode(CreatedNode(labels, CreatesUnknownPropertyKeys))
        }
    }
  }

  private def processCreateRelationships(acc: PlanCreates, relationships: Iterable[CreateRelationship]): PlanCreates = {
    relationships.foldLeft(acc) {
      case (acc, CreateRelationship(_, _, relType, _, _, maybeProperties)) =>
        maybeProperties match {
          case None => acc.withCreatedRelationship(CreatedRelationship(relType, CreatesNoPropertyKeys))
          case Some(MapExpression(properties)) =>
            acc.withCreatedRelationship(CreatedRelationship(
              relType,
              CreatesKnownPropertyKeys(properties.map(_._1).toSet)
            ))
          case Some(_) => acc.withCreatedRelationship(CreatedRelationship(relType, CreatesUnknownPropertyKeys))
        }
    }
  }

  private def processSetMutatingPatterns(
    acc: PlanSets,
    setMutatingPatterns: Seq[SetMutatingPattern]
  ): PlanSets = {
    setMutatingPatterns.foldLeft[PlanSets](acc) {
      case (acc, SetNodePropertyPattern(_, propertyName, _)) => acc.withNodePropertyWritten(propertyName)

      case (acc, SetRelationshipPropertyPattern(_, propertyName, _)) => acc.withRelPropertyWritten(propertyName)

      case (acc, SetNodePropertiesPattern(_, assignments)) =>
        assignments.foldLeft(acc) {
          case (acc, (propertyName, _)) => acc.withNodePropertyWritten(propertyName)
        }

      case (acc, SetRelationshipPropertiesPattern(_, assignments)) =>
        assignments.foldLeft(acc) {
          case (acc, (propertyName, _)) => acc.withRelPropertyWritten(propertyName)
        }

      // Set concrete properties, do not delete any other properties
      case (acc, SetNodePropertiesFromMapPattern(_, properties: MapExpression, false)) =>
        processNodePropertyMap(acc, properties)

      // Set unknown properties (i.e. not a MapExpression) or delete any other properties
      case (acc, _: SetNodePropertiesFromMapPattern) =>
        acc.withUnknownNodePropertiesWritten

      // Set concrete properties, do not delete any other properties
      case (acc, SetRelationshipPropertiesFromMapPattern(_, properties: MapExpression, false)) =>
        processRelationshipPropertyMap(acc, properties)

      // Set unknown properties (i.e. not a MapExpression) or delete any other properties
      case (acc, _: SetRelationshipPropertiesFromMapPattern) =>
        acc.withUnknownRelPropertiesWritten

      case (acc, _: SetPropertyPattern) =>
        acc.withUnknownRelPropertiesWritten.withUnknownNodePropertiesWritten

      case (acc, _: SetPropertiesPattern) =>
        acc.withUnknownRelPropertiesWritten.withUnknownNodePropertiesWritten

      case (acc, SetPropertiesFromMapPattern(_, properties: MapExpression, false)) =>
        processRelationshipPropertyMap(processNodePropertyMap(acc, properties), properties)

      case (acc, _: SetPropertiesFromMapPattern) =>
        acc.withUnknownRelPropertiesWritten.withUnknownNodePropertiesWritten

      case (acc, SetLabelPattern(_, labels)) =>
        acc.withLabelsWritten(labels.toSet)

      case (acc, RemoveLabelPattern(_, labels)) =>
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
    setMutatingPatterns.foldLeft[PlanCreates](acc) {
      case (acc, c: CreatePattern) =>
        val planNodeCreates = processCreateNodes(acc, c.nodes)
        processCreateRelationships(planNodeCreates, c.relationships)

      case (_, mutatingPattern) =>
        throw new UnsupportedOperationException(
          s"Eagerness support for mutating pattern ${mutatingPattern.productPrefix} not implemented yet."
        )
    }
  }

  private def processDeleteMutatingPatterns(
    acc: PlanDeletes,
    setMutatingPatterns: Seq[SimpleMutatingPattern]
  ): PlanDeletes = {
    setMutatingPatterns.foldLeft[PlanDeletes](acc) {
      case (acc, _: DeleteMutatingPattern) =>
        acc.withDeletedUnknownTypeExpression

      case (_, mutatingPattern) =>
        throw new UnsupportedOperationException(
          s"Eagerness support for mutating pattern ${mutatingPattern.productPrefix} not implemented yet."
        )
    }
  }
}
