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

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.AccessedLabel
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.AccessedProperty
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.asMaybeVar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.ir.DeleteMutatingPattern
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetDynamicPropertyPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
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
import org.neo4j.cypher.internal.logical.plans.SetDynamicProperty
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

/**
 * Finds all writes for a single plan.
 */
object WriteFinder {

  /**
   * Set write operations of a single plan.
   * Labels written on the same node for sets does not need to differentiated
   * since there can be a situation where another label already exists on the node.
   *
   * @param writtenNodeProperties          all node properties written by this plan
   * @param unknownNodePropertiesAccessors for each unknown Node properties access, e.g. by by doing SET m = n, the accessor variable, if available. 
   * @param writtenLabels                  all labels written by this plan
   * @param writtenRelProperties           all relationship properties written by this plan
   * @param unknownRelPropertiesAccessors  for each unknown relationship properties access, e.g. by by doing SET m = n, the accessor variable, if available.
   * @param unknownLabelAccessors          for each unknown label access, e.g. using dynamic labels, if available.
   */
  private[eager] case class PlanSets(
    writtenNodeProperties: Seq[AccessedProperty] = Seq.empty,
    writtenRelProperties: Seq[AccessedProperty] = Seq.empty,
    unknownNodePropertiesAccessors: Seq[Option[LogicalVariable]] = Seq.empty,
    unknownRelPropertiesAccessors: Seq[Option[LogicalVariable]] = Seq.empty,
    writtenLabels: Set[AccessedLabel] = Set.empty,
    unknownLabelAccessors: Seq[Option[LogicalVariable]] = Seq.empty
  ) {

    def withNodePropertyWritten(accessedProperty: AccessedProperty): PlanSets =
      copy(writtenNodeProperties = writtenNodeProperties :+ accessedProperty)

    def withRelPropertyWritten(accessedProperty: AccessedProperty): PlanSets =
      copy(writtenRelProperties = writtenRelProperties :+ accessedProperty)

    def withUnknownNodePropertiesWritten(accessor: Option[LogicalVariable]): PlanSets =
      copy(unknownNodePropertiesAccessors = unknownNodePropertiesAccessors :+ accessor)

    def withUnknownRelPropertiesWritten(accessor: Option[LogicalVariable]): PlanSets =
      copy(unknownRelPropertiesAccessors = unknownRelPropertiesAccessors :+ accessor)

    /**
     * Call this to signal that this plan writes some labels.
     */
    def withLabelsWritten(labels: Set[AccessedLabel]): PlanSets = copy(writtenLabels = writtenLabels ++ labels)

    def withUnknownLabelsWritten(accessor: Option[LogicalVariable]): PlanSets =
      copy(unknownLabelAccessors = unknownLabelAccessors :+ accessor)
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
        case SetNodeProperty(_, variable, propertyName, _) =>
          PlanWrites(sets = PlanSets(writtenNodeProperties = Seq(AccessedProperty(propertyName, Some(variable)))))

        case SetRelationshipProperty(_, variable, propertyName, _) =>
          PlanWrites(sets = PlanSets(writtenRelProperties = Seq(AccessedProperty(propertyName, Some(variable)))))

        case SetNodeProperties(_, variable, assignments) =>
          PlanWrites(sets =
            PlanSets(writtenNodeProperties = assignments.map(_._1).map(AccessedProperty(_, Some(variable))))
          )

        case SetRelationshipProperties(_, variable, assignments) =>
          PlanWrites(sets =
            PlanSets(writtenRelProperties = assignments.map(_._1).map(AccessedProperty(_, Some(variable))))
          )

        // Set concrete properties, do not delete any other properties
        case SetPropertiesFromMap(_, entity, properties: MapExpression, false) =>
          val props = properties.items.map(_._1).map(AccessedProperty(_, asMaybeVar(entity)))
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = props,
              writtenRelProperties = props
            )
          )

        case SetPropertiesFromMap(_, entity, _, _) =>
          PlanWrites(sets =
            PlanSets(
              unknownNodePropertiesAccessors = Seq(asMaybeVar(entity)),
              unknownRelPropertiesAccessors = Seq(asMaybeVar(entity))
            )
          )

        case SetProperty(_, entity, propertyName, _) =>
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = Seq(AccessedProperty(propertyName, asMaybeVar(entity))),
              writtenRelProperties = Seq(AccessedProperty(propertyName, asMaybeVar(entity)))
            )
          )

        case SetDynamicProperty(_, entity, _, _) =>
          PlanWrites(sets =
            PlanSets(
              unknownNodePropertiesAccessors = Seq(asMaybeVar(entity)),
              unknownRelPropertiesAccessors = Seq(asMaybeVar(entity))
            )
          )

        case SetProperties(_, entity, assignments) =>
          val props = assignments.map(_._1).map(AccessedProperty(_, asMaybeVar(entity)))
          PlanWrites(sets =
            PlanSets(
              writtenNodeProperties = props,
              writtenRelProperties = props
            )
          )

        // Set concrete relationship properties, do not delete any other properties
        case SetRelationshipPropertiesFromMap(_, variable, properties: MapExpression, false) =>
          PlanWrites(sets =
            PlanSets(writtenRelProperties = properties.items.map(_._1).map(AccessedProperty(_, Some(variable))))
          )

        case SetRelationshipPropertiesFromMap(_, variable, _, _) =>
          PlanWrites(sets = PlanSets(unknownRelPropertiesAccessors = Seq(Some(variable))))

        // Set concrete node properties, do not delete any other properties
        case SetNodePropertiesFromMap(_, variable, properties: MapExpression, false) =>
          PlanWrites(sets =
            PlanSets(writtenNodeProperties = properties.items.map(_._1).map(AccessedProperty(_, Some(variable))))
          )

        // Set unknown properties (i.e. not a MapExpression) or delete any other properties
        case SetNodePropertiesFromMap(_, variable, _, _) =>
          PlanWrites(sets = PlanSets(unknownNodePropertiesAccessors = Seq(Some(variable))))

        case SetLabels(_, variable, labelNames, dynamicLabels) =>
          processWrittenLabel(variable, labelNames, dynamicLabels)

        case RemoveLabels(_, variable, labelNames, dynamicLabels) =>
          processWrittenLabel(variable, labelNames, dynamicLabels)

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

  private def processWrittenLabel(
    variable: LogicalVariable,
    labelNames: Set[LabelName],
    dynamicLabels: Set[Expression]
  ): PlanWrites = {
    val writtenLabels = labelNames.map(AccessedLabel(_, Some(variable)))
    if (dynamicLabels.nonEmpty) {
      PlanWrites(sets =
        PlanSets(
          writtenLabels = writtenLabels,
          unknownLabelAccessors = Seq(Some(variable))
        )
      )
    } else {
      PlanWrites(sets =
        PlanSets(
          writtenLabels = writtenLabels
        )
      )
    }
  }

  private def processNodePropertyMap(
    acc: PlanSets,
    accessor: Option[LogicalVariable],
    properties: MapExpression
  ): PlanSets = {
    properties.items.foldLeft[PlanSets](acc) {
      case (acc, (propertyName, _)) => acc.withNodePropertyWritten(AccessedProperty(propertyName, accessor))
    }
  }

  private def processRelationshipPropertyMap(
    acc: PlanSets,
    accessor: Option[LogicalVariable],
    properties: MapExpression
  ): PlanSets = {
    properties.items.foldLeft[PlanSets](acc) {
      case (acc, (propertyName, _)) => acc.withRelPropertyWritten(AccessedProperty(propertyName, accessor))
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
      case (acc, SetNodePropertyPattern(variable, propertyName, _)) =>
        acc.withNodePropertyWritten(AccessedProperty(propertyName, Some(variable)))

      case (acc, SetRelationshipPropertyPattern(variable, propertyName, _)) =>
        acc.withRelPropertyWritten(AccessedProperty(propertyName, Some(variable)))

      case (acc, SetNodePropertiesPattern(variable, assignments)) =>
        assignments.foldLeft(acc) {
          case (acc, (propertyName, _)) => acc.withNodePropertyWritten(AccessedProperty(propertyName, Some(variable)))
        }

      case (acc, SetRelationshipPropertiesPattern(variable, assignments)) =>
        assignments.foldLeft(acc) {
          case (acc, (propertyName, _)) => acc.withRelPropertyWritten(AccessedProperty(propertyName, Some(variable)))
        }

      // Set concrete properties, do not delete any other properties
      case (acc, SetNodePropertiesFromMapPattern(variable, properties: MapExpression, false)) =>
        processNodePropertyMap(acc, Some(variable), properties)

      // Set unknown properties (i.e. not a MapExpression) or delete any other properties
      case (acc, SetNodePropertiesFromMapPattern(variable, _, _)) =>
        acc.withUnknownNodePropertiesWritten(Some(variable))

      // Set concrete properties, do not delete any other properties
      case (acc, SetRelationshipPropertiesFromMapPattern(variable, properties: MapExpression, false)) =>
        processRelationshipPropertyMap(acc, Some(variable), properties)

      // Set unknown properties (i.e. not a MapExpression) or delete any other properties
      case (acc, SetRelationshipPropertiesFromMapPattern(variable, _, _)) =>
        acc.withUnknownRelPropertiesWritten(Some(variable))

      case (acc, SetPropertyPattern(entity, _, _)) =>
        acc.withUnknownRelPropertiesWritten(asMaybeVar(entity))
          .withUnknownNodePropertiesWritten(asMaybeVar(entity))

      case (acc, SetDynamicPropertyPattern(entity, _, _)) =>
        acc.withUnknownRelPropertiesWritten(asMaybeVar(entity))
          .withUnknownNodePropertiesWritten(asMaybeVar(entity))

      case (acc, SetPropertiesPattern(entity, _)) =>
        acc.withUnknownRelPropertiesWritten(asMaybeVar(entity))
          .withUnknownNodePropertiesWritten(asMaybeVar(entity))

      case (acc, SetPropertiesFromMapPattern(entity, properties: MapExpression, false)) =>
        processRelationshipPropertyMap(
          processNodePropertyMap(acc, asMaybeVar(entity), properties),
          asMaybeVar(entity),
          properties
        )

      case (acc, SetPropertiesFromMapPattern(entity, _, _)) =>
        acc.withUnknownRelPropertiesWritten(asMaybeVar(entity))
          .withUnknownNodePropertiesWritten(asMaybeVar(entity))

      // TODO dynamic labels
      case (acc, SetLabelPattern(variable, labels, _)) =>
        acc.withLabelsWritten(labels.map(AccessedLabel(_, Some(variable))).toSet)

      // TODO dynamic labels
      case (acc, RemoveLabelPattern(variable, labels, _)) =>
        acc.withLabelsWritten(labels.map(AccessedLabel(_, Some(variable))).toSet)
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
