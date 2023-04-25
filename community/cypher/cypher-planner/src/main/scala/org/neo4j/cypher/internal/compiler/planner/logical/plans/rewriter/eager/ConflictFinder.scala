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

import org.neo4j.cypher.internal.compiler.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.FilterExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanThatIntroducesVariable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PossibleDeleteConflictPlans
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.ReadsAndWrites
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.CreatedNode
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.CreatedRelationship
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.Reason
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.DeleteOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.Expressions
import org.neo4j.cypher.internal.label_expressions.LabelExpressionLeafName
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref

import scala.collection.mutable

/**
 * Finds conflicts between plans that need Eager to solve them.
 */
object ConflictFinder {

  /**
   * Two plans that have a read/write conflict. The plans are in no particular order.
   *
   * @param first  one of the two plans.
   * @param second the other plan.
   * @param reasons the reasons of the conflict.
   */
  private[eager] case class ConflictingPlanPair(
    first: Ref[LogicalPlan],
    second: Ref[LogicalPlan],
    reasons: Set[Reason]
  )

  private def propertyConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    writtenProperties: ReadsAndWritesFinder.Sets => Seq[(Option[PropertyKeyName], Seq[LogicalPlan])],
    plansReadingProperty: (ReadsAndWritesFinder.Reads, Option[PropertyKeyName]) => Seq[LogicalPlan]
  ): Seq[ConflictingPlanPair] = {
    for {
      (prop, writePlans) <- writtenProperties(readsAndWrites.writes.sets)
      readPlan <- plansReadingProperty(readsAndWrites.reads, prop)
      writePlan <- writePlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } yield {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      val reasons = Set[Reason](prop.map(PropertyReadSetConflict(_, conflict))
        .getOrElse(UnknownPropertyReadSetConflict(conflict)))

      ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
    }
  }

  private def labelConflicts(readsAndWrites: ReadsAndWrites, wholePlan: LogicalPlan): Iterable[ConflictingPlanPair] = {
    for {
      (label, writePlans) <- readsAndWrites.writes.sets.writtenLabels
      readPlan <- readsAndWrites.reads.plansReadingLabel(label)
      writePlan <- writePlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } yield {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      writePlan match {
        case _: RemoveLabels =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadRemoveConflict(label, conflict)))
        case forEach: Foreach if forEach.mutations.exists(_.isInstanceOf[RemoveLabelPattern]) =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadRemoveConflict(label, conflict)))
        case _ =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadSetConflict(label, conflict)))
      }
    }
  }

  private def createConflicts[T <: LabelExpressionLeafName, C <: WriteFinder.CreatedEntity[T]](
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    createdEntities: ReadsAndWritesFinder.Creates => Map[Ref[LogicalPlan], Set[C]],
    filterExpressionSnapshots: (
      ReadsAndWritesFinder.Creates,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, FilterExpressions],
    filterExpressions: ReadsAndWritesFinder.Reads => Map[LogicalVariable, FilterExpressions],
    createEntityReason: (String, Option[Conflict]) => Reason
  ): Iterable[ConflictingPlanPair] = {
    for {
      (Ref(writePlan), createdEntities) <- createdEntities(readsAndWrites.writes.creates)

      (variable, FilterExpressions(plansThatIntroduceVariable, expression)) <-
        // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
        // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
        filterExpressionSnapshots(readsAndWrites.writes.creates, Ref(writePlan)).fuse(
          filterExpressions(readsAndWrites.reads)
        )((x, _) => x)

      // Filter out Create vs Create conflicts
      readPlans = plansThatIntroduceVariable.filter(!_.value.isInstanceOf[UpdatingPlan])
      if readPlans.nonEmpty

      createdEntity <- createdEntities

      entitySet = createdEntity.getCreatedLabelsOrTypes
      // We need to split the expression in order to filter single predicates.
      // We only want to keep the predicates that depend on only variable, since that is a requirement of CreateOverlaps.overlap
      expressionsDependantOnlyOnVariable =
        Expressions.splitExpression(expression).filter(_.dependencies == Set(variable))

      overlap =
        CreateOverlaps.overlap(
          expressionsDependantOnlyOnVariable,
          entitySet.map(_.name),
          createdEntity.getCreatedProperties
        )
      if (overlap match {
        case CreateOverlaps.NoPropertyOverlap => false
        case CreateOverlaps.NoLabelOverlap    => false
        case _: CreateOverlaps.Overlap        => true
      })

      Ref(readPlan) <- readPlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } yield {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      // If no labels are read or written this is a ReadCreateConflict, otherwise a LabelReadSetConflict
      val reasons: Set[Reason] = overlap match {
        case CreateOverlaps.Overlap(_, propertiesOverlap, entityOverlap) =>
          val entityReasons: Set[Reason] = entityOverlap match {
            case NodeLabels.KnownLabels(entityNames) => entityNames
                .map(ln => createEntityReason(ln, conflict))
            case NodeLabels.SomeUnknownLabels =>
              throw new IllegalStateException("SomeUnknownLabels is not possible for a CREATE conflict")
          }
          val propertyReasons = propertiesOverlap match {
            case PropertiesOverlap.Overlap(properties) =>
              properties.map(PropertyReadSetConflict(_, conflict))
            case PropertiesOverlap.UnknownOverlap =>
              Set(UnknownPropertyReadSetConflict(conflict))
          }
          val allReasons = entityReasons ++ propertyReasons
          if (allReasons.isEmpty) Set(ReadCreateConflict(conflict))
          else allReasons

        // Other cases have been filtered out above
        case x @ (CreateOverlaps.NoLabelOverlap | CreateOverlaps.NoPropertyOverlap) =>
          throw new IllegalStateException(s"Only Overlap expected at this point, but got: $x")
      }
      ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
    }
  }

  /**
   * The read variables for variable DELETE conflicts
   */
  private def deleteReadVariables(
    readsAndWrites: ReadsAndWrites,
    writePlan: LogicalPlan,
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  ): Map[LogicalVariable, (PossibleDeleteConflictPlans, DeleteConflictType)] = {
    // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
    // But we have to include other possibleDeleteConflictPlans that are not in the snapshot, to also cover write-read conflicts.
    possibleDeleteConflictPlanSnapshots(readsAndWrites.writes.deletes, Ref(writePlan))
      .view.mapValues[(PossibleDeleteConflictPlans, DeleteConflictType)](x => (x, MatchDeleteConflict)).toMap
      .fuse(possibleDeleteConflictPlans(readsAndWrites.reads).view.mapValues(x => (x, DeleteMatchConflict)).toMap)(
        (x, _) => x
      )
  }

  private def deleteVariableConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    deletedEntities: ReadsAndWritesFinder.Deletes => Map[Ref[LogicalPlan], Set[Variable]],
    filterExpressions: ReadsAndWritesFinder.Reads => Map[LogicalVariable, FilterExpressions],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  ): Iterable[ConflictingPlanPair] = {
    for {
      (Ref(writePlan), deletedEntities) <- deletedEntities(readsAndWrites.writes.deletes)

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(readsAndWrites, writePlan, possibleDeleteConflictPlans, possibleDeleteConflictPlanSnapshots)

      // Filter out Delete vs Create conflicts
      readPlans = plansThatIntroduceVar.filter(!_.plan.isInstanceOf[UpdatingPlan])
      if readPlans.nonEmpty

      deletedEntity <- deletedEntities

      FilterExpressions(_, deletedExpression) =
        filterExpressions(readsAndWrites.reads).getOrElse(deletedEntity, FilterExpressions(Set.empty))
      if deleteOverlaps(readPlans, Seq(deletedExpression))

      // For a MatchDeleteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a DeleteMatchConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case MatchDeleteConflict => plansThatReferenceVariable
        case DeleteMatchConflict => readPlans.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, wholePlan)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  private def deleteExpressionConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    deleteExpressions: ReadsAndWritesFinder.Deletes => Seq[LogicalPlan],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  ): Iterable[ConflictingPlanPair] = {
    for {
      writePlan <-
        deleteExpressions(
          readsAndWrites.writes.deletes
        ) ++ readsAndWrites.writes.deletes.plansThatDeleteUnknownTypeExpressions

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(readsAndWrites, writePlan, possibleDeleteConflictPlans, possibleDeleteConflictPlanSnapshots)
      if deleteOverlaps(plansThatIntroduceVar, Seq.empty)

      // For a MatchDeleteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a DeleteMatchConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case MatchDeleteConflict => plansThatReferenceVariable
        case DeleteMatchConflict => plansThatIntroduceVar.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, wholePlan)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  // Conflicts between a MATCH and a DELETE
  sealed private trait DeleteConflictType
  private case object MatchDeleteConflict extends DeleteConflictType
  private case object DeleteMatchConflict extends DeleteConflictType

  /**
   * Add a DELETE conflict
   */
  private def deleteConflict(
    readVariable: LogicalVariable,
    readPlan: LogicalPlan,
    writePlan: LogicalPlan
  ): ConflictingPlanPair = {
    val conflict = Some(Conflict(writePlan.id, readPlan.id))
    val reasons: Set[Reason] = Set(ReadDeleteConflict(readVariable.name, conflict))
    ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
  }

  /**
   * Check if there is a DELETE overlap
   *
   * @param plansThatIntroduceVariable all plans that introduce the variable, with their predicates if they are leaf plans
   * @param predicatesOnDeletedEntity all predicates on the deleted node
   */
  private def deleteOverlaps(
    plansThatIntroduceVariable: Seq[PlanThatIntroducesVariable],
    predicatesOnDeletedEntity: Seq[Expression]
  ): Boolean = {
    val readEntityPredicateCombinations =
      if (plansThatIntroduceVariable.isEmpty) {
        // Variable was not introduced by a leaf plan
        Seq(Seq.empty[Expression])
      } else {
        plansThatIntroduceVariable.map(_.predicates)
      }
    readEntityPredicateCombinations.exists(DeleteOverlaps.overlap(_, predicatesOnDeletedEntity) match {
      case DeleteOverlaps.NoLabelOverlap => false
      case _: DeleteOverlaps.Overlap     => true
    })
  }

  /**
   * By inspecting the reads and writes, return a [[ConflictingPlanPair]] for each Read/write conflict.
   * In the result there is only one ConflictingPlanPair per pair of plans, and the reasons are merged
   * if plans conflicts because of several reasons.
   */
  private[eager] def findConflictingPlans(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan
  ): Seq[ConflictingPlanPair] = {
    val map = mutable.Map[Set[Ref[LogicalPlan]], Set[Reason]]()

    def addConflict(conflictingPlanPair: ConflictingPlanPair): Unit = {
      map(Set(conflictingPlanPair.first, conflictingPlanPair.second)) = map.getOrElse(
        Set(conflictingPlanPair.first, conflictingPlanPair.second),
        Set.empty[Reason]
      ) ++ conflictingPlanPair.reasons
    }

    // Conflict between a Node property read and a property write
    propertyConflicts(
      readsAndWrites,
      wholePlan,
      _.writtenNodeProperties.nodeEntries,
      _.plansReadingNodeProperty(_)
    ).foreach(addConflict)

    // Conflict between a Relationship property read and a property write
    propertyConflicts(
      readsAndWrites,
      wholePlan,
      _.writtenRelProperties.relEntries,
      _.plansReadingRelProperty(_)
    ).foreach(addConflict)

    // Conflicts between a label read and a label SET
    labelConflicts(readsAndWrites, wholePlan).foreach(addConflict)

    // Conflicts between a label read (determined by a snapshot filterExpressions) and a label CREATE
    createConflicts[LabelName, CreatedNode](
      readsAndWrites,
      wholePlan,
      _.createdNodes,
      _.nodeFilterExpressionsSnapshots(_),
      _.nodeFilterExpressions,
      (ln, conflict) => LabelReadSetConflict(LabelName(ln)(InputPosition.NONE), conflict)
    ).foreach(addConflict)

    // Conflicts between a type read (determined by a snapshot filterExpressions) and a type CREATE
    createConflicts[RelTypeName, CreatedRelationship](
      readsAndWrites,
      wholePlan,
      _.createdRelationships,
      _.relationshipFilterExpressionsSnapshots(_),
      _.relationshipFilterExpressions,
      (tn, conflict) => TypeReadSetConflict(RelTypeName(tn)(InputPosition.NONE), conflict)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node variable
    deleteVariableConflicts(
      readsAndWrites,
      wholePlan,
      _.deletedNodeVariables,
      _.nodeFilterExpressions,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship variable
    deleteVariableConflicts(
      readsAndWrites,
      wholePlan,
      _.deletedRelationshipVariables,
      _.relationshipFilterExpressions,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node expression
    deleteExpressionConflicts(
      readsAndWrites,
      wholePlan,
      _.plansThatDeleteNodeExpressions,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship expression
    deleteExpressionConflicts(
      readsAndWrites,
      wholePlan,
      _.plansThatDeleteRelationshipExpressions,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    map.map {
      case (SetExtractor(plan1, plan2), reasons) => ConflictingPlanPair(plan1, plan2, reasons)
      case (set, _) => throw new IllegalStateException(s"Set must have 2 elements. Got: $set")
    }.toSeq
  }

  private def isValidConflict(readPlan: LogicalPlan, writePlan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    // A plan can never conflict with itself
    def conflictsWithItself = writePlan eq readPlan

    // a merge plan can never conflict with its children
    def mergeConflictWithChild = writePlan.isInstanceOf[Merge] && hasChildRec(writePlan, readPlan)

    def hasChildRec(plan: LogicalPlan, child: LogicalPlan): Boolean = {
      (plan eq child) || plan.lhs.exists(hasChildRec(_, child)) || plan.rhs.exists(hasChildRec(_, child))
    }

    // We consider the leftmost plan to be potentially stable unless we are in a call in transactions.
    def conflictsWithUnstablePlan =
      (readPlan ne wholePlan.leftmostLeaf) ||
        !readPlan.isInstanceOf[StableLeafPlan] ||
        isInTransactionalApply(
          writePlan,
          wholePlan
        )

    def deletingPlan(plan: LogicalPlan) =
      plan.isInstanceOf[DeleteNode] || plan.isInstanceOf[DetachDeleteNode] ||
        plan.isInstanceOf[DeleteRelationship]

    def deletingPlansConflict =
      deletingPlan(writePlan) && deletingPlan(readPlan)

    !deletingPlansConflict &&
    !conflictsWithItself &&
    !mergeConflictWithChild &&
    conflictsWithUnstablePlan
  }

  private def isInTransactionalApply(plan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    val parents = parentsOfIn(plan, wholePlan).get
    parents.exists {
      case _: TransactionApply   => true
      case _: TransactionForeach => true
      case _                     => false
    }
  }

  private def parentsOfIn(
    innerPlan: LogicalPlan,
    outerPlan: LogicalPlan,
    acc: Seq[LogicalPlan] = Seq.empty
  ): Option[Seq[LogicalPlan]] = {
    outerPlan match {
      case plan: LogicalPlan if plan eq innerPlan => Some(acc)
      case _ =>
        def recurse = plan => parentsOfIn(innerPlan, plan, acc :+ outerPlan)
        val maybeLhs = outerPlan.lhs.flatMap(recurse)
        val maybeRhs = outerPlan.rhs.flatMap(recurse)
        maybeLhs.orElse(maybeRhs)
    }
  }
}
