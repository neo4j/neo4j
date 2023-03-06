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

import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.compiler.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.FilterExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanThatIntroducesVariable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PossibleDeleteConflictPlans
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.ReadsAndWrites
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.Reason
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.DeleteOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.Expressions
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.logical.plans.TransactionApply
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

    def addConflict(plan1: LogicalPlan, plan2: LogicalPlan, reasons: Set[Reason]): Unit =
      map(Set(Ref(plan1), Ref(plan2))) = map.getOrElse(
        Set(Ref(plan1), Ref(plan2)),
        Set.empty[Reason]
      ) ++ reasons

    // Conflict between a property read and a property write
    for {
      (prop, writePlans) <-
        readsAndWrites.writes.sets.writtenProperties.entries
      readPlan <- readsAndWrites.reads.plansReadingProperty(prop)
      writePlan <- writePlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      addConflict(
        writePlan,
        readPlan,
        Set(prop.map(PropertyReadSetConflict(_, conflict))
          .getOrElse(UnknownPropertyReadSetConflict(conflict)))
      )
    }

    // Conflicts between a label read and a label SET
    for {
      (label, writePlans) <- readsAndWrites.writes.sets.writtenLabels
      readPlan <- readsAndWrites.reads.plansReadingLabel(label)
      writePlan <- writePlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      writePlan match {
        case _: RemoveLabels => addConflict(writePlan, readPlan, Set(LabelReadRemoveConflict(label, conflict)))
        case forEach: Foreach if forEach.mutations.exists(_.isInstanceOf[RemoveLabelPattern]) =>
          addConflict(writePlan, readPlan, Set(LabelReadRemoveConflict(label, conflict)))
        case _ => addConflict(writePlan, readPlan, Set(LabelReadSetConflict(label, conflict)))
      }
    }

    // Conflicts between a label read (determined by a snapshot filterExpressions) and a label CREATE
    for {
      (Ref(writePlan), createdNodes) <- readsAndWrites.writes.creates.createdNodes

      (variable, FilterExpressions(plansThatIntroduceVariable, expression)) <-
        // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
        // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
        readsAndWrites.writes.creates.nodeFilterExpressionsSnapshots(Ref(writePlan)).fuse(
          readsAndWrites.reads.nodeFilterExpressions
        )((x, _) => x)

      // Filter out Create vs Create conflicts
      readPlans = plansThatIntroduceVariable.filter(!_.value.isInstanceOf[UpdatingPlan])
      if readPlans.nonEmpty

      createdNode <- createdNodes

      labelSet = createdNode.createdLabels
      // We need to split the expression in order to filter single predicates.
      // We only want to keep the predicates that depend on only variable, since that is a requirement of CreateOverlaps.overlap
      expressionsDependantOnlyOnVariable =
        Expressions.splitExpression(expression).filter(_.dependencies == Set(variable))

      overlap =
        CreateOverlaps.overlap(expressionsDependantOnlyOnVariable, labelSet.map(_.name), createdNode.createdProperties)
      if (overlap match {
        case CreateOverlaps.NoPropertyOverlap => false
        case CreateOverlaps.NoLabelOverlap    => false
        case _: CreateOverlaps.Overlap        => true
      })

      Ref(readPlan) <- readPlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      // If no labels are read or written this is a ReadCreateConflict, otherwise a LabelReadSetConflict
      val reasons: Set[Reason] = overlap match {
        // Other cases have been filtered out above
        case CreateOverlaps.Overlap(_, propertiesOverlap, labelsOverlap) =>
          val labelReasons: Set[Reason] = labelsOverlap match {
            case NodeLabels.KnownLabels(labelNames) => labelNames
                .map(ln => LabelReadSetConflict(LabelName(ln)(InputPosition.NONE), conflict))
            // SomeUnknownLabels is not possible for a CREATE conflict
          }
          val propertyReasons = propertiesOverlap match {
            case PropertiesOverlap.Overlap(properties) =>
              properties.map(PropertyReadSetConflict(_, conflict))
            case PropertiesOverlap.UnknownOverlap =>
              Set(UnknownPropertyReadSetConflict(conflict))
          }
          val allReasons = labelReasons ++ propertyReasons
          if (allReasons.isEmpty) Set(ReadCreateConflict(conflict))
          else allReasons
      }
      addConflict(writePlan, readPlan, reasons)
    }

    // Conflicts between a MATCH and a DELETE
    sealed trait DeleteConflictType
    case object MatchDeleteConflict extends DeleteConflictType
    case object DeleteMatchConflict extends DeleteConflictType

    /**
     * Add a DELETE conflict
     */
    def deleteConflict(readNodeVariable: LogicalVariable, readPlan: LogicalPlan, writePlan: LogicalPlan): Unit = {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      val reasons: Set[Reason] = Set(ReadDeleteConflict(readNodeVariable.name, conflict))
      addConflict(writePlan, readPlan, reasons)
    }

    /**
     * Check if there is a DELETE overlap
     *
     * @param plansThatIntroduceVariable all plans that introduce the variable, with their predicates if they are leaf plans
     * @param predicatesOnDeletedNode all predicates on the deleted node
     */
    def deleteOverlaps(
      plansThatIntroduceVariable: Seq[PlanThatIntroducesVariable],
      predicatesOnDeletedNode: Seq[Expression]
    ): Boolean = {
      val readNodePredicateCombinations =
        if (plansThatIntroduceVariable.isEmpty) {
          // Variable was not introduced by a leaf plan
          Seq(Seq.empty[Expression])
        } else {
          plansThatIntroduceVariable.map(_.predicates)
        }
      readNodePredicateCombinations.exists(DeleteOverlaps.overlap(_, predicatesOnDeletedNode) match {
        case DeleteOverlaps.NoLabelOverlap => false
        case _: DeleteOverlaps.Overlap     => true
      })
    }

    /**
     * The read variables for DELETE conflicts
     */
    def deleteReadVariables(writePlan: LogicalPlan)
      : Map[LogicalVariable, (PossibleDeleteConflictPlans, DeleteConflictType)] = {
      // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
      // But we have to include other possibleDeleteConflictPlans that are not in the snapshot, to also cover write-read conflicts.
      readsAndWrites.writes.deletes.possibleNodeDeleteConflictPlanSnapshots(Ref(writePlan))
        .view.mapValues[(PossibleDeleteConflictPlans, DeleteConflictType)](x => (x, MatchDeleteConflict)).toMap
        .fuse(readsAndWrites.reads.possibleNodeDeleteConflictPlans.view.mapValues(x => (x, DeleteMatchConflict)).toMap)(
          (x, _) => x
        )
    }

    // Conflicts between a MATCH and a DELETE with a node variable
    for {
      (Ref(writePlan), deletedNodes) <- readsAndWrites.writes.deletes.deletedNodeVariables

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, lastPlansToReferenceVar), conflictType)) <-
        deleteReadVariables(writePlan)

      // Filter out Delete vs Create conflicts
      readPlans = plansThatIntroduceVar.filter(!_.plan.isInstanceOf[UpdatingPlan])
      if readPlans.nonEmpty

      deletedNode <- deletedNodes

      FilterExpressions(_, deletedNodeExpression) =
        readsAndWrites.reads.nodeFilterExpressions.getOrElse(deletedNode, FilterExpressions(Set.empty))
      if deleteOverlaps(readPlans, Seq(deletedNodeExpression))

      // For a MatchDeleteConflict we need to place the Eager between the last plan to reference the variable and the Delete plan.
      // For a DeleteMatchConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case MatchDeleteConflict => lastPlansToReferenceVar
        case DeleteMatchConflict => readPlans.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      deleteConflict(variable, readPlan, writePlan)
    }

    // Conflicts between a MATCH and a DELETE with an expression
    for {
      writePlan <-
        readsAndWrites.writes.deletes.plansThatDeleteNodeExpressions ++ readsAndWrites.writes.deletes.plansThatDeleteUnknownTypeExpressions

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, lastPlansToReferenceVar), conflictType)) <-
        deleteReadVariables(writePlan)
      if deleteOverlaps(plansThatIntroduceVar, Seq.empty)

      // For a MatchDeleteConflict we need to place the Eager between the last plan to reference the variable and the Delete plan.
      // For a DeleteMatchConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case MatchDeleteConflict => lastPlansToReferenceVar
        case DeleteMatchConflict => plansThatIntroduceVar.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      deleteConflict(variable, readPlan, writePlan)
    }

    map.map {
      case (SetExtractor(plan1, plan2), reasons) => ConflictingPlanPair(plan1, plan2, reasons)
    }.toSeq
  }

  private def isValidConflict(readPlan: LogicalPlan, writePlan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    // A plan can never conflict with itself
    def conflictsWithItself = writePlan eq readPlan

    // a merge plan can never conflict with its children
    def mergeConflictWithChild = writePlan.isInstanceOf[Merge] && writePlan.folder.treeExists {
      case plan: LogicalPlan if plan eq readPlan => true
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
      plan.isInstanceOf[DeleteNode] || plan.isInstanceOf[DetachDeleteNode]

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
      case _: TransactionApply => true
      case _                   => false
    }
  }

  private def parentsOfIn(
    innerPlan: LogicalPlan,
    outerPlan: LogicalPlan,
    acc: Seq[LogicalPlan] = Seq.empty
  ): Option[Seq[LogicalPlan]] = {
    outerPlan match {
      case plan: LogicalPlan if (plan eq innerPlan) => Some(acc)
      case _ =>
        def recurse = plan => parentsOfIn(innerPlan, plan, acc :+ outerPlan)
        val maybeLhs = outerPlan.lhs.flatMap(recurse)
        val maybeRhs = outerPlan.rhs.flatMap(recurse)
        maybeLhs.orElse(maybeRhs)
    }
  }
}
