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

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.ConflictingPlanPair
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.PlanChildrenLookup
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.FilterExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanThatIntroducesVariable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanWithAccessor
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
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.DeleteOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.Expressions
import org.neo4j.cypher.internal.label_expressions.LabelExpressionLeafName
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Finds conflicts between plans that need Eager to solve them.
 */
sealed trait ConflictFinder {

  private def propertyConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan,
    writtenProperties: ReadsAndWritesFinder.Sets => Seq[(Option[PropertyKeyName], Seq[PlanWithAccessor])],
    plansReadingProperty: (ReadsAndWritesFinder.Reads, Option[PropertyKeyName]) => Seq[PlanWithAccessor]
  )(implicit planChildrenLookup: PlanChildrenLookup): Seq[ConflictingPlanPair] = {
    for {
      (prop, writePlans) <- writtenProperties(readsAndWrites.writes.sets)
      read @ PlanWithAccessor(readPlan, _) <- plansReadingProperty(readsAndWrites.reads, prop)
      write @ PlanWithAccessor(writePlan, _) <- writePlans

      conflictType = mostDownstreamPlan(wholePlan, Set(readPlan, writePlan)).get match {
        case `readPlan` => WriteReadConflict
        case _          => ReadWriteConflict
      }

      // Potentially discard distinct Read -> Write conflicts (keep non ReadWriteConflicts)
      if conflictType != ReadWriteConflict ||
        !distinctConflictOnSameSymbol(read, write, wholePlan) ||
        // For property conflicts, we cannot disregard all conflicts with distinct reads on the same variable.
        // We have to keep conflicts between property reads from leaf plans and writes in TransactionalApply.
        // That is because changing the property of a node `n` and committing these changes might put `n`
        // at a different position in the index that is being traversed by the read, so that `n` could potentially
        // be encountered again.
        (isInTransactionalApply(writePlan, wholePlan) && readPlan.isInstanceOf[LogicalLeafPlan])

      // Discard distinct Write -> Read conflicts (keep non WriteReadConflict)
      if conflictType != WriteReadConflict ||
        // invoke distinctConflictOnSameSymbol with swapped read and write to check that the write is unique.
        !distinctConflictOnSameSymbol(write, read, wholePlan)

      if isValidConflict(readPlan, writePlan, wholePlan, leftMostLeaf)
    } yield {
      val conflict = Conflict(writePlan.id, readPlan.id)
      val reasons = Set[EagernessReason](prop.map(PropertyReadSetConflict(_).withConflict(conflict))
        .getOrElse(UnknownPropertyReadSetConflict.withConflict(conflict)))

      ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
    }
  }

  private def labelConflicts(readsAndWrites: ReadsAndWrites, wholePlan: LogicalPlan, leftMostLeaf: LogicalPlan)(implicit
  planChildrenLookup: PlanChildrenLookup): Iterable[ConflictingPlanPair] = {
    for {
      (label, writePlans) <- readsAndWrites.writes.sets.writtenLabels
      read @ PlanWithAccessor(readPlan, _) <- readsAndWrites.reads.plansReadingLabel(label)
      write @ PlanWithAccessor(writePlan, _) <- writePlans
      if !distinctConflictOnSameSymbol(read, write, wholePlan)
      if isValidConflict(readPlan, writePlan, wholePlan, leftMostLeaf)
    } yield {
      val conflict = Conflict(writePlan.id, readPlan.id)
      writePlan match {
        case _: RemoveLabels =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadRemoveConflict(label).withConflict(conflict)))
        case forEach: Foreach if forEach.mutations.exists(_.isInstanceOf[RemoveLabelPattern]) =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadRemoveConflict(label).withConflict(conflict)))
        case _ =>
          ConflictingPlanPair(Ref(writePlan), Ref(readPlan), Set(LabelReadSetConflict(label).withConflict(conflict)))
      }
    }
  }

  private def canConflictWithCreateOrDelete(lp: LogicalPlan): Boolean = {
    !lp.isUpdatingPlan || containsNestedPlanExpression(lp)
  }

  protected[eager] def containsNestedPlanExpression(lp: LogicalPlan): Boolean = {
    lp.folder.treeFold(false) {
      case _: NestedPlanExpression => _ => SkipChildren(true)
      // We do not want to find NestedPlanExpressions in child plans.
      case otherLP: LogicalPlan if otherLP ne lp => acc => SkipChildren(acc)
    }
  }

  private def createConflicts[T <: LabelExpressionLeafName, C <: WriteFinder.CreatedEntity[T]](
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan,
    createdEntities: ReadsAndWritesFinder.Creates => Map[Ref[LogicalPlan], Set[C]],
    filterExpressionSnapshots: (
      ReadsAndWritesFinder.Creates,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, FilterExpressions],
    filterExpressions: ReadsAndWritesFinder.Reads => Map[LogicalVariable, FilterExpressions],
    createEntityReason: (String, Conflict) => EagernessReason
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterable[ConflictingPlanPair] = {
    for {
      (Ref(writePlan), createdEntities) <- createdEntities(readsAndWrites.writes.creates)

      (variable, FilterExpressions(plansThatIntroduceVariable, expression)) <-
        // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
        // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
        filterExpressionSnapshots(readsAndWrites.writes.creates, Ref(writePlan)).fuse(
          filterExpressions(readsAndWrites.reads)
        )((x, _) => x)

      // Filter out Create vs Create conflicts
      readPlans = plansThatIntroduceVariable.filter(ref => canConflictWithCreateOrDelete(ref.value))
      if readPlans.nonEmpty

      // We need to split the expression in order to filter single predicates.
      // We only want to keep the predicates that depend on only variable, since that is a requirement of CreateOverlaps.overlap
      expressionsDependantOnlyOnVariable =
        Expressions.splitExpression(expression).filter(_.dependencies == Set(variable))

      createdEntity <- createdEntities

      entitySet = createdEntity.getCreatedLabelsOrTypes

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
      if isValidConflict(readPlan, writePlan, wholePlan, leftMostLeaf)
    } yield {
      val conflict = Conflict(writePlan.id, readPlan.id)
      // If no labels are read or written this is a ReadCreateConflict, otherwise a LabelReadSetConflict
      val reasons: Set[EagernessReason] = overlap match {
        case CreateOverlaps.Overlap(_, propertiesOverlap, entityOverlap) =>
          val entityReasons: Set[EagernessReason] = entityOverlap match {
            case NodeLabels.KnownLabels(entityNames) => entityNames
                .map(ln => createEntityReason(ln, conflict))
            case NodeLabels.SomeUnknownLabels =>
              throw new IllegalStateException("SomeUnknownLabels is not possible for a CREATE conflict")
          }
          val propertyReasons = propertiesOverlap match {
            case PropertiesOverlap.Overlap(properties) =>
              properties.map(PropertyReadSetConflict(_).withConflict(conflict))
            case PropertiesOverlap.UnknownOverlap =>
              Set(UnknownPropertyReadSetConflict.withConflict(conflict))
          }
          val allReasons = entityReasons ++ propertyReasons
          if (allReasons.isEmpty) Set(ReadCreateConflict.withConflict(conflict))
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
   * 
   *  @return A map. For each variable, the possible read plans that can conflict with a given delete (`writePlan`).
   *          Those are split into plansThatIntroduceVariable and plansThatReferenceVariable 
   *          (see [[PossibleDeleteConflictPlans]]).
   *          
   *          Also return the conflict type for each variable.
   */
  private def deleteReadVariables(
    readsAndWrites: ReadsAndWrites,
    writePlan: LogicalPlan,
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  ): Map[LogicalVariable, (PossibleDeleteConflictPlans, ConflictType)] = {
    // All plans that access the variable.
    val all = possibleDeleteConflictPlans(readsAndWrites.reads)
      .view.mapValues(x => (x, WriteReadConflict)).toMap
    // In the snapshot, we will find only plans that access the variable that are upstream from the write plan.
    val snapshot: Map[LogicalVariable, (PossibleDeleteConflictPlans, ConflictType)] =
      possibleDeleteConflictPlanSnapshots(readsAndWrites.writes.deletes, Ref(writePlan))
        .view.mapValues[(PossibleDeleteConflictPlans, ConflictType)](x => (x, ReadWriteConflict)).toMap

    // If a variable only exists in `all` it is a WriteReadConflict, which we return unchanged here.
    // A variable cannot _only_ exist in `snapshot`.
    snapshot.fuse(all) {
      // If a variable exists in `all` and `snapshot` it is a ReadWriteConflict. We return all plans.
      case ((_, conflictType), (allPlans, _)) => (allPlans, conflictType)
    }
  }

  private def deleteVariableConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan,
    deletedEntities: ReadsAndWritesFinder.Deletes => Map[Ref[LogicalPlan], Set[Variable]],
    filterExpressions: ReadsAndWritesFinder.Reads => Map[LogicalVariable, FilterExpressions],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterable[ConflictingPlanPair] = {
    for {
      (Ref(writePlan), deletedEntities) <- deletedEntities(readsAndWrites.writes.deletes)

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(readsAndWrites, writePlan, possibleDeleteConflictPlans, possibleDeleteConflictPlanSnapshots)

      // Filter out Delete vs Create conflicts
      readPlans = plansThatIntroduceVar.filter(ptiv => canConflictWithCreateOrDelete(ptiv.plan))
      if readPlans.nonEmpty

      deletedEntity <- deletedEntities

      FilterExpressions(_, deletedExpression) =
        filterExpressions(readsAndWrites.reads).getOrElse(deletedEntity, FilterExpressions(Set.empty))
      if deleteOverlaps(readPlans, Seq(deletedExpression))

      // For a ReadWriteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a WriteReadConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case ReadWriteConflict => plansThatReferenceVariable
        case WriteReadConflict => readPlans.map(_.plan)
      }
      // For delete, we can only disregard ReadWriteConflicts. We therefore have to keep all WriteReadConflicts.
      if conflictType == WriteReadConflict || !distinctConflictOnSameSymbol(
        PlanWithAccessor(readPlan, Some(variable)),
        PlanWithAccessor(writePlan, Some(deletedEntity)),
        wholePlan
      )
      if isValidConflict(readPlan, writePlan, wholePlan, leftMostLeaf)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  private def deleteExpressionConflicts(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan,
    deleteExpressions: ReadsAndWritesFinder.Deletes => Seq[LogicalPlan],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterable[ConflictingPlanPair] = {
    for {
      writePlan <-
        deleteExpressions(
          readsAndWrites.writes.deletes
        ) ++ readsAndWrites.writes.deletes.plansThatDeleteUnknownTypeExpressions

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(readsAndWrites, writePlan, possibleDeleteConflictPlans, possibleDeleteConflictPlanSnapshots)

      // For a ReadWriteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a WriteReadConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      readPlan <- conflictType match {
        case ReadWriteConflict => plansThatReferenceVariable
        case WriteReadConflict => plansThatIntroduceVar.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, wholePlan, leftMostLeaf)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  private def callInTxConflict(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan
  ): Iterable[ConflictingPlanPair] = {
    if (readsAndWrites.reads.callInTxPlans.nonEmpty) {
      for {
        updatingPlan <- wholePlan.folder.findAllByClass[UpdatingPlan].filterNot(isInTransactionalApply(_, wholePlan))
        txPlan <- readsAndWrites.reads.callInTxPlans
      } yield {
        ConflictingPlanPair(Ref(txPlan), Ref(updatingPlan), Set(EagernessReason.WriteAfterCallInTransactions))
      }
    } else Iterable.empty
  }

  // Conflicts between a Read and a Write
  sealed private trait ConflictType
  private case object ReadWriteConflict extends ConflictType
  private case object WriteReadConflict extends ConflictType

  /**
   * Add a DELETE conflict
   */
  private def deleteConflict(
    readVariable: LogicalVariable,
    readPlan: LogicalPlan,
    writePlan: LogicalPlan
  ): ConflictingPlanPair = {
    val conflict = Conflict(writePlan.id, readPlan.id)
    val reasons: Set[EagernessReason] = Set(ReadDeleteConflict(readVariable.name).withConflict(conflict))
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
  )(implicit planChildrenLookup: PlanChildrenLookup): Seq[ConflictingPlanPair] = {
    val leftMostLeaf = wholePlan.leftmostLeaf
    val map = mutable.Map[Set[Ref[LogicalPlan]], Set[EagernessReason]]()

    def addConflict(conflictingPlanPair: ConflictingPlanPair): Unit = {
      map(Set(conflictingPlanPair.first, conflictingPlanPair.second)) = map.getOrElse(
        Set(conflictingPlanPair.first, conflictingPlanPair.second),
        Set.empty[EagernessReason]
      ) ++ conflictingPlanPair.reasons
    }

    // Conflict between a Node property read and a property write
    propertyConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.writtenNodeProperties.nodeEntries,
      _.plansReadingNodeProperty(_)
    ).foreach(addConflict)

    // Conflict between a Relationship property read and a property write
    propertyConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.writtenRelProperties.relEntries,
      _.plansReadingRelProperty(_)
    ).foreach(addConflict)

    // Conflicts between a label read and a label SET
    labelConflicts(readsAndWrites, wholePlan, leftMostLeaf).foreach(addConflict)

    // Conflicts between a label read (determined by a snapshot filterExpressions) and a label CREATE
    createConflicts[LabelName, CreatedNode](
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.createdNodes,
      _.nodeFilterExpressionsSnapshots(_),
      _.nodeFilterExpressions,
      (ln, conflict) => LabelReadSetConflict(LabelName(ln)(InputPosition.NONE)).withConflict(conflict)
    ).foreach(addConflict)

    // Conflicts between a type read (determined by a snapshot filterExpressions) and a type CREATE
    createConflicts[RelTypeName, CreatedRelationship](
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.createdRelationships,
      _.relationshipFilterExpressionsSnapshots(_),
      _.relationshipFilterExpressions,
      (tn, conflict) => TypeReadSetConflict(RelTypeName(tn)(InputPosition.NONE)).withConflict(conflict)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node variable
    deleteVariableConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.deletedNodeVariables,
      _.nodeFilterExpressions,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship variable
    deleteVariableConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.deletedRelationshipVariables,
      _.relationshipFilterExpressions,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node expression
    deleteExpressionConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.plansThatDeleteNodeExpressions,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship expression
    deleteExpressionConflicts(
      readsAndWrites,
      wholePlan,
      leftMostLeaf,
      _.plansThatDeleteRelationshipExpressions,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a Updating Plan and a Call in Transaction.
    // This will find conflicts between updating clauses before a CALL IN TX as well, but that is already disallowed by semantic analysis.
    callInTxConflict(readsAndWrites, wholePlan).foreach(addConflict)

    map.map {
      case (SetExtractor(plan1, plan2), reasons) => ConflictingPlanPair(plan1, plan2, reasons)
      case (set, _) => throw new IllegalStateException(s"Set must have 2 elements. Got: $set")
    }.toSeq
  }

  /**
   * Some conflicts can be disregarded if they access the same symbol through the same variable,
   * and if that variable is distinct when reading.
   */
  private def distinctConflictOnSameSymbol(
    read: PlanWithAccessor,
    write: PlanWithAccessor,
    wholePlan: LogicalPlan
  ): Boolean = {
    read.accessor match {
      case Some(variable) =>
        write.accessor.contains(variable) && isGloballyUniqueAndCursorInitialized(
          variable,
          read.plan,
          wholePlan,
          write.plan
        )
      case None => false
    }
  }

  private def isValidConflict(
    readPlan: LogicalPlan,
    writePlan: LogicalPlan,
    wholePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    // A plan can never conflict with itself
    def conflictsWithItself = writePlan eq readPlan

    // a merge plan can never conflict with its children
    def mergeConflictWithChild = writePlan.isInstanceOf[Merge] && planChildrenLookup.hasChild(writePlan, readPlan)

    // a ForeachApply can conflict with its RHS children.
    // For now, we ignore those conflicts, to ensure side-effect visibility.
    def foreachConflictWithRHS = Seq(writePlan, readPlan).permutations.exists {
      case Seq(planA, planB) => planA match {
          case ForeachApply(_, rhs, _, _) =>
            (rhs eq planB) || planChildrenLookup.hasChild(rhs, planB)
          case _ => false
        }
      case _ => throw new IllegalStateException()
    }

    // We consider the leftmost plan to be potentially stable unless we are in a call in transactions.
    def conflictsWithUnstablePlan =
      (readPlan ne leftMostLeaf) ||
        !readPlan.isInstanceOf[StableLeafPlan] ||
        isInTransactionalApply(writePlan, wholePlan)

    /**
     * Deleting plans can conflict, if they evaluate expressions. Otherwise not.
     */
    def simpleDeletingPlansConflict =
      simpleDeletingPlan(writePlan) && simpleDeletingPlan(readPlan)

    def nonConflictingReadPlan(): Boolean = readPlan.isInstanceOf[Argument]

    !simpleDeletingPlansConflict &&
    !conflictsWithItself &&
    !mergeConflictWithChild &&
    !foreachConflictWithRHS &&
    conflictsWithUnstablePlan &&
    !nonConflictingReadPlan()
  }

  private def isInTransactionalApply(plan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    val parents = pathFromRoot(plan, wholePlan).get
    parents.exists {
      case _: TransactionApply   => true
      case _: TransactionForeach => true
      case _                     => false
    }
  }

  private def isDistinctLeafPlan(readPlan: LogicalPlan): Boolean = readPlan match {
    case _: NodeLogicalLeafPlan           => true
    case rlp: RelationshipLogicalLeafPlan => rlp.directed
    case _                                => false
  }

  private def isSimpleDeleteAndDistinctLeaf(readPlan: LogicalPlan, writePlan: LogicalPlan): Boolean =
    simpleDeletingPlan(writePlan) && isDistinctLeafPlan(readPlan)

  private def isGloballyUniqueAndCursorInitialized(
    variable: LogicalVariable,
    readPlan: LogicalPlan,
    wholePlan: LogicalPlan,
    writePlan: LogicalPlan
  ): Boolean = {
    // plan.distinctness is "per argument"
    // In order to make sure a column is "globally unique", i.e. over multiple invocations,
    // we need to make sure the operator does only execute once.
    // Also, we must make sure that the cursor performing the read will be initialized at the point in
    // time the write for the same variable happens.
    (isSimpleDeleteAndDistinctLeaf(readPlan, writePlan) || readPlan.distinctness.covers(
      Seq(variable)
    )) && !readMightNotBeInitialized(readPlan, wholePlan)
  }

  /**
   * Tests whether then plan is nested on the RHS of a binary plan that might not have initialized the rhs
   * before yielding a row.
   */
  private[eager] def readMightNotBeInitialized(plan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    def rhsMightNotBeInitializedBeforeYieldingFirstRow(b: LogicalBinaryPlan): Boolean = b match {
      case _: ApplyPlan              => true
      case _: CartesianProduct       => true
      case _: AssertSameNode         => true
      case _: AssertSameRelationship => true
      case _: RepeatOptions          => true
      case _: LeftOuterHashJoin      => false
      case _: NodeHashJoin           => false
      case _: RightOuterHashJoin     => false
      case _: ValueHashJoin          => false
      case _: Union                  => true
      case _: OrderedUnion           => false
    }

    @tailrec
    def recurse(plans: List[LogicalPlan]): Boolean = plans match {
      case head :: (p: LogicalBinaryPlan) :: _
        if (p.right eq head) && rhsMightNotBeInitializedBeforeYieldingFirstRow(p) => true
      case _ :: tail => recurse(tail)
      case Seq()     => false
    }

    val parents = pathFromRoot(plan, wholePlan).get
    recurse(parents.reverse.toList)
  }

  /**
   * In `wholePlan`, finds the plan among `plans` that is most downstream.
   */
  private[eager] def mostDownstreamPlan(wholePlan: LogicalPlan, plans: Set[LogicalPlan]): Option[LogicalPlan] = {
    if (plans.contains(wholePlan)) {
      return Some(wholePlan)
    }

    wholePlan match {
      case p: LogicalBinaryPlan =>
        mostDownstreamPlan(p.right, plans).orElse(mostDownstreamPlan(p.left, plans))
      case p: LogicalUnaryPlan =>
        mostDownstreamPlan(p.source, plans)
      case _ =>
        None
    }
  }

  /**
   * Traverses the logical plan tree of `outerPlan` to find the path to `innerPlan`.
   * Returns the path from the root (`outerPlan`) to `innerPlan`, including both plans.
   */
  private[eager] def pathFromRoot(
    innerPlan: LogicalPlan,
    outerPlan: LogicalPlan,
    acc: Seq[LogicalPlan] = Seq.empty
  ): Option[Seq[LogicalPlan]] = {
    outerPlan match {
      case plan: LogicalPlan if plan eq innerPlan => Some(acc :+ innerPlan)
      case _ =>
        def recurse = plan => pathFromRoot(innerPlan, plan, acc :+ outerPlan)
        val maybeLhs = outerPlan.lhs.flatMap(recurse)
        maybeLhs.orElse(outerPlan.rhs.flatMap(recurse))
    }
  }

  /**
   * Tests whether a plan is a simple deleting plan that does not evaluate any expressions,
   * apart from the variable that it is supposed to delete. This variable read can never conflict
   * with another delete, even when the entity has already been deleted.
   * This is because deletes are idempotent.
   */
  private def simpleDeletingPlan(plan: LogicalPlan): Boolean = plan match {
    case DeleteNode(_, _: LogicalVariable)         => true
    case DetachDeleteNode(_, _: LogicalVariable)   => true
    case DeleteRelationship(_, _: LogicalVariable) => true
    case _                                         => false
  }
}

object ConflictFinder extends ConflictFinder {

  /**
   * Two plans that have a read/write conflict. The plans are in no particular order.
   *
   * @param first   one of the two plans.
   * @param second  the other plan.
   * @param reasons the reasons of the conflict.
   */
  private[eager] case class ConflictingPlanPair(
    first: Ref[LogicalPlan],
    second: Ref[LogicalPlan],
    reasons: Set[EagernessReason]
  )

  def withCaching(): ConflictFinder = {
    new ConflictFinderWithCaching()
  }

  private class ConflictFinderWithCaching() extends ConflictFinder {

    private val containsNestedPlanExpressionCache: Ref[LogicalPlan] => Boolean = {
      CachedFunction {
        lpRef => super.containsNestedPlanExpression(lpRef.value)
      }
    }

    override protected[eager] def containsNestedPlanExpression(lp: LogicalPlan): Boolean = {
      containsNestedPlanExpressionCache(Ref(lp))
    }
  }
}
