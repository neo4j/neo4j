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
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.ReadsAndWrites
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.logical.plans.TransactionApply
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
    reasons: Set[EagernessReason.Reason]
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
    val map = mutable.Map[Set[Ref[LogicalPlan]], Set[EagernessReason.Reason]]()

    def addConflict(plan1: LogicalPlan, plan2: LogicalPlan, reasons: Set[EagernessReason.Reason]): Unit =
      map(Set(Ref(plan1), Ref(plan2))) = map.getOrElse(
        Set(Ref(plan1), Ref(plan2)),
        Set.empty[EagernessReason.Reason]
      ) ++ reasons

    // Conflict between a property read and a property write
    for {
      (prop, writePlans) <-
        readsAndWrites.writes.sets.writtenProperties.entries ++ readsAndWrites.writes.creates.writtenProperties.entries
      readPlan <- readsAndWrites.reads.plansReadingProperty(prop)
      writePlan <- writePlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      addConflict(
        writePlan,
        readPlan,
        Set(prop.map(EagernessReason.PropertyReadSetConflict(_, conflict))
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
      addConflict(writePlan, readPlan, Set(EagernessReason.LabelReadSetConflict(label, conflict)))
    }

    // Conflicts between a label read (determined by a snapshot filterExpressions) and a label CREATE
    for {
      (writePlan, labelCombinations) <- readsAndWrites.writes.creates.writtenLabels
      (variable, FilterExpressions(readPlans, expression)) <-
        // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
        // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
        readsAndWrites.writes.creates.filterExpressionsSnapshots(writePlan).fuse(
          readsAndWrites.reads.filterExpressions
        )((x, _) => x)
      labelSet <- labelCombinations
      if (CreateOverlaps.overlap(Seq(expression), labelSet.map(_.name), CreatesNoPropertyKeys) match {
        case CreateOverlaps.NoPropertyOverlap => false
        case CreateOverlaps.NoLabelOverlap    => false
        case _: CreateOverlaps.Overlap        => true
      })
      readPlan <- readPlans
      if isValidConflict(readPlan, writePlan, wholePlan)
    } {
      val conflict = Some(Conflict(writePlan.id, readPlan.id))
      val emptyFilterExpressions = FilterExpressions(Set.empty)
      // If no labels are read or written this is a ReadCreateConflict, otherwise a LabelReadSetConflict
      val reasons: Set[EagernessReason.Reason] =
        if (labelSet.isEmpty || expression == emptyFilterExpressions.expression) Set(ReadCreateConflict(conflict))
        else labelSet.map(EagernessReason.LabelReadSetConflict(_, conflict))
      addConflict(writePlan, readPlan, reasons)
    }

    map.map {
      case (SetExtractor(plan1, plan2), reasons) => ConflictingPlanPair(plan1, plan2, reasons)
    }.toSeq
  }

  private def isValidConflict(readPlan: LogicalPlan, writePlan: LogicalPlan, wholePlan: LogicalPlan): Boolean = {
    // A plan can never conflict with itself
    writePlan != readPlan &&
    // We consider the leftmost plan to be potentially stable unless we are in a call in transactions.
    (readPlan != wholePlan.leftmostLeaf ||
      !readPlan.isInstanceOf[StableLeafPlan] ||
      isInTransactionalApply(
        writePlan,
        wholePlan
      ))
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
      case `innerPlan` => Some(acc)
      case _ =>
        def recurse = plan => parentsOfIn(innerPlan, plan, acc :+ outerPlan)
        val maybeLhs = outerPlan.lhs.flatMap(recurse)
        val maybeRhs = outerPlan.rhs.flatMap(recurse)
        maybeLhs.orElse(maybeRhs)
    }
  }
}
