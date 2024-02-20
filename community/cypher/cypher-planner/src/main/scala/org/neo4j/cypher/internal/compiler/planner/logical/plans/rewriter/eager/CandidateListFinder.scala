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
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.hasChild
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.hasChildMatching
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

import scala.annotation.tailrec

/**
 * Given conflicts between plans, computes candidate lists that describe where Eager can be planned to solve the conflicts.
 */
object CandidateListFinder {

  /**
   * @param candidates Eager may be planned on top of any of these plans.
   *                   The candidates are in an order such that `candidates(n).children.contains(candidates(n+1))`.
   * @param conflict   the original conflict
   */
  private[eager] case class CandidateList(candidates: List[Ref[LogicalPlan]], conflict: ConflictingPlanPair)

  /**
   * Represents a growing (therefore open) list of candidate plans on which an eager could be planned to solve the given conflict.
   *
   * @param candidates candidates for an eager operator found so far.
   *                   The candidates are in an order such that `candidates(n).children.contains(candidates(n+1))`.
   * @param layer      layer of the conflicting plan that was already traversed.
   * @param conflict   the conflict that should be solved by planning an eager on either of the candidate plans
   */
  private case class OpenSequence(candidates: List[Ref[LogicalPlan]], layer: Int, conflict: ConflictingPlanPair) {

    /**
     * @return a [[CandidateList]] with the current candidates and the reason of the conflict.
     */
    def candidateListWithConflict: CandidateList =
      CandidateList(candidates = candidates, conflict = conflict)

    def withAddedCandidate(candidate: Ref[LogicalPlan]): OpenSequence =
      copy(candidates = candidate :: candidates)
  }

  /**
   * @param eagerizeLHSvsRHSConflicts               if `true` a conflict between the LHS and the RHS needs an Eager on the LHS
   * @param emptyCandidateListsForRHSvsTopConflicts if `true` a conflict between the RHS and the Top can only be solved by
   *                                                an Eager on Top
   */
  private case class BinaryPlanEagerizationStrategy(
    eagerizeLHSvsRHSConflicts: Boolean,
    emptyCandidateListsForRHSvsTopConflicts: Boolean
  )

  private object BinaryPlanEagerizationStrategy {

    private def assertNoLhsVsRHSConflicts(plan: LogicalPlan, hasLhsVsRhsConflicts: Boolean): Boolean = {
      if (hasLhsVsRhsConflicts) {
        throw new IllegalStateException(
          s"We do not expect conflicts between the two branches of a ${plan.getClass.getSimpleName} yet."
        )
      }
      false
    }

    def forPlan(plan: LogicalBinaryPlan, hasLhsVsRhsConflicts: Boolean): BinaryPlanEagerizationStrategy = plan match {
      case _: EagerLogicalPlan => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = false,
          emptyCandidateListsForRHSvsTopConflicts = false
        )
      case _: Union => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = false,
          emptyCandidateListsForRHSvsTopConflicts = false
        )
      case _: OrderedUnion => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = assertNoLhsVsRHSConflicts(plan, hasLhsVsRhsConflicts),
          emptyCandidateListsForRHSvsTopConflicts = false
        )
      case _: AssertSameNode => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = assertNoLhsVsRHSConflicts(plan, hasLhsVsRhsConflicts),
          emptyCandidateListsForRHSvsTopConflicts = true
        )
      case _: AssertSameRelationship => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = assertNoLhsVsRHSConflicts(plan, hasLhsVsRhsConflicts),
          emptyCandidateListsForRHSvsTopConflicts = true
        )
      case _: CartesianProduct => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = true,
          emptyCandidateListsForRHSvsTopConflicts = true
        )
      case _: RepeatOptions => BinaryPlanEagerizationStrategy(
          eagerizeLHSvsRHSConflicts = assertNoLhsVsRHSConflicts(plan, hasLhsVsRhsConflicts),
          emptyCandidateListsForRHSvsTopConflicts = true
        )
      case p: ApplyPlan =>
        throw new IllegalStateException(s"combineWithRhs is not supposed to be called with ApplyPlans. Got: $p")
    }
  }

  private object SequencesAcc {

    def removeConflict(
      openConflicts: Map[Ref[LogicalPlan], Set[ConflictingPlanPair]],
      toRemove: ConflictingPlanPair
    ): Map[Ref[LogicalPlan], Set[ConflictingPlanPair]] = {
      def updateMapping(conflicts: Option[Set[ConflictingPlanPair]]): Option[Set[ConflictingPlanPair]] =
        conflicts match {
          case Some(conflicts) =>
            val updatedConflicts = conflicts - toRemove
            if (updatedConflicts.isEmpty) None
            else Some(updatedConflicts)
          case None => None
        }
      openConflicts
        .updatedWith(toRemove.first)(updateMapping)
        .updatedWith(toRemove.second)(updateMapping)
    }
  }

  /**
   * @param openConflicts  all conflicts where no conflicting plan of the pair was traversed yet.
   *                       The key is one of the two logical plans of a conflict and the value is all conflicts where that plan appears.
   * @param openSequences  A map of open sequences where one of two conflicting plans was already traversed.
   *                       The key is the end of the open sequence, so that we can quickly lookup all open sequences that end with a given plan.
   *                       The value are the open sequences, i.e. candidate lists that are not complete yet
   * @param candidateLists Both conflicting plans haven already been traversed and these are final candidate lists.
   * @param currentLayer   used to keep track how nested a plan is. This value is equal to the number of ApplyPlans that the current plan is nested under.
   */
  private case class SequencesAcc(
    openConflicts: Map[Ref[LogicalPlan], Set[ConflictingPlanPair]],
    openSequences: Map[Ref[LogicalPlan], Seq[OpenSequence]] = Map.empty,
    candidateLists: Seq[CandidateList] = Seq.empty,
    currentLayer: Int = -1
  ) {

    def withAddedOpenSequence(
      start: Ref[LogicalPlan],
      end: Ref[LogicalPlan],
      conflict: ConflictingPlanPair,
      layer: Int
    ): SequencesAcc = {
      val oS = openSequences.getOrElse(end, Seq.empty)
      copy(openSequences =
        openSequences.updated(end, oS :+ OpenSequence(List(start), layer, conflict))
      )
    }

    def pushLayer(): SequencesAcc = copy(currentLayer = currentLayer + 1)

    def popLayer(): SequencesAcc = {
      val newLayer = currentLayer - 1
      // All open sequences of this layer are moved to the upper layer and lose all candidates so far.
      // This is because a conflict between a plan on the RHS and on top of an Apply must be solved with an Eager on top of the Apply.
      val newOpenSequences = openSequences.view.mapValues {
        _.map {
          case OpenSequence(_, layer, conflict) if layer == currentLayer =>
            OpenSequence(List.empty, newLayer, conflict)
          case x => x
        }
      }.toMap
      copy(
        currentLayer = newLayer,
        openSequences = newOpenSequences,
        candidateLists = candidateLists
      )
    }

    /**
     * Combine a SequencesAcc from the LHS of a binary plan with a SequencesAcc from the RHS.
     *
     * @param rhs  the RHS SequencesAcc
     * @param plan the binary plan. Not an ApplyPlan.
     * @return the combined SequencesAcc.
     */
    def combineWithRhs(rhs: SequencesAcc, plan: LogicalBinaryPlan): SequencesAcc = {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(currentLayer == rhs.currentLayer)
      // For better readability
      val lhs = this

      def conflictsWithRHS(conflictingPlanPair: ConflictingPlanPair): Boolean = {
        hasChildMatching(plan.right, p => conflictingPlanPair.first == Ref(p) || conflictingPlanPair.second == Ref(p))
      }

      val lhsConflicts = lhs.openSequences.values.flatten.map(_.conflict)
      val rhsConflicts = rhs.openSequences.values.flatten.map(_.conflict)
      // Conflicts where one conflicting plan is on the LHS and the other on the RHS
      val solvedLHSvsRHSConflicts = (lhsConflicts.toSet intersect rhsConflicts.toSet)
        .filter {
          case ConflictingPlanPair(first, second, _) =>
            (hasChild(plan.left, first.value) && hasChild(plan.right, second.value)) ||
            (hasChild(plan.left, second.value) && hasChild(plan.right, first.value))
        }

      def isSolvedConflictingPlanPair(cpp: ConflictingPlanPair) = {
        solvedLHSvsRHSConflicts.contains(cpp) ||
          lhs.candidateLists.exists(_.conflict == cpp) ||
          rhs.candidateLists.exists(_.conflict == cpp)
      }

      def isSolved(os: OpenSequence) = {
        isSolvedConflictingPlanPair(os.conflict)
      }

      val eagerizationStrategy = BinaryPlanEagerizationStrategy.forPlan(plan, solvedLHSvsRHSConflicts.nonEmpty)

      // Compute new open sequences
      val filteredRhsOpenSequences =
        if (eagerizationStrategy.emptyCandidateListsForRHSvsTopConflicts)
          rhs.openSequences.view.mapValues(_.map {
            case o @ OpenSequence(_, layer, conflict) =>
              if (conflictsWithRHS(conflict)) {
                OpenSequence(List.empty, layer, conflict)
              } else {
                o
              }
          }).toMap
        else rhs.openSequences

      // We keep only those open sequences where one of the conflicting plans has not been traversed yet and remove the ones that have already been solved.
      val newOpenSequences = lhs.openSequences.fuse(filteredRhsOpenSequences)(_ ++ _).map {
        case (endPlan, openSequences) =>
          (endPlan, openSequences.filter(!isSolved(_)).distinct)
      }
        .filter(_._2.nonEmpty)

      val lhsVsRhsConflictCandidateLists =
        if (eagerizationStrategy.eagerizeLHSvsRHSConflicts) {
          // Take the candidate list from the LHS for a conflict between LHS and RHS
          lhs.openSequences.values.flatten
            .filter(os => solvedLHSvsRHSConflicts.contains(os.conflict))
            .map(_.candidateListWithConflict)
        } else {
          Seq.empty
        }

      // If one of the plans of a conflict has been found and an open sequence was created, we can disregard the original conflict coming from the other side
      val lhsRemainingOpenConflicts = rhsConflicts.foldLeft(lhs.openConflicts) {
        case (lhsConflictsMap, rhsConflict) => SequencesAcc.removeConflict(lhsConflictsMap, rhsConflict)
      }
      val rhsRemainingOpenConflicts = lhsConflicts.foldLeft(rhs.openConflicts) {
        case (rhsConflictsMap, lhsConflict) => SequencesAcc.removeConflict(rhsConflictsMap, lhsConflict)
      }
      val remainingOpenConflicts = lhsRemainingOpenConflicts.fuse(rhsRemainingOpenConflicts)(_ ++ _)
        .view
        .mapValues(_.filterNot(isSolvedConflictingPlanPair))
        .filter {
          case (_, conflicts) => conflicts.nonEmpty
        }
        .toMap

      copy(
        openConflicts = remainingOpenConflicts,
        openSequences = newOpenSequences,
        candidateLists = lhs.candidateLists ++ rhs.candidateLists ++ lhsVsRhsConflictCandidateLists
      )
    }
  }

  private def pushLayerForLeafPlans(acc: SequencesAcc, p: LogicalPlan): SequencesAcc = {
    p match {
      case _: LogicalLeafPlan => acc.pushLayer()
      case _ => acc
    }
  }

  private def checkConstraints(p: LogicalPlan): Unit = {
    p match {
      // All Semi Apply variants must have read-only RHSs
      // If this changes in the future, the correct way to eagerize a SemiApply variant
      // that conflicts with its RHS is like this:
      // .selectOrSemiApply("var1")
      // .|.setLabel("n:A")
      // .|. ...
      // .eager()
      // .projection("a:A AS var1")
      // . ...
      case s: SingleFromRightLogicalPlan if !s.right.readOnly =>
        throw new IllegalStateException(
          "Eagerness analysis does not support if the RHS of a SingleFromRightLogicalPlan contains writes"
        )

      case _ =>
    }
  }

  private def updateSequences(acc: SequencesAcc, p: Ref[LogicalPlan]): SequencesAcc = {
    // Find conflicts that contain this plan
    val SequencesAcc(remainingConflicts, newOpenSequences, _, _) =
      acc.openConflicts.get(p) match {
        case Some(conflicts) =>
          // Start with an empty accumulator, except that all conflicts are kept, except the ones that will now
          // get converted to open sequences.
          val innerAcc = SequencesAcc(acc.openConflicts.removed(p))
          conflicts.foldLeft(innerAcc) {
            case (innerAcc, conflict) =>
              val withOpenSequence = if (conflict.first == p) {
                // Add an open sequence that ends at the other plan and initially contains p as a candidate
                innerAcc.withAddedOpenSequence(p, conflict.second, conflict, acc.currentLayer)
              } else {
                // Add an open sequence that ends at the other plan and initially contains p as a candidate
                innerAcc.withAddedOpenSequence(p, conflict.first, conflict, acc.currentLayer)
              }

              // Make sure to also remove the conflict from the "other" side.
              // It has already been removed from the Set with `p` as the key.
              val updatedInnerConflicts = SequencesAcc.removeConflict(innerAcc.openConflicts, conflict)
              withOpenSequence.copy(openConflicts = updatedInnerConflicts)
          }
        case None =>
          SequencesAcc(acc.openConflicts)
      }

    // Find open sequences that are closed by this plan
    val newCandidateLists = acc.openSequences.getOrElse(p, Seq.empty).map(_.candidateListWithConflict)

    val remainingOpenSequences = {
      // All sequences that do not end in p
      (acc.openSequences - p)
        .view.mapValues(_.map {
        case os@OpenSequence(_, layer, _) if acc.currentLayer == layer =>
          // Add p to all remaining open sequences on the same layer
          os.withAddedCandidate(p)
        case os => os
      }).toMap
    }

    acc.copy(
      openConflicts = remainingConflicts,
      openSequences = remainingOpenSequences.fuse(newOpenSequences)(_ ++ _),
      candidateLists = acc.candidateLists ++ newCandidateLists
    )
  }

  private def processPlan(acc: SequencesAcc, plan: LogicalPlan): SequencesAcc = {
    checkConstraints(plan)
    updateSequences(pushLayerForLeafPlans(acc, plan), Ref(plan))
  }

  /**
   * For each conflict between two plans, find a candidate list of plans. Planning Eager on top of any plan
   * in the candidate list will solve the respective conflict.
   *
   * This is done by traversing the plan and accumulating the plans between the two conflicting plans as candidates.
   */
  private[eager] def findCandidateLists(plan: LogicalPlan, conflicts: Seq[ConflictingPlanPair]): Seq[CandidateList] = {
    val conflictsMapBuilder = scala.collection.mutable.MultiDict.empty[Ref[LogicalPlan], ConflictingPlanPair]
    conflicts.foreach {
      case c @ ConflictingPlanPair(first, second, _) =>
        conflictsMapBuilder.addOne(first -> c).addOne(second -> c)
    }
    val conflictsMap = conflictsMapBuilder.sets.view.mapValues(_.toSet).toMap

    val sequencesAcc = LogicalPlans.foldPlan(SequencesAcc(conflictsMap))(
      plan,
      (acc, p) =>
        processPlan(acc, p),
      (lhsAcc, rhsAcc, p) =>
        p match {
          case _: ApplyPlan =>
            // Pop a layer and use the RHS acc which was initialized with the LHS acc
            processPlan(rhsAcc.popLayer(), p)
          case b: LogicalBinaryPlan =>
            // as a non-apply binary plan, we need to combine the information from both legs
            processPlan(
              lhsAcc.combineWithRhs(rhsAcc, b),
              b
            )
        }
    )

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openSequences.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openConflicts.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.currentLayer == 0)
    val candidateLists = sequencesAcc.candidateLists

    // Remove CandidateLists that traverse an EagerLogicalPlan coming from the LHS.
    // These do not need to be eagerized.
    candidateLists.filterNot(cl => traversesEagerPlanFromLeft(cl.candidates))
  }

  @tailrec
  private def traversesEagerPlanFromLeft(candidates: List[Ref[LogicalPlan]]): Boolean = candidates match {
    case Ref(first: EagerLogicalPlan) :: Ref(second) :: _ if first.lhs.contains(second) => true
    case _ :: tail => traversesEagerPlanFromLeft(tail)
    case Nil       => false
  }
}
