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
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.ConflictingPlanPair
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.macros.AssertMacros

/**
 * Given conflicts between plans, computes candidate lists that describe where Eager can be planned to solve the conflicts.
 */
object CandidateListFinder {

  /**
   * @param candidates Eager may be planned on top of any of these plans.
   *                   The candidates are in an order such that `candidates(n+1).children.contains(candidates(n))`.
   * @param reasons    the reasons of the original conflict(s)
   */
  private[eager] case class CandidateList(candidates: Seq[LogicalPlan], reasons: Set[EagernessReason.Reason])

  /**
   * Represents a growing (therefore open) list of candidate plans on which an eager could be planned to solve the given conflict.
   *
   * @param candidates candidates for an eager operator found so far.
   *                   The candidates are in an order such that `candidates(n+1).children.contains(candidates(n))`.
   * @param layer      layer of the conflicting plan that was already traversed.
   * @param conflict   the conflict that should be solved by planning an eager on either of the candidate plans
   */
  private case class OpenSequence(candidates: Seq[LogicalPlan], layer: Int, conflict: ConflictingPlanPair) {

    /**
     * @return a [[CandidateList]] with the current candidates and the reason of the conflict.
     */
    def candidateListWithReason: CandidateList =
      CandidateList(candidates = candidates, reasons = conflict.reasons)

    def withAddedCandidate(candidate: LogicalPlan): OpenSequence =
      copy(candidates = candidates :+ candidate)
  }

  /**
   * @param openConflicts  all conflicts where no conflicting plan of the pair was traversed yet.
   * @param openSequences  A map of open sequences where one of two conflicting plans was already traversed.
   *                       The key is the end of the open sequence, so that we can quickly lookup all open sequences that end with a given plan.
   *                       The value are the open sequences, i.e. candidate lists that are not complete yet
   * @param candidateLists Both conflicting plans haven already been traversed and these are final candidate lists.
   * @param currentLayer   used to keep track how nested a plan is. This value is equal to the number of ApplyPlans that the current plan is nested under.
   */
  private case class SequencesAcc(
    openConflicts: Seq[ConflictingPlanPair],
    openSequences: Map[LogicalPlan, Seq[OpenSequence]] = Map.empty,
    candidateLists: Seq[CandidateList] = Seq.empty,
    currentLayer: Int = -1
  ) {

    def withAddedConflict(conflict: ConflictingPlanPair): SequencesAcc = copy(openConflicts = openConflicts :+ conflict)

    def withAddedOpenSequence(
      start: LogicalPlan,
      end: LogicalPlan,
      conflict: ConflictingPlanPair,
      layer: Int
    ): SequencesAcc = {
      val oS = openSequences.getOrElse(end, Seq.empty)
      copy(openSequences =
        openSequences.updated(end, oS :+ OpenSequence(Seq(start), layer, conflict))
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
            OpenSequence(Seq.empty, newLayer, conflict)
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

      val lhsConflicts = lhs.openSequences.values.flatten.map(_.conflict)
      val rhsConflicts = rhs.openSequences.values.flatten.map(_.conflict)
      // Conflicts where one conflicting plan is on the LHS and the other on the RHS
      val solvedConflicts = lhsConflicts.toSet intersect rhsConflicts.toSet

      /**
       * Whether the two plans in the conflict have been found in LHS and RHS respectively
       */
      def isSolved(os: OpenSequence) = {
        solvedConflicts.contains(os.conflict)
      }

      /**
       * @param eagerizeLHSvsRHSConflicts               if `true` a conflict between the LHS and the RHS needs an Eager on the LHS
       * @param emptyCandidateListsForRHSvsTopConflicts if `true` a conflict between the RHS and the Top can only be solved by
       *                                                an Eager on Top
       */
      case class BinaryPlanEagerizationStrategy(
        eagerizeLHSvsRHSConflicts: Boolean,
        emptyCandidateListsForRHSvsTopConflicts: Boolean
      )

      val eagerizationStrategy = plan match {
        case _: EagerLogicalPlan => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = false,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: Union => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = false,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: OrderedUnion if solvedConflicts.nonEmpty =>
          // do not expect conflicts between lhs and rhs
          throw new IllegalStateException(
            "We do not expect conflicts between the two branches of an OrderedUnion yet."
          )
        case _: OrderedUnion => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = false,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: AssertSameNode if solvedConflicts.nonEmpty =>
          // do not expect conflicts between lhs and rhs
          throw new IllegalStateException(
            "We do not expect conflicts between the two branches of an AssertSameNode yet."
          )
        case _: AssertSameNode => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = false,
            emptyCandidateListsForRHSvsTopConflicts = true
          )
        case _: CartesianProduct => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = true,
            emptyCandidateListsForRHSvsTopConflicts = true
          )
      }

      // Compute new open sequences
      val filteredRhsOpenSequences =
        if (eagerizationStrategy.emptyCandidateListsForRHSvsTopConflicts)
          rhs.openSequences.view.mapValues(_.map(_.copy(candidates = Seq.empty))).toMap
        else rhs.openSequences

      // We keep only those open sequences where one of the conflicting plans has not been traversed yet.
      val newOpenSequences = lhs.openSequences.fuse(filteredRhsOpenSequences)(_ ++ _).map {
        case (endPlan, openSequences) =>
          (endPlan, openSequences.filter(!isSolved(_)))
      }.filter(_._2.nonEmpty)

      val lhsVsRhsConflictCandidateLists =
        if (eagerizationStrategy.eagerizeLHSvsRHSConflicts) {
          // Take the candidate list from the LHS for a conflict between LHS and RHS
          lhs.openSequences.values.flatten.filter(isSolved).map(_.candidateListWithReason)
        } else {
          Seq.empty
        }

      // If one of the plans of a conflict has been found and an open sequence was created, we can disregard the original conflict coming from the other side
      val remainingOpenConflicts =
        lhs.openConflicts.filterNot(conflict => rhs.openSequences.values.flatten.exists(_.conflict == conflict)) ++
          rhs.openConflicts.filterNot(conflict => lhs.openSequences.values.flatten.exists(_.conflict == conflict))

      copy(
        openConflicts = remainingOpenConflicts,
        openSequences = newOpenSequences,
        candidateLists = lhs.candidateLists ++ rhs.candidateLists ++ lhsVsRhsConflictCandidateLists
      )
    }
  }

  /**
   * For each conflict between two plans, find a candidate list of plans. Planning Eager on top of any plan
   * in the candidate list will solve the respective conflict.
   *
   * This is done by traversing the plan and accumulating the plans between the two conflicting plans as candidates.
   */
  private[eager] def findCandidateLists(plan: LogicalPlan, conflicts: Seq[ConflictingPlanPair]): Seq[CandidateList] = {

    def pushLayerForLeafPlans(acc: SequencesAcc, p: LogicalPlan): SequencesAcc = {
      p match {
        case _: LogicalLeafPlan => acc.pushLayer()
        case _                  => acc
      }
    }

    def checkConstraints(p: LogicalPlan): Unit = {
      p match {
        // All Semi Apply variants must have read-only RHSs
        // If this changes in the future, the correct way to eagerize a SemiApply variant
        // that conflicts with its RHS is like this:
        // .selectOrSemiApply("var1")
        // .|.setLabel("n:A")
        // .|. ...
        // .eager()
        // .projection("a:A AS va1")
        // . ...
        case s: SingleFromRightLogicalPlan if !s.right.readOnly =>
          throw new IllegalStateException(
            "Eagerness analysis does not support if the RHS of a SingleFromRightLogicalPlan contains writes"
          )

        case _ =>
      }
    }

    def updateSequences(acc: SequencesAcc, p: LogicalPlan): SequencesAcc = {
      // Find conflicts that contain this plan
      val SequencesAcc(remainingConflicts, newOpenSequences, _, _) =
        acc.openConflicts.foldLeft(SequencesAcc(Seq.empty)) {
          case (innerAcc, conflict) =>
            if (conflict.first == p) {
              // Add an open sequence that ends at the other plan and initially contains p as a candidate
              innerAcc.withAddedOpenSequence(p, conflict.second, conflict, acc.currentLayer)
            } else if (conflict.second == p) {
              // Add an open sequence that ends at the other plan and initially contains p as a candidate
              innerAcc.withAddedOpenSequence(p, conflict.first, conflict, acc.currentLayer)
            } else {
              // Keep the conflict around
              innerAcc.withAddedConflict(conflict)
            }
        }

      // Find open sequences that are closed by this plan
      val newCandidateLists = acc.openSequences.getOrElse(p, Seq.empty).map(_.candidateListWithReason)

      val remainingOpenSequences = {
        // All sequences that do not end in p
        (acc.openSequences - p)
          .view.mapValues(_.map {
            case os @ OpenSequence(_, layer, _) if acc.currentLayer == layer =>
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

    def processPlan(acc: SequencesAcc, plan: LogicalPlan): SequencesAcc = {
      checkConstraints(plan)
      updateSequences(pushLayerForLeafPlans(acc, plan), plan)
    }

    val sequencesAcc = LogicalPlans.foldPlan(SequencesAcc(conflicts))(
      plan,
      (acc, p) => processPlan(acc, p),
      (lhsAcc, rhsAcc, p) =>
        p match {
          case _: ApplyPlan =>
            // Pop a layer and use the RHS acc which was initialized with the LHS acc
            processPlan(rhsAcc.popLayer(), p)
          case b: LogicalBinaryPlan =>
            // as a non-apply binary plan, we need to combine the information from both legs
            processPlan(lhsAcc.combineWithRhs(rhsAcc, b), b)
        }
    )

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openSequences.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openConflicts.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.currentLayer == 0)
    val candidateLists = sequencesAcc.candidateLists

    // Remove CandidateLists that traverse an EagerLogicalPlan coming from the LHS.
    // These do not need to be eagerized.
    candidateLists.filter(_.candidates.sliding(2).forall {
      case Seq(first, second: EagerLogicalPlan) if second.lhs.contains(first) => false
      case _                                                                  => true
    })
  }
}
