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
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

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

  private object OpenSequence {

    def apply(firstCandidate: Ref[LogicalPlan], layer: Int, conflict: ConflictingPlanPair): OpenSequence =
      OpenSequence(List(firstCandidate), layer, conflict, traversesEagerPlanFromLeft = false)
  }

  /**
   * Represents a growing (therefore open) list of candidate plans on which an eager could be planned to solve the given conflict.
   *
   * @param candidates                 candidates for an eager operator found so far.
   *                                   The candidates are in an order such that `candidates(n).children.contains(candidates(n+1))`.
   * @param layer                      layer of the conflicting plan that was already traversed.
   * @param conflict                   the conflict that should be solved by planning an eager on either of the candidate plans
   * @param traversesEagerPlanFromLeft `true` if, traversing `candidates` a [[EagerLogicalPlan]] is traversed from left.
   *                                   Kept as a field for performance reasons.
   */
  private case class OpenSequence(
    candidates: List[Ref[LogicalPlan]],
    layer: Int,
    conflict: ConflictingPlanPair,
    traversesEagerPlanFromLeft: Boolean
  ) {

    /**
     * Filters out CandidateLists that traverse an EagerLogicalPlan coming from the LHS.
     * These do not need to be eagerized.
     *
     * @return a [[CandidateList]] with the current candidates and the reason of the conflict, if traversesEagerPlanFromLeft == `false`. `None`, otherwise.
     */
    def candidateListWithConflict: Option[CandidateList] = {
      if (!traversesEagerPlanFromLeft)
        Some(CandidateList(candidates = candidates, conflict = conflict))
      else
        None
    }

    /**
     * Adds a candidate and also updates `traversesEagerPlanFromLeft`.
     */
    def withAddedCandidate(candidate: Ref[LogicalPlan]): OpenSequence = {
      copy(
        candidates = candidate :: candidates,
        traversesEagerPlanFromLeft = traversesEagerPlanFromLeft || ((candidate, candidates) match {
          case (Ref(first: EagerLogicalPlan), Ref(second) :: _) if first.lhs.contains(second) => true
          case _                                                                              => false
        })
      )
    }

    def withEmptyCandidates(): OpenSequence =
      copy(candidates = Nil, traversesEagerPlanFromLeft = false)
  }

  sealed trait LHSvsRHSEagerization {

    /**
     * Given the candidate lists accumulated so far, filter them out based
     * on if this strategy should eagerite LHS vs RHS conflicts.
     */
    def filterCandidateLists(candidateLists: Vector[CandidateList], plan: LogicalBinaryPlan)(implicit
    planChildrenLookup: PlanChildrenLookup): Vector[CandidateList]
  }

  object LHSvsRHSEagerization {

    /**
     * Eagerize conflicts between the LHS and the RHS of the plan.
     */
    case object Yes extends LHSvsRHSEagerization {

      override def filterCandidateLists(candidateLists: Vector[CandidateList], plan: LogicalBinaryPlan)(implicit
      planChildrenLookup: PlanChildrenLookup): Vector[CandidateList] = {
        candidateLists
      }
    }

    /**
     * Do not eagerize conflicts between the LHS and the RHS of the plan.
     */
    case object No extends LHSvsRHSEagerization {

      override def filterCandidateLists(candidateLists: Vector[CandidateList], plan: LogicalBinaryPlan)(implicit
      planChildrenLookup: PlanChildrenLookup): Vector[CandidateList] = {
        val otherCandidateListsBuilder = Vector.newBuilder[CandidateList]
        candidateLists.foreach { cl =>
          if (!isLhsVsRhsConflict(plan, cl))
            otherCandidateListsBuilder.addOne(cl)
        }
        otherCandidateListsBuilder.result()
      }
    }

    /**
     * Assert there are no conflicts between the LHS and the RHS of the plan.
     */
    case object AssertNoConflicts extends LHSvsRHSEagerization {

      override def filterCandidateLists(candidateLists: Vector[CandidateList], plan: LogicalBinaryPlan)(implicit
      planChildrenLookup: PlanChildrenLookup): Vector[CandidateList] = {
        val otherCandidateListsBuilder = Vector.newBuilder[CandidateList]
        candidateLists.foreach { cl =>
          if (!isLhsVsRhsConflict(plan, cl))
            otherCandidateListsBuilder.addOne(cl)
          else
            throw new IllegalStateException(
              s"We do not expect conflicts between the two branches of a ${plan.getClass.getSimpleName} yet."
            )
        }
        otherCandidateListsBuilder.result()
      }
    }
  }

  /**
   * @param eagerizeLHSvsRHSConflicts               if `Yes` a conflict between the LHS and the RHS needs an Eager on the LHS
   * @param emptyCandidateListsForRHSvsTopConflicts if `true` a conflict between the RHS and the Top can only be solved by
   *                                                an Eager on Top
   */
  private case class BinaryPlanEagerizationStrategy(
    eagerizeLHSvsRHSConflicts: LHSvsRHSEagerization,
    emptyCandidateListsForRHSvsTopConflicts: Boolean
  )

  private object BinaryPlanEagerizationStrategy {

    private def assertHasReadOnlyRHS(plan: LogicalBinaryPlan): Boolean = {
      if (!plan.right.readOnly) {
        throw new IllegalStateException(
          s"Eagerness analysis does not support if the RHS of a ${plan.getClass.getSimpleName} contains writes."
        )
      }
      true
    }

    def forPlan(plan: LogicalBinaryPlan): BinaryPlanEagerizationStrategy = {
      // In all places where `emptyCandidateListsForRHSvsTopConflicts = true`,
      // we must be sure that plan does not conflict with its RHS.
      // Otherwise we will get empty candidate lists.
      plan match {
        case _: EagerLogicalPlan => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.No,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: Union => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.No,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: OrderedUnion => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.AssertNoConflicts,
            emptyCandidateListsForRHSvsTopConflicts = false
          )
        case _: AssertSameNode => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.AssertNoConflicts,
            emptyCandidateListsForRHSvsTopConflicts = assertHasReadOnlyRHS(plan)
          )
        case _: AssertSameRelationship => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.AssertNoConflicts,
            emptyCandidateListsForRHSvsTopConflicts = assertHasReadOnlyRHS(plan)
          )
        case _: CartesianProduct => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.Yes,
            emptyCandidateListsForRHSvsTopConflicts = assertHasReadOnlyRHS(plan)
          )
        case _: RepeatOptions => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.AssertNoConflicts,
            emptyCandidateListsForRHSvsTopConflicts = assertHasReadOnlyRHS(plan)
          )
        case ap: ApplyPlan => BinaryPlanEagerizationStrategy(
            eagerizeLHSvsRHSConflicts = LHSvsRHSEagerization.Yes,
            emptyCandidateListsForRHSvsTopConflicts =
              // Most Apply variants must have read-only RHSs
              // If this changes in the future, the correct way to eagerize, e.g. a SemiApply variant
              // that conflicts with its RHS is like this:
              // .selectOrSemiApply("var1")
              // .|.setLabel("n:A")
              // .|. ...
              // .eager()
              // .projection("a:A AS var1")
              // . ...
              // Extra match for exhaustiveness check
              ap match {
                case _: SingleFromRightLogicalPlan |
                  _: AntiConditionalApply |
                  _: ConditionalApply |
                  _: BidirectionalRepeatTrail |
                  _: PathPropagatingBFS |
                  _: RollUpApply |
                  _: Trail |
                  _: TriadicSelection =>
                  assertHasReadOnlyRHS(plan)
                case _: ForeachApply =>
                  // Do nothing.
                  // For now, we accept that ForeachApply can conflict with its RHS, and simply don't solve such conflicts.
                  // We first need to define side-effect visibility of FOREACH before making changes to this.
                  true
                case _: SubqueryForeach | _: TransactionApply | _: TransactionForeach | _: Apply =>
                  // Do nothing.
                  // These plans can have writes on the RHS, but they should not be able to conflict with their RHS,
                  // given the things that they allow. E.g. a TransactionApply can have an Expression in `batchSize`,
                  // but that expression needs to be a literal int, so it cannot be a CASE expression performing
                  // any reads that could conflict with the RHS.
                  true
              }
          )
      }
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
    openSequences: Map[Ref[LogicalPlan], List[OpenSequence]] = Map.empty,
    candidateLists: Vector[CandidateList] = Vector.empty,
    currentLayer: Int = -1
  ) {

    def withAddedOpenSequence(
      start: Ref[LogicalPlan],
      end: Ref[LogicalPlan],
      conflict: ConflictingPlanPair,
      layer: Int
    ): SequencesAcc = {
      val oS = openSequences.getOrElse(end, Nil)
      copy(openSequences =
        openSequences.updated(end, OpenSequence(start, layer, conflict) :: oS)
      )
    }

    def pushLayer(): SequencesAcc = copy(currentLayer = currentLayer + 1)

    def popLayer(emptyCandidateListsForRHSvsTopConflicts: Boolean): SequencesAcc = {
      val newLayer = currentLayer - 1
      // All open sequences of this layer are moved to the upper layer.
      // If instructed to, they also lose all candidates so far.
      // This is because a conflict between a plan on the RHS and on top of, e.g. an Apply must be solved with an Eager on top of the Apply.
      val newOpenSequences = openSequences.view.mapValues {
        _.map {
          case os @ OpenSequence(_, layer, _, _) if layer == currentLayer =>
            val osWithNewLayer = os.copy(layer = newLayer)
            if (emptyCandidateListsForRHSvsTopConflicts)
              osWithNewLayer.withEmptyCandidates()
            else
              osWithNewLayer
          case x => x
        }
      }.toMap
      copy(
        currentLayer = newLayer,
        openSequences = newOpenSequences,
        candidateLists = candidateLists
      )
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
    val newCandidateLists = acc.openSequences.getOrElse(p, Seq.empty).flatMap(_.candidateListWithConflict)

    val remainingOpenSequences = {
      // All sequences that do not end in p
      (acc.openSequences - p)
        .view.mapValues(_.map {
          case os @ OpenSequence(_, layer, _, _) if acc.currentLayer == layer =>
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

  private def planTreeContains(
    planTree: LogicalPlan,
    planToFind: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    (planTree eq planToFind) || planChildrenLookup.hasChild(planTree, planToFind)
  }

  private def isLhsVsRhsConflict(
    plan: LogicalBinaryPlan,
    candidateList: CandidateList
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    candidateList match {
      case CandidateList(_, ConflictingPlanPair(first, second, _)) =>
        (planTreeContains(plan.left, first.value) && planTreeContains(plan.right, second.value)) ||
        (planTreeContains(plan.left, second.value) && planTreeContains(plan.right, first.value))
    }
  }

  private def preProcessBinaryPlan(
    acc: SequencesAcc,
    plan: LogicalBinaryPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): SequencesAcc = {
    val eagerizationStrategy = BinaryPlanEagerizationStrategy.forPlan(plan)
    val newCandidateLists =
      eagerizationStrategy.eagerizeLHSvsRHSConflicts.filterCandidateLists(acc.candidateLists, plan)

    acc.copy(
      candidateLists = newCandidateLists
    ).popLayer(eagerizationStrategy.emptyCandidateListsForRHSvsTopConflicts)
  }

  private def processPlan(
    acc: SequencesAcc,
    plan: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): SequencesAcc = {

    Function.chain[SequencesAcc](Seq(
      plan match {
        case plan: LogicalBinaryPlan => preProcessBinaryPlan(_, plan)
        case _: LogicalLeafPlan      => _.pushLayer()
        case _                       => identity
      },
      updateSequences(_, Ref(plan))
    ))(acc)
  }

  /**
   * For each conflict between two plans, find a candidate list of plans. Planning Eager on top of any plan
   * in the candidate list will solve the respective conflict.
   *
   * This is done by traversing the plan and accumulating the plans between the two conflicting plans as candidates.
   */
  private[eager] def findCandidateLists(
    plan: LogicalPlan,
    conflicts: Seq[ConflictingPlanPair]
  )(implicit planChildrenLookup: PlanChildrenLookup): Seq[CandidateList] = {
    val conflictsMapBuilder = scala.collection.mutable.MultiDict.empty[Ref[LogicalPlan], ConflictingPlanPair]
    conflicts.foreach {
      case c @ ConflictingPlanPair(first, second, _) =>
        conflictsMapBuilder.addOne(first -> c).addOne(second -> c)
    }
    val conflictsMap = conflictsMapBuilder.sets.view.mapValues(_.toSet).toMap

    val sequencesAcc = LogicalPlans.simpleFoldPlan(SequencesAcc(conflictsMap))(
      plan,
      processPlan
    )

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openSequences.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.openConflicts.isEmpty)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(sequencesAcc.currentLayer == 0)
    val candidateLists = sequencesAcc.candidateLists
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(candidateLists.forall(_.candidates.nonEmpty))

    candidateLists
  }
}
