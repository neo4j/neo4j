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

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.CandidateListFinder.CandidateList
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import scala.collection.CypherPlannerBitSetOptimizations
import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
 * Given the candidate lists, finds the best positions for Eager plans by looking at Cardinalities and trying to merge candidate lists,
 */
object BestPositionFinder {

  /**
   * If we have more candidateLists than this, then we won't attempt to merge them,
   * and insert Eager at the local optima of each candidateList.
   */
  private val SIZE_LIMIT = 50

  /**
   * @param candidates the candidate plans, as a BitSet of their IDs
   * @param minimum    the id of plan with the lowest cardinality in candidates
   * @param reasons    all reasons that contributed to this candidateSet
   */
  private[eager] case class CandidateSetWithMinimum(
    candidates: BitSet,
    minimum: Int,
    reasons: Set[EagernessReason]
  )

  /**
   * By looking at the candidates in all lists, pick the best locations for planning Eager.
   *
   * @return a map from a plan id on which we need to plan Eager to the EagernessReasons.
   */
  private[eager] def pickPlansToEagerize(
    cardinalities: Cardinalities,
    candidateLists: Seq[CandidateList]
  ): Map[Id, ListSet[EagernessReason]] = {
    val results = if (candidateLists.size > SIZE_LIMIT) {
      candidateLists.map(cl =>
        cl.candidates.minBy(planId => cardinalities.get(Id(planId))) -> cl.conflict.reasons
      )
    } else {
      // Find the minimum of each candidate set
      val csWithMinima = candidateLists.map(cl =>
        CandidateSetWithMinimum(
          cl.candidates,
          cl.candidates.minBy(planId => cardinalities.get(Id(planId))),
          cl.conflict.reasons
        )
      )
      mergeCandidateSets(csWithMinima)
        .map(cl => cl.minimum -> cl.reasons)
    }

    results
      .groupBy {
        case (id, _) => Id(id)
      }
      .view
      .mapValues(_.view.flatMap(_._2).to(ListSet))
      .toMap
  }

  /**
   * Try to find the best overall location by looking at all candidate sets.
   * If there are situations where sets more than pairwise overlap, this algorithm will not necessarily find the global optimum,
   * since it only tries pairwise merging of sequences. But, it "only" has quadratic complexity.
   *
   * @param csWithMinima a sequence of candidate sets
   * @return the same sequence, but with merged candidate sets, if possible
   */
  private[eager] def mergeCandidateSets(csWithMinima: Seq[CandidateSetWithMinimum]): Seq[CandidateSetWithMinimum] = {
    val buffer = mutable.ArrayBuffer[CandidateSetWithMinimum]()
    csWithMinima.foreach { listA =>
      if (buffer.isEmpty) {
        buffer += listA
      } else {
        var merged = false
        val it = buffer.zipWithIndex.iterator

        // Go through all lists already in results and see if the current one can get merged with any other list.
        while (!merged && it.hasNext) {
          val (listB, i) = it.next()
          tryMerge(listA, listB).foreach { mergedList =>
            // If so, only keep the merged list
            merged = true
            buffer.remove(i)
            buffer += mergedList
          }
        }
        if (!merged) {
          // Otherwise keep all lists and add the new one.
          buffer += listA
        }
      }
    }
    buffer.toSeq
  }

  /**
   * Merge two candidate sets if they overlap.
   */
  private[eager] def tryMerge(
    a: CandidateSetWithMinimum,
    b: CandidateSetWithMinimum
  ): Option[CandidateSetWithMinimum] = {
    val aSet = a.candidates
    val bSet = b.candidates
    val intersection = aSet intersect bSet

    if (intersection.isEmpty) {
      // We cannot merge non-intersecting sets
      None
    } else if (CypherPlannerBitSetOptimizations.subsetOf(aSet, bSet)) {
      // If a is a subset of b, we can return that a, with merged reasons.
      Some(a.copy(reasons = a.reasons ++ b.reasons))
    } else if (CypherPlannerBitSetOptimizations.subsetOf(bSet, aSet)) {
      // If b is a subset of a, we can return that b, with merged reasons.
      Some(b.copy(reasons = a.reasons ++ b.reasons))
    } else if (a.minimum == b.minimum || intersection.contains(a.minimum)) {
      // If they have the same minimum, or a's minimum lies in the intersection,
      // return the intersection of both sets with a's minimum and merged reasons.
      Some(CandidateSetWithMinimum(intersection, a.minimum, a.reasons ++ b.reasons))
    } else if (intersection.contains(b.minimum)) {
      // If b's minimum lies in the intersection,
      // return the intersection of both sets with b's minimum and merged reasons.
      Some(CandidateSetWithMinimum(intersection, b.minimum, a.reasons ++ b.reasons))
    } else {
      // Both sets have their own minima, and neither lies in the intersection.
      None
    }
  }

}
