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

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.BestPositionFinder.CandidateSetWithMinimum
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

//noinspection ZeroIndexToHead
class BestPositionFinderTest extends CypherFunSuite {

  implicit val idGen: IdGen = new SequentialIdGen()

  // --------
  // tryMerge
  // --------

  test("should not merge two candidate sets (no intersection)") {
    val plans = 0 until 2

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1)), plans(1), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(None)
  }

  test("should merge two candidate sets (1st subset of 2nd)") {
    val plans = 0 until 2

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(Some(
      CandidateSetWithMinimum(
        BitSet(plans(0)),
        plans(0),
        Set(EagernessReason.Unknown, EagernessReason.ReadCreateConflict)
      )
    ))
  }

  test("should merge two candidate sets (2nd subset of 1st)") {
    val plans = 0 until 2

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(Some(
      CandidateSetWithMinimum(
        BitSet(plans(0)),
        plans(0),
        Set(EagernessReason.Unknown, EagernessReason.ReadCreateConflict)
      )
    ))
  }

  test("should merge two candidate sets (same minimum)") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1), plans(2)), plans(1), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(Some(
      CandidateSetWithMinimum(
        BitSet(plans(1)),
        plans(1),
        Set(EagernessReason.Unknown, EagernessReason.ReadCreateConflict)
      )
    ))
  }

  test("should merge two candidate sets (first minimum lies in intersection)") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1), plans(2)), plans(2), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(Some(
      CandidateSetWithMinimum(
        BitSet(plans(1)),
        plans(1),
        Set(EagernessReason.Unknown, EagernessReason.ReadCreateConflict)
      )
    ))
  }

  test("should merge two candidate sets (second minimum lies in intersection)") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1), plans(2)), plans(1), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(Some(
      CandidateSetWithMinimum(
        BitSet(plans(1)),
        plans(1),
        Set(EagernessReason.Unknown, EagernessReason.ReadCreateConflict)
      )
    ))
  }

  test("should not merge two candidate sets (no minimum lies in intersection)") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1), plans(2)), plans(2), Set(EagernessReason.ReadCreateConflict))
    )

    BestPositionFinder.tryMerge(candidateSets(0), candidateSets(1)) should equal(None)
  }

  // ------------------
  // mergeCandidateSets
  // ------------------

  test("should merge 1st and 2nd candidate set") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown))
    ))
  }

  test("should merge 1st and 3rd candidate set") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown))
    ))
  }

  test("should merge 2nd and 3rd candidate set") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown))
    ))
  }

  test("should merge three candidate set") {
    val plans = 0 until 3

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1), plans(2)), plans(2), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown))
    ))
  }

  test("should merge multiple candidate sets") {
    val plans = 0 until 4

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(2), plans(3)), plans(3), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(0)), plans(0), Set(EagernessReason.Unknown))
    ))
  }

  test("in a chain of intersecting sets, merge 1st and 2nd") {
    val plans = 0 until 5

    val candidateSets = Seq(
      CandidateSetWithMinimum(BitSet(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(1), plans(2), plans(3)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(3), plans(4)), plans(3), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets).toSet should equal(Set(
      CandidateSetWithMinimum(BitSet(plans(1)), plans(1), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(BitSet(plans(3), plans(4)), plans(3), Set(EagernessReason.Unknown))
    ))

    // An alternative would have been to merge the 2nd and 3rd.
    // And, depending on cardinalities, that alternative could be better.
    // Since we don't do an exhaustive search, the algorithm always tries to merge
    // a candidateSet with the "earliest" in the buffer that works (lowest index).
    // So this test is mostly to describe the current behavior, not to assert that this
    // the best solution.
  }

}
