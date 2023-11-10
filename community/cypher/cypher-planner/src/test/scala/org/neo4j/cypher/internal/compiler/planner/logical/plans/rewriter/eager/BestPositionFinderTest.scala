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

import org.neo4j.cypher.internal.compiler.helpers.FakeLeafPlan
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.BestPositionFinder.CandidateSetWithMinimum
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class BestPositionFinderTest extends CypherFunSuite {

  implicit val idGen: IdGen = new SequentialIdGen()

  test("should merge 2nd and 3rd candidate set") {
    val plans = Seq.fill(3)(Ref(FakeLeafPlan()))

    val candidateSets = Seq(
      CandidateSetWithMinimum(Set(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(Set(plans(0)), plans(0), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(Set(plans(0), plans(1)), plans(1), Set(EagernessReason.Unknown))
    )

    BestPositionFinder.mergeCandidateSets(candidateSets) should equal(Seq(
      CandidateSetWithMinimum(Set(plans(2)), plans(2), Set(EagernessReason.Unknown)),
      CandidateSetWithMinimum(Set(plans(0)), plans(0), Set(EagernessReason.Unknown))
    ))
  }

}
