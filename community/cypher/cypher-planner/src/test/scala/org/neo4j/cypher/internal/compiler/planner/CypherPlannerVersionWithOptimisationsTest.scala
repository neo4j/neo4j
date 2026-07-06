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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations.Experimental
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations.Next
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations.V2026_04
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations.V2026_05
import org.neo4j.cypher.internal.compiler.planner.Optimisation.MergeLabelInfo
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption

class CypherPlannerVersionWithOptimisationsTest extends CypherPlannerTestSuite {

  test("latest version is V2026_05") {
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.latest) should be(V2026_05)
  }

  test("fromQueryOption should convert versions correctly") {
    // v2026_03 is retired, so it resolves to the default planner (V2026_05) rather than its own object.
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.v2026_03) shouldEqual V2026_05
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.v2026_04) shouldEqual V2026_04
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.v2026_05) shouldEqual V2026_05
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.latest) shouldEqual V2026_05
    CypherPlannerVersionWithOptimisations.fromQueryOption(CypherPlannerVersionOption.next) shouldEqual Next
    CypherPlannerVersionWithOptimisations.fromQueryOption(
      CypherPlannerVersionOption.experimental
    ) shouldEqual Experimental

    CypherPlannerVersionOption.supportedValues
      .map(CypherPlannerVersionWithOptimisations.fromQueryOption) shouldEqual
      Seq(
        Experimental,
        Next,
        V2026_05,
        V2026_04,
        // v2026_03 is retired and resolves to the default planner.
        V2026_05
      )
  }

  test("version chain is properly linked") {
    V2026_04.previous shouldEqual None
    V2026_05.previous shouldEqual Some(V2026_04)
    Next.previous shouldEqual Some(V2026_05)
    Experimental.previous shouldEqual Some(Next)
  }

  test("allSupportedOptimisations traverses the version chain correctly") {
    V2026_04.allSupportedOptimisations shouldBe empty
    V2026_05.allSupportedOptimisations shouldBe empty
    Next.allSupportedOptimisations shouldBe empty
    Experimental.allSupportedOptimisations shouldEqual Set(MergeLabelInfo)
  }

  test("allSupportedOptimisations on companion object returns correct results") {
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.v2026_03) shouldBe empty
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.v2026_04) shouldBe empty
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.v2026_05) shouldBe empty
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.next) shouldBe empty
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(
      CypherPlannerVersionOption.experimental
    ) shouldEqual Set(
      MergeLabelInfo
    )
    CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.latest) shouldEqual
      CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.v2026_05)
  }

  test("the top-of-chain planner version supports every optimisation") {
    // Experimental is the top of the version chain, so it accumulates every optimisation. If a future
    // retirement drops a version's optimisations from the chain (or a new optimisation is never wired into a
    // version), this fails and forces the optimisation to be carried over.
    Experimental.allSupportedOptimisations shouldEqual Optimisation.values
  }
}
