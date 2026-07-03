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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DistinctnessTest extends CypherFunSuite {

  implicit private val idGen: SequentialIdGen = new SequentialIdGen()

  private val n = varFor("n")
  private val m = varFor("m")
  private val r = varFor("r")

  // Sources with controlled distinctness values
  // AllNodesScan is a NodeLogicalLeafPlan, so it has DistinctColumns(idName)
  private def distinctOnN: LogicalPlan = AllNodesScan(n, Set.empty)
  private def distinctOnM: LogicalPlan = AllNodesScan(m, Set.empty)

  // OptionalExpand hardcodes NotDistinct regardless of source or direction
  private def notDistinctSource: LogicalPlan =
    OptionalExpand(AllNodesScan(n, Set.empty), n, OUTGOING, Seq.empty, Some(m), Some(varFor("x")))

  // Limit(_, 1) triggers distinctColumnsOfLimit: AtMostOneRow
  private def atMostOneRow: LogicalPlan =
    Limit(AllNodesScan(n, Set.empty), SignedDecimalIntegerLiteral("1")(InputPosition.NONE))

  private val notDistinctCases =
    Seq(
      (distinctOnN, n, BOTH, Some(m), Some(r), "BOTH direction (self-loop risk)"),
      (distinctOnN, n, OUTGOING, Some(m), None, "no rel variable"),
      (notDistinctSource, n, OUTGOING, Some(m), Some(r), "non-distinct source"),
      (distinctOnM, n, OUTGOING, Some(m), Some(r), "source distinct on different variable")
    )

  // distinctColumnsOfExpand: direct unit tests

  test("directed Expand from distinct source produces DistinctColumns on rel") {
    for {
      (source, sourceClue) <- Seq((distinctOnN, "distinct"), (atMostOneRow, "atMostOneRow"))
      (dir, dirClue) <- Seq((OUTGOING, "OUTGOING"), (INCOMING, "INCOMING"))
    } {
      withClue(s"$dirClue from $sourceClue:") {
        Distinctness.distinctColumnsOfExpand(source, n, dir, Some(r)) shouldEqual DistinctColumns(r)
      }
    }
  }

  test("distinctColumnsOfExpand produces NotDistinct when conditions are not met") {
    for ((source, from, dir, to, relName, clue) <- notDistinctCases) {
      withClue(clue) {
        Distinctness.distinctColumnsOfExpand(source, from, dir, relName) shouldEqual NotDistinct
      }
    }
  }

  // Expand plan wiring tests — verify that Expand.distinctness delegates correctly

  test("Expand with directed traversal from distinct node has DistinctColumns on rel") {
    for {
      (source, sourceClue) <- Seq((distinctOnN, "distinct"), (atMostOneRow, "atMostOneRow"))
      expansionMode <- Seq(ExpandAll, ExpandInto)
      (dir, dirClue) <- Seq((OUTGOING, "OUTGOING"), (INCOMING, "INCOMING"))
    } {
      withClue(s"$dirClue of $expansionMode on $sourceClue:") {
        val expand = Expand(source, n, dir, Seq.empty, Some(m), Some(r), expansionMode)
        expand.distinctness shouldEqual DistinctColumns(r)
      }
    }
  }

  test("Expand plan has NotDistinct when conditions are not met") {
    for ((source, from, dir, to, relName, clue) <- notDistinctCases) {
      withClue(clue) {
        val expand = Expand(source, from, dir, Seq.empty, to, relName)
        expand.distinctness shouldEqual NotDistinct
      }
    }
  }
}
