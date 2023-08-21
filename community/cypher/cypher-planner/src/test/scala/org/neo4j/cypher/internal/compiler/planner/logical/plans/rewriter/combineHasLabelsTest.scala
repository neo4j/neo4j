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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class combineHasLabelsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should combine two has labels") {
    val expr = ors(
      hasLabels(varFor("n"), "Drummer"),
      hasLabels(varFor("n"), "BassPlayer")
    )
    rewrite(expr) shouldBe hasAnyLabel("n", "Drummer", "BassPlayer")
  }

  test("should not rewrite single has labels") {
    val expr = ors(hasLabels(varFor("n"), "Drummer"))
    rewrite(expr) should be theSameInstanceAs expr
  }

  test("should not rewrite multi labels") {
    val expr = ors(
      hasLabels(varFor("n"), "Drummer"),
      hasLabels(varFor("n"), "Pianist", "Singer")
    )
    rewrite(expr) should be theSameInstanceAs expr
  }

  test("should rewrite some has labels") {
    val expr = ors(
      hasLabels(varFor("n"), "Drummer"),
      hasLabels(varFor("n"), "Pianist", "Singer"),
      hasLabels(varFor("n"), "BassPlayer")
    )
    rewrite(expr) shouldBe ors(
      hasLabels(varFor("n"), "Pianist", "Singer"),
      hasAnyLabel("n", "Drummer", "BassPlayer")
    )
  }

  test("should combine has labels on multiple nodes") {
    val expr = ors(
      hasLabels(varFor("a"), "Drummer"),
      hasLabels(varFor("b"), "Sailor"),
      hasLabels(varFor("a"), "Singer"),
      hasLabels(varFor("b"), "Captain"),
      hasLabels(varFor("c"), "Cat")
    )
    rewrite(expr) shouldBe ors(
      hasLabels(varFor("c"), "Cat"),
      hasAnyLabel("a", "Drummer", "Singer"),
      hasAnyLabel("b", "Sailor", "Captain")
    )
  }

  private def rewrite(e: Expression): Expression = e.endoRewrite(combineHasLabels)
}
