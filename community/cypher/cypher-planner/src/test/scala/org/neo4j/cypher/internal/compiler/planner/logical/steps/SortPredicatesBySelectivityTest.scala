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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SortPredicatesBySelectivity.groupReorderablePredicates
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.PredicateCost
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.OptionValues

class SortPredicatesBySelectivityTest extends CypherFunSuite with AstConstructionTestSupport with OptionValues {

  private val cheap = PredicateCost(CostPerRow(0.5), Selectivity.of(0.1).value)
  private val expensive = PredicateCost(CostPerRow(0.7), Selectivity.of(0.1).value)

  test("should handle empty input") {
    groupReorderablePredicates(Seq.empty, SemanticTable()) shouldEqual Seq()
  }

  test("should not group predicates without store access") {
    val predicatesWithCosts = Seq(
      greaterThan(literal(1), literal(2)) -> cheap,
      greaterThan(literal(3), literal(4)) -> cheap,
      greaterThan(literal(5), literal(6)) -> expensive,
      greaterThan(literal(7), literal(8)) -> expensive
    )

    val res = groupReorderablePredicates(predicatesWithCosts, SemanticTable())
    res shouldEqual predicatesWithCosts.map(_._1)
  }

  test("should group predicates with equal costs and store access count") {
    val aLabel = hasLabels("a", "A")
    val bLabel = hasLabels("b", "B")
    val cLabel = hasLabels("c", "C")
    val dLabel = hasLabels("d", "D")

    val predicatesWithCosts = Seq(
      aLabel -> cheap,
      bLabel -> cheap,
      cLabel -> expensive,
      dLabel -> expensive
    )

    val res = groupReorderablePredicates(predicatesWithCosts, SemanticTable())

    res shouldEqual Seq(
      andsReorderableAst(aLabel, bLabel),
      andsReorderableAst(cLabel, dLabel)
    )
  }

  test("should ungroup predicates with equal cost but no store access") {
    val aLabel = hasLabels("a", "A")
    val bLabel = hasLabels("b", "B")
    val cLabel = hasLabels("c", "C")
    val dLabel = hasLabels("d", "D")
    val gt = greaterThan(literal(1), literal(2))

    val predicatesWithCosts = Seq(
      aLabel -> cheap,
      bLabel -> cheap,
      cLabel -> expensive,
      gt -> expensive,
      dLabel -> expensive
    )

    val res = groupReorderablePredicates(predicatesWithCosts, SemanticTable())

    res shouldEqual Seq(
      andsReorderableAst(aLabel, bLabel),
      gt,
      andsReorderableAst(cLabel, dLabel)
    )
  }

  test("should group different types of predicates with equal costs") {
    val aLabel = hasLabels("a", "A")
    val bLabel = hasLabels("b", "B")
    val cLabel = hasLabels("c", "C")
    val dLabel = hasLabels("d", "D")
    val nPropGt = greaterThan(prop("n", "prop"), literal(2))

    val predicatesWithCosts = Seq(
      aLabel -> cheap,
      bLabel -> cheap,
      nPropGt -> expensive,
      cLabel -> expensive,
      dLabel -> expensive
    )

    val semanticTable = SemanticTable().addNode(v"n")
    val res = groupReorderablePredicates(predicatesWithCosts, semanticTable)

    res shouldEqual Seq(
      andsReorderableAst(aLabel, bLabel),
      andsReorderableAst(nPropGt, cLabel, dLabel)
    )
  }

  test(
    "should put inequality predicate into a cheaper group if that group already contains an inequality predicate with the same store access count"
  ) {
    val aLabel = hasLabels("a", "A")
    val bLabel = hasLabels("b", "B")
    val cLabel = hasLabels("c", "C")
    val nPropGt = greaterThan(prop("n", "prop"), literal(2))
    val xPropGte = greaterThanOrEqual(prop("x", "prop"), literal(3))

    val predicatesWithCosts = Seq(
      aLabel -> cheap,
      xPropGte -> cheap,
      nPropGt -> expensive,
      bLabel -> expensive,
      cLabel -> expensive
    )

    val semanticTable = SemanticTable().addNode(v"n").addNode(v"x")
    val res = groupReorderablePredicates(predicatesWithCosts, semanticTable)

    res shouldEqual Seq(
      andsReorderableAst(aLabel, xPropGte, nPropGt),
      andsReorderableAst(bLabel, cLabel)
    )
  }
}
