/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.v3_4.expressions.{Equals, Expression, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_4.logical.plans.{NodeHashJoin, Selection}

class PredicateRemovalThroughJoinsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val aHasLabel = identHasLabel("a", "LABEL")
  val pred1: Expression = Equals(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("42")_)_
  val pred2: Expression = Equals(SignedDecimalIntegerLiteral("44")_, SignedDecimalIntegerLiteral("44")_)_

  test("same predicate on both sides - Selection is removed entirely") {
    // Given
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val lhsSelection = selectionOp("a", solveds, cardinalities, aHasLabel)
    val rhsLeaf = newMockedLogicalPlan(solveds, cardinalities, "a")
    val rhsSelection = Selection(Seq(aHasLabel), rhsLeaf)
    val join = NodeHashJoin(Set("a"), lhsSelection, rhsSelection)

    // When
    val result = join.endoRewrite(predicateRemovalThroughJoins(solveds, cardinalities, Attributes(idGen)))

    // Then the Selection operator is removed from the RHS
    result should equal(NodeHashJoin(Set("a"), lhsSelection, rhsLeaf))
  }

  test("multiple predicates on both sides - only one is common on both sides and is removed") {
    // Given
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val lhsSelection = selectionOp("a", solveds, cardinalities, aHasLabel, pred1)
    val rhsLeaf = newMockedLogicalPlan(solveds, cardinalities, "a")
    val rhsSelection = Selection(Seq(aHasLabel, pred2), rhsLeaf)
    val join = NodeHashJoin(Set("a"), lhsSelection, rhsSelection)

    // When rewritten
    val result = join.endoRewrite(predicateRemovalThroughJoins(solveds, cardinalities, Attributes(idGen)))

    // Then the predicate is removed from the RHS selection operator
    val newRhsSelection = Selection(Seq(pred2), rhsLeaf)

    result should equal(
      NodeHashJoin(Set("a"), lhsSelection, newRhsSelection))
  }

  test("same predicate on both sides, but not depending on the join ids - nothing should be removed") {
    // Given
    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val lhsSelection = selectionOp("a", solveds, cardinalities, pred1)
    val rhsLeaf = newMockedLogicalPlan(solveds, cardinalities, "a")
    val rhsSelection = Selection(Seq(pred1), rhsLeaf)
    val join = NodeHashJoin(Set("a"), lhsSelection, rhsSelection)

    // When rewritten
    val result = join.endoRewrite(predicateRemovalThroughJoins(solveds, cardinalities, Attributes(idGen)))

    // Then nothing is removed
    result should equal(join)
  }

  private def selectionOp(id: String, solveds: Solveds, cardinalities: Cardinalities, predicates: Expression*) = {
    val selections = Selections.from(predicates)
    val lhsLeaf = newMockedLogicalPlan("a")
    val solved = PlannerQuery.empty.withQueryGraph(QueryGraph(selections = selections))
    val c = Cardinality(0)
    val res = Selection(Seq(aHasLabel), lhsLeaf)
    solveds.set(res.id, solved)
    cardinalities.set(res.id, c)
    res
  }
}
