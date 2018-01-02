/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, NodeHashJoin, Selection}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, LogicalPlanningTestSupport, PlannerQuery, QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PredicateRemovalThroughJoinsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val aHasLabel = identHasLabel("a", "LABEL")
  val rhsLeaf = newMockedLogicalPlan("a")
  val pred1: Expression = Equals(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("42")_)_
  val pred2: Expression = Equals(SignedDecimalIntegerLiteral("44")_, SignedDecimalIntegerLiteral("44")_)_

  test("same predicate on both sides - Selection is removed entirely") {
    // Given
    val lhsSelection = selectionOp("a", aHasLabel)
    val rhsSelection = Selection(Seq(aHasLabel), rhsLeaf)(solved)
    val join = NodeHashJoin(Set(IdName("a")), lhsSelection, rhsSelection)(solved)

    // When
    val result = join.endoRewrite(predicateRemovalThroughJoins)

    // Then the Selection operator is removed from the RHS
    result should equal(NodeHashJoin(Set(IdName("a")), lhsSelection, rhsLeaf)(solved))
  }

  test("multiple predicates on both sides - only one is common on both sides and is removed") {
    // Given
    val lhsSelection = selectionOp("a", aHasLabel, pred1)
    val rhsSelection = Selection(Seq(aHasLabel, pred2), rhsLeaf)(solved)
    val join = NodeHashJoin(Set(IdName("a")), lhsSelection, rhsSelection)(solved)

    // When rewritten
    val result = join.endoRewrite(predicateRemovalThroughJoins)

    // Then the predicate is removed from the RHS selection operator
    val newRhsSelection = Selection(Seq(pred2), rhsLeaf)(solved)

    result should equal(
      NodeHashJoin(Set(IdName("a")), lhsSelection, newRhsSelection)(solved))
  }

  test("same predicate on both sides, but not depending on the join ids - nothing should be removed") {
    // Given
    val lhsSelection = selectionOp("a", pred1)
    val rhsSelection = Selection(Seq(pred1), rhsLeaf)(solved)
    val originalJoin = NodeHashJoin(Set(IdName("a")), lhsSelection, rhsSelection)(solved)

    // When rewritten
    val result = originalJoin.endoRewrite(predicateRemovalThroughJoins)

    // Then nothing is removed
    result should equal(originalJoin)
  }

  private def selectionOp(id: String, predicates: Expression*) = {
    val selections = Selections.from(predicates: _*)
    val lhsLeaf = newMockedLogicalPlan("a")
    val solved: PlannerQuery = PlannerQuery.empty.withGraph(QueryGraph(selections = selections))
    Selection(Seq(aHasLabel), lhsLeaf)(CardinalityEstimation.lift(solved, Cardinality(0)))
  }
}
