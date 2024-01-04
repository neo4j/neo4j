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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PredicateRemovalThroughJoinsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val aHasLabel = hasLabels("a", "LABEL")
  private val pred = equals(literalInt(42), literalInt(42))

  test("same predicate on both sides - Selection is removed entirely") {
    // Given
    val planningAttributes = PlanningAttributes.newAttributes
    val lhsSelection = selectionOp("a", planningAttributes, aHasLabel)
    val rhsLeaf = newMockedLogicalPlan(planningAttributes, "a")
    val rhsSelection = Selection(Seq(aHasLabel), rhsLeaf)
    val join = NodeHashJoin(Set(v"a"), lhsSelection, rhsSelection)

    // When
    val result = join.endoRewrite(predicateRemovalThroughJoins(
      planningAttributes.solveds,
      planningAttributes.cardinalities,
      Attributes(idGen, planningAttributes.providedOrders)
    ))

    // Then the Selection operator is removed from the RHS
    result should equal(NodeHashJoin(Set(v"a"), lhsSelection, rhsLeaf))
  }

  test("multiple predicates on both sides - only one is common on both sides and is removed") {
    // Given
    val predEquals = equals(literalInt(44), literalInt(44))
    val planningAttributes = PlanningAttributes.newAttributes
    val lhsSelection = selectionOp("a", planningAttributes, aHasLabel, pred)
    val rhsLeaf = newMockedLogicalPlan(planningAttributes, "a")
    val rhsSelection = Selection(Seq(aHasLabel, predEquals), rhsLeaf)
    val join = NodeHashJoin(Set(v"a"), lhsSelection, rhsSelection)

    // When rewritten
    val result = join.endoRewrite(predicateRemovalThroughJoins(
      planningAttributes.solveds,
      planningAttributes.cardinalities,
      Attributes(idGen, planningAttributes.providedOrders)
    ))

    // Then the predicate is removed from the RHS selection operator
    val newRhsSelection = Selection(Seq(predEquals), rhsLeaf)

    result should equal(
      NodeHashJoin(Set(v"a"), lhsSelection, newRhsSelection)
    )
  }

  test("same predicate on both sides, but not depending on the join ids - nothing should be removed") {
    // Given
    val planningAttributes = PlanningAttributes.newAttributes
    val lhsSelection = selectionOp("a", planningAttributes, pred)
    val rhsLeaf = newMockedLogicalPlan(planningAttributes, "a")
    val rhsSelection = Selection(Seq(pred), rhsLeaf)
    val join = NodeHashJoin(Set(v"a"), lhsSelection, rhsSelection)

    // When rewritten
    val result = join.endoRewrite(predicateRemovalThroughJoins(
      planningAttributes.solveds,
      planningAttributes.cardinalities,
      Attributes(idGen, planningAttributes.providedOrders)
    ))

    // Then nothing is removed
    result should equal(join)
  }

  private def selectionOp(id: String, planningAttributes: PlanningAttributes, predicates: Expression*) = {
    val selections = Selections.from(predicates)
    val lhsLeaf = newMockedLogicalPlan("a")
    val solved = SinglePlannerQuery.empty.withQueryGraph(QueryGraph(selections = selections))
    val c = Cardinality(0)
    val res = Selection(Seq(aHasLabel), lhsLeaf)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, c)
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }
}
