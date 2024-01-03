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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LeafPlanSelectorHeuristicTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val nodeByLabelScan: LogicalPlanBuilder => LogicalPlanBuilder =
    _.nodeByLabelScan("x", "Label")

  private val nodeIndexScan: LogicalPlanBuilder => LogicalPlanBuilder =
    _.nodeIndexOperator("x:Label(prop)")

  private val relationshipTypeScan: LogicalPlanBuilder => LogicalPlanBuilder =
    _.relationshipTypeScan("(a)-[x:REL]->(b)")

  private val relationshipIndexScan: LogicalPlanBuilder => LogicalPlanBuilder =
    _.relationshipIndexOperator("(a)-[x:REL(prop)]-(b)")

  private def tiedLeafPlans = Seq(
    (nodeByLabelScan, nodeIndexScan),
    (relationshipTypeScan, relationshipIndexScan)
  )

  private def selectionOnTopOf(appendLeafPlan: LogicalPlanBuilder => LogicalPlanBuilder): LogicalPlan = {
    val selectionPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter("id(x) > 123")

    appendLeafPlan(selectionPlan).build()
  }

  private def semiApplyOnTopOf(appendLeafPlan: LogicalPlanBuilder => LogicalPlanBuilder): LogicalPlan = {
    val semiApplyPlan = new LogicalPlanBuilder(wholePlan = false)
      .semiApply()
      .|.filter("id(x) > 123")
      .|.argument("x")

    appendLeafPlan(semiApplyPlan).build()
  }

  private def antiSemiApplyOnTopOf(appendLeafPlan: LogicalPlanBuilder => LogicalPlanBuilder): LogicalPlan = {
    val antiSemiApplyPlan = new LogicalPlanBuilder(wholePlan = false)
      .antiSemiApply()
      .|.filter("id(x) > 123")
      .|.argument("x")

    appendLeafPlan(antiSemiApplyPlan).build()
  }

  private def noPredicateOnTopOf(appendLeafPlan: LogicalPlanBuilder => LogicalPlanBuilder): LogicalPlan = {
    appendLeafPlan(new LogicalPlanBuilder(wholePlan = false)).build()
  }

  private def predicatesToPlaceOnTop = Seq(
    selectionOnTopOf _,
    semiApplyOnTopOf _,
    antiSemiApplyOnTopOf _,
    noPredicateOnTopOf _
  )

  private def newContext(): LogicalPlanningContext = {
    newMockedLogicalPlanningContext(newMockedPlanContext())
  }

  private def newContextWithPropertyAccess(): LogicalPlanningContext = {
    newContext().withModifiedPlannerState(_.withAccessedProperties(Set(PropertyAccess(v"x", "prop"))))
  }

  private def newContextWithAggregation(): LogicalPlanningContext = {
    newContext().withModifiedPlannerState(_.withAggregationProperties(Set(PropertyAccess(v"x", "prop"))))
  }

  test("should prefer lookup index scan without property access") {
    val heuristic = new LeafPlanSelectorHeuristic(newContext())
    for {
      predicateOnTopOf <- predicatesToPlaceOnTop
      (lookupIndexLeaf, propertyIndexLeaf) <- tiedLeafPlans
    } {
      val lookupIndexScan = predicateOnTopOf(lookupIndexLeaf)
      val propertyIndexScan = predicateOnTopOf(propertyIndexLeaf)

      withClue(s"$lookupIndexScan\n\n$propertyIndexScan\n\n") {
        heuristic.tieBreaker(lookupIndexScan) should be > heuristic.tieBreaker(propertyIndexScan)
      }
    }
  }

  test("should prefer property index scan with property access") {
    for {
      predicateOnTopOf <- predicatesToPlaceOnTop
      (lookupIndexLeaf, propertyIndexLeaf) <- tiedLeafPlans
      (context, contextDesc) <- Seq(
        newContextWithPropertyAccess() -> "context with property access",
        newContextWithAggregation() -> "context with aggregation"
      )
    } {
      val lookupIndexScan = predicateOnTopOf(lookupIndexLeaf)
      val propertyIndexScan = predicateOnTopOf(propertyIndexLeaf)

      val heuristic = new LeafPlanSelectorHeuristic(context)

      withClue(s"$contextDesc\n\n$lookupIndexScan\n\n$propertyIndexScan\n\n") {
        heuristic.tieBreaker(propertyIndexScan) should be > heuristic.tieBreaker(lookupIndexScan)
      }
    }
  }
}
