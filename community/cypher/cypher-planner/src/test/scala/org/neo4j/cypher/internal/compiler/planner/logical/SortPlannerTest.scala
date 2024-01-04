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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.SatisfiedForPlan
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.planIfAsSortedAsPossible
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.functions.Rand
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SortPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  // ---------------
  // maybeSortedPlan
  // ---------------

  test("should return the same plan for an already sorted plan") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = io)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan should equal(Some(inputPlan))
    }
  }

  test("should return the same plan for a plan with order from index") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sorted from index
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan should equal(Some(inputPlan))
    }
  }

  test("should return sorted plan when needed") {
    // [WITH x] WITH x AS x ORDER BY x.foo
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  x.foo@0` ASC")
        .projection("x.foo AS `  x.foo@0`")
        .fakeLeafPlan("x")
        .build()

      sortedPlan should equal(Some(sorted))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should copy projections from interesting order to provided order: aliased sort expression") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate
        .asc(v"xfoo", Map(v"xfoo" -> prop("x", "foo")))
        .desc(v"yprop", Map(v"yprop" -> prop("y", "prop"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      context.staticComponents.planningAttributes.providedOrders.get(sortedPlan.get.id) should equal(ProvidedOrder
        .asc(v"xfoo", Map(v"xfoo" -> prop("x", "foo")))
        .desc(v"yprop", Map(v"yprop" -> prop("y", "prop"))))
    }
  }

  test("should copy projections from interesting order to provided order: unaliased sort expression") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("x", "foo"), Map(v"x" -> v"xx"))
        .desc(prop("y", "prop"), Map(v"y" -> v"yy")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      context.staticComponents.planningAttributes.providedOrders.get(sortedPlan.get.id) should equal(ProvidedOrder
        .asc(prop("x", "foo"), Map(v"x" -> v"xx"))
        .desc(prop("y", "prop"), Map(v"y" -> v"yy")))
    }
  }

  test("should do a partial sort if things are partially in the right order already") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(v"n").asc(v"m"))
      val solvedIO = InterestingOrder.required(RequiredOrderCandidate.asc(v"n"))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "n", "m")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = solvedIO)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(v"n"))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan should equal(Some(PartialSort(inputPlan, Seq(Ascending(v"n")), Seq(Ascending(v"m")))))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should do a partial sort and projection if things are partially in the right order already") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("x", "bar")))
      val solvedIO = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = solvedIO)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .partialSort(Seq("`  x.foo@0` ASC"), Seq("`  x.bar@1` ASC"))
        .projection("x.foo AS `  x.foo@0`", "x.bar AS `  x.bar@1`")
        .fakeLeafPlan("x")
        .build()

      sortedPlan should equal(Some(sorted))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should return sorted plan when needed for renamed property") {
    // [WITH x] WITH x.foo AS a ORDER BY a
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(v"a", Map(v"a" -> prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("a ASC")
        .projection("x.foo AS a")
        .fakeLeafPlan("x")
        .build()

      sortedPlan should equal(Some(sorted))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(
          interestingOrder = io,
          horizon = RegularQueryProjection(Map(v"a" -> prop("x", "foo")))
        )
      )
    }
  }

  test("should return sorted plan when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY 2 * (42 + x.foo)
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val sortOn = multiply(literalInt(2), add(literalInt(42), prop("x", "foo")))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  2 * (42 + x.foo)@0` ASC")
        .projection("2 * (42 + x.foo) AS `  2 * (42 + x.foo)@0`")
        .fakeLeafPlan("x")
        .build()

      sortedPlan should equal(Some(sorted))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should return None when unable to solve the required order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None when no required order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None if required sort is an aggregation function") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(count(prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan shouldBe None
    }
  }

  test("should return None if required sort is a non-deterministic function and we would be pushing down Sort") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(function(Rand.name)))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = true,
        context,
        updateSolved = true
      )

      // Then
      sortedPlan shouldBe None
    }
  }

  test("should return sorted plan if required sort is a non-deterministic function and we are not pushing down Sort") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(function(Rand.name)))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(
        inputPlan,
        InterestingOrderConfig(io),
        isPushDownSort = false,
        context,
        updateSolved = true
      )

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  rand()@0` ASC")
        .projection("rand() AS `  rand()@0`")
        .fakeLeafPlan("x")
        .build()
      sortedPlan should equal(Some(sorted))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  // ----------------
  // SatisfiedForPlan
  // ----------------

  test("SatisfiedForPlan should return false when both satisfied prefix and missing suffix are empty") {
    val plan = fakeLogicalPlanFor("x")
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(Seq.empty, Seq.empty))

    asSortedAsPossible shouldBe false
  }

  test("SatisfiedForPlan should return false when satisfied prefix is empty") {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrder = Seq(Asc(v"x"))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(Seq.empty, interestingOrder))

    asSortedAsPossible shouldBe false
  }

  test("SatisfiedForPlan should return true when satisfied prefix is non empty and missing suffix is empty") {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrder = Seq(Asc(v"x"))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrder, Seq.empty))

    asSortedAsPossible shouldBe true
  }

  test(
    "SatisfiedForPlan should return false when both prefix and suffix are non empty and all suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x", "y")
    val interestingOrderOnX = Seq(Asc(v"x"))
    val interestingOrderOnY = Seq(Asc(v"y"))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnY))

    asSortedAsPossible shouldBe false
  }

  test(
    "SatisfiedForPlan should return true when both prefix and suffix are non empty and only some suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x", "y")
    val interestingOrderOnX = Seq(Asc(v"x"))
    val interestingOrderOnYZ = Seq(Asc(v"y"), Asc(v"z"))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnYZ))

    asSortedAsPossible shouldBe true
  }

  test(
    "SatisfiedForPlan should return true when both prefix and suffix are non empty and no suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrderOnX = Seq(Asc(v"x"))
    val interestingOrderOnY = Seq(Asc(v"y"))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnY))

    asSortedAsPossible shouldBe true
  }

  // ------------------------
  // planIfAsSortedAsPossible
  // ------------------------

  test("planIfAsSortedAsPossible should sort if possible") {
    val sortExpr = prop("x", "foo")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpr))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan.get shouldBe a[Sort]
    }
  }

  test("planIfAsSortedAsPossible should return sorted plan if already sorted") {
    val sortExpr = prop("x", "foo")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpr))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(sortExpr))

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(Some(plan))
    }
  }

  test(
    "planIfAsSortedAsPossible should return plan if partially sorted, but non-sorted columns not in available symbols"
  ) {
    val sortExprX = prop("x", "foo")
    val sortExprY = prop("y", "foo")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX).asc(sortExprY))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(sortExprX))

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(Some(plan))
    }
  }

  test("planIfAsSortedAsPossible should plan partial sort, when non-sorted columns dependencies in available symbols") {
    val sortExprX = v"x"
    val sortExprY = v"y"
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX).asc(sortExprY))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(sortExprX))

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(Some(
        PartialSort(plan, Seq(Ascending(v"x")), Seq(Ascending(v"y")))
      ))
    }
  }

  test("planIfAsSortedAsPossible should return None when not possible to sort, plan not partially sorted.") {
    val sortExprX = v"x"
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "y")

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test(
    "planIfAsSortedAsPossible should return None when not possible to sort, plan not partially sorted but same prefix."
  ) {
    val sortExprX = v"x"
    val sortExprY = v"y"
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX).asc(sortExprY))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(None)
    }
  }

  // --------------------------
  // ensureSortedPlanWithSolved
  // --------------------------

  test("should do nothing to an already sorted plan") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = io)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(inputPlan)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        context.staticComponents.planningAttributes.solveds.get(inputPlan.id)
      )
    }
  }

  test("should do nothing to a plan but update solved with order from index") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sorted from index
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(inputPlan)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder =
          io
        )
      )
    }
  }

  test("should sort when needed") {
    // [WITH x] WITH x AS x ORDER BY x.foo
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  x.foo@0` ASC")
        .projection("x.foo AS `  x.foo@0`")
        .fakeLeafPlan("x")
        .build()

      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should sort without pre-projection if things are already projected in previous horizon") {
    // [WITH n, m] WITH n AS n ORDER BY m
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(v"m"))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "m", "n")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("m ASC")
        .fakeLeafPlan("m", "n")
        .build()
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should sort when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY x.foo + 42
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  x.foo + 42@0` ASC")
        .projection("x.foo + 42 AS `  x.foo + 42@0`")
        .fakeLeafPlan("x")
        .build()
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should sort when needed for renamed expression") {
    // [WITH x] WITH x.foo + 42 AS add ORDER BY add
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(v"add", Map(v"add" -> sortOn)))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("add ASC")
        .projection("x.foo + 42 AS add")
        .fakeLeafPlan("x")
        .build()
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map(v"add" -> sortOn)))
      )
    }
  }

  test("should sort and two step pre-projection for expressions") {
    // [WITH n] WITH n + 10 AS m ORDER BY m + 5 ASCENDING
    val mExpr = add(v"n", literalInt(10))
    val sortExpression = add(v"m", literalInt(5))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "n")
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpression, Map(v"m" -> mExpr)))

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  m + 5@0` ASC")
        .projection("m + 5 AS `  m + 5@0`")
        .projection("n + 10 AS m")
        .fakeLeafPlan("n")
        .build()
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map(v"m" -> mExpr)))
      )
    }
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    // [WITH p] WITH p, p.born IS NOT NULL AS bday ORDER BY p.name, bday
    val bdayExp = isNotNull(prop("p", "born"))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("p", "name")).asc(
        v"bday",
        Map(v"bday" -> bdayExp)
      ))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "p")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("`  p.name@0` ASC", "bday ASC")
        .projection("p.name AS `  p.name@0`")
        .projection("p.born IS NOT NULL AS bday")
        .fakeLeafPlan("p")
        .build()
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map(v"bday" -> bdayExp)))
      )
    }
  }

  test("should sort first aliased and then unaliased columns in the right order") {
    // [WITH p] WITH p, p.born IS NOT NULL AS bday ORDER BY bday, p.name
    val bdayExp = isNotNull(prop("p", "born"))

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(v"bday", Map(v"bday" -> bdayExp)).asc(prop(
        "p",
        "name"
      )))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "p")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val sorted = new LogicalPlanBuilder(wholePlan = false)
        .sort("bday ASC", "`  p.name@0` ASC")
        .projection("p.name AS `  p.name@0`")
        .projection("p.born IS NOT NULL AS bday")
        .fakeLeafPlan("p")
        .build()

      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map(v"bday" -> bdayExp)))
      )
    }
  }

  test("should give AssertionError when unable to solve the required order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes)

      try {
        // When
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)
      } catch {
        // Then
        case e: AssertionError => assert(e.getMessage.startsWith("Expected a sorted plan but got"))
      }
    }
  }

  test("should do nothing when no required order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(inputPlan)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        context.staticComponents.planningAttributes.solveds.get(inputPlan.id)
      )
    }
  }
}
