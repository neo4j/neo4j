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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.SatisfiedForPlan
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.planIfAsSortedAsPossible
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SortPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should return the same plan for an already sorted plan") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = io)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(inputPlan))
    }
  }

  test("should return the same plan for a plan with order from index") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      // Fake sorted from index
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(inputPlan))
    }
  }

  test("should return sorted plan when needed") {
    // [WITH x] WITH x AS x ORDER BY x.foo
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(Sort(
        Projection(inputPlan, Map(varFor("x.foo") -> prop("x", "foo"))),
        Seq(Ascending(varFor("x.foo")))
      )))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should copy projections from interesting order to provided order: aliased sort expression") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate
        .asc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo")))
        .desc(varFor("yprop"), Map("yprop" -> prop("y", "prop"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      context.staticComponents.planningAttributes.providedOrders.get(sortedPlan.get.id) should equal(ProvidedOrder
        .asc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo")))
        .desc(varFor("yprop"), Map("yprop" -> prop("y", "prop"))))
    }
  }

  test("should copy projections from interesting order to provided order: unaliased sort expression") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("x", "foo"), Map("x" -> varFor("xx")))
        .desc(prop("y", "prop"), Map("y" -> varFor("yy"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      context.staticComponents.planningAttributes.providedOrders.get(sortedPlan.get.id) should equal(ProvidedOrder
        .asc(prop("x", "foo"), Map("x" -> varFor("xx")))
        .desc(prop("y", "prop"), Map("y" -> varFor("yy"))))
    }
  }

  test("should do a partial sort if things are partially in the right order already") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n")).asc(varFor("m")))
      val solvedIO = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "n", "m")
      // Fake sort the plan
      context.staticComponents.planningAttributes.solveds.set(
        inputPlan.id,
        RegularSinglePlannerQuery(interestingOrder = solvedIO)
      )
      context.staticComponents.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(varFor("n")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(PartialSort(inputPlan, Seq(Ascending(varFor("n"))), Seq(Ascending(varFor("m"))))))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should do a partial sort and projection if things are partially in the right order already") {
    new given().withLogicalPlanningContext { (_, context) =>
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
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(PartialSort(
        Projection(inputPlan, Map(varFor("x.foo") -> prop("x", "foo"), varFor("x.bar") -> prop("x", "bar"))),
        Seq(Ascending(varFor("x.foo"))),
        Seq(Ascending(varFor("x.bar")))
      )))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should return sorted plan when needed for renamed property") {
    // [WITH x] WITH x.foo AS a ORDER BY a
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a"), Map("a" -> prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(Sort(
        Projection(inputPlan, Map(varFor("a") -> prop("x", "foo"))),
        Seq(Ascending(varFor("a")))
      )))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("a" -> prop("x", "foo"))))
      )
    }
  }

  test("SatisfiedForPlan should return false when both satisfied prefix and missing suffix are empty") {
    val plan = fakeLogicalPlanFor("x")
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(Seq.empty, Seq.empty))

    asSortedAsPossible shouldBe false
  }

  test("SatisfiedForPlan should return false when satisfied prefix is empty") {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrder = Seq(Asc(varFor("x")))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(Seq.empty, interestingOrder))

    asSortedAsPossible shouldBe false
  }

  test("SatisfiedForPlan should return true when satisfied prefix is non empty and missing suffix is empty") {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrder = Seq(Asc(varFor("x")))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrder, Seq.empty))

    asSortedAsPossible shouldBe true
  }

  test(
    "SatisfiedForPlan should return false when both prefix and suffix are non empty and all suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x", "y")
    val interestingOrderOnX = Seq(Asc(varFor("x")))
    val interestingOrderOnY = Seq(Asc(varFor("y")))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnY))

    asSortedAsPossible shouldBe false
  }

  test(
    "SatisfiedForPlan should return true when both prefix and suffix are non empty and only some suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x", "y")
    val interestingOrderOnX = Seq(Asc(varFor("x")))
    val interestingOrderOnYZ = Seq(Asc(varFor("y")), Asc(varFor("z")))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnYZ))

    asSortedAsPossible shouldBe true
  }

  test(
    "SatisfiedForPlan should return true when both prefix and suffix are non empty and no suffix symbols are available"
  ) {
    val plan = fakeLogicalPlanFor("x")
    val interestingOrderOnX = Seq(Asc(varFor("x")))
    val interestingOrderOnY = Seq(Asc(varFor("y")))
    val asSortedAsPossible = SatisfiedForPlan(plan).unapply(Satisfaction(interestingOrderOnX, interestingOrderOnY))

    asSortedAsPossible shouldBe true
  }

  test("planIfAsSortedAsPossible should sort if possible") {
    val sortExpr = prop("x", "foo")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpr))

    new given().withLogicalPlanningContext { (_, context) =>
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

    new given().withLogicalPlanningContext { (_, context) =>
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

    new given().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(sortExprX))

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(Some(plan))
    }
  }

  test("planIfAsSortedAsPossible should plan partial sort, when non-sorted columns dependencies in available symbols") {
    val sortExprX = varFor("x")
    val sortExprY = varFor("y")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX).asc(sortExprY))

    new given().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(sortExprX))

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(Some(
        PartialSort(plan, Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))))
      ))
    }
  }

  test("planIfAsSortedAsPossible should return None when not possible to sort, plan not partially sorted.") {
    val sortExprX = varFor("x")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX))

    new given().withLogicalPlanningContext { (_, context) =>
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
    val sortExprX = varFor("x")
    val sortExprY = varFor("y")
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExprX).asc(sortExprY))

    new given().withLogicalPlanningContext { (_, context) =>
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = planIfAsSortedAsPossible(plan, InterestingOrderConfig(io), context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return sorted plan when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY 2 * (42 + x.foo)
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = multiply(literalInt(2), add(literalInt(42), prop("x", "foo")))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Some(Sort(
        Projection(inputPlan, Map(varFor("2 * (42 + x.foo)") -> sortOn)),
        Seq(Ascending(varFor("2 * (42 + x.foo)")))
      )))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io)
      )
    }
  }

  test("should return None when unable to solve the required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None when no required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should do nothing to an already sorted plan") {
    new given().withLogicalPlanningContext { (_, context) =>
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
    new given().withLogicalPlanningContext { (_, context) =>
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
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Sort(
        Projection(inputPlan, Map(varFor("x.foo") -> prop("x", "foo"))),
        Seq(Ascending(varFor("x.foo")))
      ))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder =
          io
        )
      )
    }
  }

  test("should sort without pre-projection if things are already projected in previous horizon") {
    // [WITH n, m] WITH n AS n ORDER BY m
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("m")))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "m", "n")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Sort(inputPlan, Seq(Ascending(varFor("m")))))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder =
          io
        )
      )
    }
  }

  test("should sort when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY x.foo + 42
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(
        Sort(
          Projection(inputPlan, Map(varFor(sortOn.asCanonicalStringVal) -> sortOn)),
          Seq(Ascending(varFor(sortOn.asCanonicalStringVal)))
        )
      )
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder =
          io
        )
      )
    }
  }

  test("should sort when needed for renamed expression") {
    // [WITH x] WITH x.foo + 42 AS add ORDER BY add
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("add"), Map("add" -> sortOn)))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan should equal(Sort(
        Projection(inputPlan, Map(varFor("add") -> sortOn)),
        Seq(Ascending(varFor("add")))
      ))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("add" -> sortOn)))
      )
    }
  }

  test("should sort and two step pre-projection for expressions") {
    // [WITH n] WITH n + 10 AS m ORDER BY m + 5 ASCENDING
    val mExpr = add(varFor("n"), literalInt(10))
    val sortExpression = add(varFor("m"), literalInt(5))

    new given().withLogicalPlanningContext { (_, context) =>
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "n")
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpression, Map("m" -> mExpr)))

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val projection1 = Projection(inputPlan, Map(varFor("m") -> mExpr))
      val projection2 = Projection(projection1, Map(varFor("m + 5") -> sortExpression))
      sortedPlan should equal(Sort(projection2, Seq(Ascending(varFor("m + 5")))))
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("m" -> mExpr)))
      )
    }
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    // [WITH p] WITH p, p.born IS NOT NULL AS bday ORDER BY p.name, bday
    val bdayExp = isNotNull(prop("p", "born"))

    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("p", "name")).asc(
        varFor("bday"),
        Map("bday" -> bdayExp)
      ))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "p")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val projection1 = Projection(inputPlan, Map(varFor("bday") -> bdayExp))
      val projection2 = Projection(projection1, Map(varFor("p.name") -> prop("p", "name")))
      val sorted = Sort(projection2, Seq(Ascending(varFor("p.name")), Ascending(varFor("bday"))))
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("bday" -> bdayExp)))
      )
    }
  }

  test("should sort first aliased and then unaliased columns in the right order") {
    // [WITH p] WITH p, p.born IS NOT NULL AS bday ORDER BY bday, p.name
    val bdayExp = isNotNull(prop("p", "born"))

    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("bday"), Map("bday" -> bdayExp)).asc(prop(
        "p",
        "name"
      )))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "p")

      // When
      val sortedPlan =
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      val projection1 = Projection(inputPlan, Map(varFor("bday") -> bdayExp))
      val projection2 = Projection(projection1, Map(varFor("p.name") -> prop("p", "name")))
      val sorted = Sort(projection2, Seq(Ascending(varFor("bday")), Ascending(varFor("p.name"))))
      sortedPlan should equal(sorted)
      context.staticComponents.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("bday" -> bdayExp)))
      )
    }
  }

  test("should give AssertionError when unable to solve the required order") {
    new given().withLogicalPlanningContext { (_, context) =>
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
    new given().withLogicalPlanningContext { (_, context) =>
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

  test("should return None if required sort is an aggregation function") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(count(prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, InterestingOrderConfig(io), context, updateSolved = true)

      // Then
      sortedPlan shouldBe None
    }
  }
}
