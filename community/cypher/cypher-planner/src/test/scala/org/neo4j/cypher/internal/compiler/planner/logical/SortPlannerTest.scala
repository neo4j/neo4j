/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SortPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should return None for an already sorted plan") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularSinglePlannerQuery(interestingOrder = io))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None for a plan with order from index") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sorted from index
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return sorted plan when needed") {
    // [WITH x] WITH x AS x ORDER BY x.foo
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map("x.foo" -> prop("x", "foo"))), Seq(Ascending("x.foo")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should do a partial sort if things are partially in the right order already") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n")).asc(varFor("m")))
      val solvedIO = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "n", "m")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularSinglePlannerQuery(interestingOrder = solvedIO))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(varFor("n")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(PartialSort(inputPlan, Seq(Ascending("n")), Seq(Ascending("m")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should do a partial sort and projection if things are partially in the right order already") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("x", "bar")))
      val solvedIO = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularSinglePlannerQuery(interestingOrder = solvedIO))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(PartialSort(Projection(inputPlan, Map("x.foo" -> prop("x", "foo"), "x.bar" -> prop("x", "bar"))), Seq(Ascending("x.foo")), Seq(Ascending("x.bar")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should return sorted plan when needed for renamed property") {
    // [WITH x] WITH x.foo AS a ORDER BY a
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a"), Map("a" -> prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map("a" -> prop("x", "foo"))), Seq(Ascending("a")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("a" -> prop("x", "foo")))))
    }
  }

  test("should return sorted plan when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY 2 * (42 + x.foo)
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = multiply(literalInt(2), add(literalInt(42), prop("x", "foo")))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map(sortOn.asCanonicalStringVal -> sortOn)), Seq(Ascending(sortOn.asCanonicalStringVal)))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should return None when unable to solve the required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None when no required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should do nothing to an already sorted plan") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularSinglePlannerQuery(interestingOrder = io))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(context.planningAttributes.solveds.get(inputPlan.id))
    }
  }

  test("should do nothing to a plan but update solved with order from index") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sorted from index
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc(prop("x", "foo")))

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed") {
    // [WITH x] WITH x AS x ORDER BY x.foo
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(Sort(Projection(inputPlan, Map("x.foo" -> prop("x", "foo"))), Seq(Ascending("x.foo"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should sort without pre-projection if things are already projected in previous horizon") {
    // [WITH n, m] WITH n AS n ORDER BY m
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("m")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "m", "n")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(Sort(inputPlan, Seq(Ascending("m"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed for expression") {
    // [WITH x] WITH x AS x ORDER BY x.foo + 42
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(
        Sort(
          Projection(inputPlan, Map(sortOn.asCanonicalStringVal -> sortOn)),
          Seq(Ascending(sortOn.asCanonicalStringVal))
        )
      )
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularSinglePlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed for renamed expression") {
    // [WITH x] WITH x.foo + 42 AS add ORDER BY add
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = add(prop("x", "foo"), literalInt(42))
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("add"), Map("add" -> sortOn)))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(Sort(Projection(inputPlan, Map("add" -> sortOn)), Seq(Ascending("add"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("add" -> sortOn))))
    }
  }

  test("should sort and two step pre-projection for expressions") {
    // [WITH n] WITH n + 10 AS m ORDER BY m + 5 ASCENDING
    val mExpr = add(varFor("n"), literalInt(10))
    val sortExpression = add(varFor("m"), literalInt(5))

    new given().withLogicalPlanningContext { (_, context) =>
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "n")
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortExpression, Map("m" -> mExpr)))

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      val projection1 = Projection(inputPlan, Map("m" -> mExpr))
      val projection2 = Projection(projection1, Map("m + 5" -> sortExpression))
      sortedPlan should equal(Sort(projection2, Seq(Ascending("m + 5"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("m" -> mExpr))))
    }
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    // [WITH p] WITH p, EXISTS(p.born) AS bday ORDER BY p.name, bday
    val bdayExp = function("exists", prop("p", "born"))

    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("p", "name")).asc(varFor("bday"), Map("bday" -> bdayExp)))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "p")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      val projection1 = Projection(inputPlan, Map("bday" -> bdayExp))
      val projection2 = Projection(projection1, Map("p.name" -> prop("p", "name")))
      val sorted = Sort(projection2, Seq(Ascending("p.name"), Ascending("bday")))
      sortedPlan should equal(sorted)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("bday" -> bdayExp))))
    }
  }

  test("should sort first aliased and then unaliased columns in the right order") {
    // [WITH p] WITH p, EXISTS(p.born) AS bday ORDER BY bday, p.name
    val bdayExp = function("exists", prop("p", "born"))

    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("bday"), Map("bday" -> bdayExp)).asc(prop("p", "name")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "p")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      val projection1 = Projection(inputPlan, Map("bday" -> bdayExp))
      val projection2 = Projection(projection1, Map("p.name" -> prop("p", "name")))
      val sorted = Sort(projection2, Seq(Ascending("bday"), Ascending("p.name")))
      sortedPlan should equal(sorted)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularSinglePlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("bday" -> bdayExp))))
    }
  }

  test("should give AssertionError when unable to solve the required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)

      try {
        // When
        SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)
      } catch {
        // Then
        case e: AssertionError => assert(e.getMessage == "Expected a sorted plan")
      }
    }
  }

  test("should do nothing when no required order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(context.planningAttributes.solveds.get(inputPlan.id))
    }
  }
}
