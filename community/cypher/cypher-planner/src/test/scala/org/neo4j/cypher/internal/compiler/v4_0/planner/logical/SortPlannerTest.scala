package org.neo4j.cypher.internal.compiler.v4_0.planner.logical

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.expressions.{Add, Multiply, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SortPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  test("should return None for an already sorted plan") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularPlannerQuery(interestingOrder = io))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc("x.foo"))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None for a plan with order from index") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sorted from index
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc("x.foo"))

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return sorted plan when needed") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map("x.foo" -> prop("x", "foo"))), Seq(Ascending("x.foo")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularPlannerQuery(interestingOrder = io))
    }
  }

  test("should return sorted plan when needed for renamed property") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a"), Map("a" -> prop("x", "foo"))))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map("a" -> prop("x", "foo"))), Seq(Ascending("a")))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(
        RegularPlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("a" -> prop("x", "foo")))))
    }
  }

  test("should return sorted plan when needed for expression") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = Multiply(SignedDecimalIntegerLiteral("2")_, Add(SignedDecimalIntegerLiteral("42")_, prop("x", "foo"))_)_
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(Some(Sort(Projection(inputPlan, Map(sortOn.asCanonicalStringVal -> sortOn)), Seq(Ascending(sortOn.asCanonicalStringVal)))))
      context.planningAttributes.solveds.get(sortedPlan.get.id) should equal(RegularPlannerQuery(interestingOrder = io))
    }
  }

  test("should return None when unable to solve the required order") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should return None when no required order") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.maybeSortedPlan(inputPlan, io, context)

      // Then
      sortedPlan should equal(None)
    }
  }

  test("should do nothing to an already sorted plan") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sort the plan
      context.planningAttributes.solveds.set(inputPlan.id, RegularPlannerQuery(interestingOrder = io))
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc("x.foo"))

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(context.planningAttributes.solveds.get(inputPlan.id))
    }
  }

  test("should do nothing to a plan but update solved with order from index") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Fake sorted from index
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)
      context.planningAttributes.providedOrders.set(inputPlan.id, ProvidedOrder.asc("x.foo"))

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularPlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(Sort(Projection(inputPlan, Map("x.foo" -> prop("x", "foo"))), Seq(Ascending("x.foo"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularPlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed for expression") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = Add(prop("x", "foo"), SignedDecimalIntegerLiteral("42")_)_
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(sortOn))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(
        Sort(
          Projection(inputPlan, Map(sortOn.asCanonicalStringVal -> sortOn)),
          Seq(Ascending(sortOn.asCanonicalStringVal))
        )
      )
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(RegularPlannerQuery(interestingOrder = io))
    }
  }

  test("should sort when needed for renamed expression") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val sortOn = Add(prop("x", "foo"), SignedDecimalIntegerLiteral("42")_)_
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("add"), Map("add" -> sortOn)))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(Sort(Projection(inputPlan, Map("add" -> sortOn)), Seq(Ascending("add"))))
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(
        RegularPlannerQuery(interestingOrder = io, horizon = RegularQueryProjection(Map("add" -> sortOn))))
    }
  }

  test("should give AssertionError when unable to solve the required order") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

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
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val io = InterestingOrder.empty
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")
      // Unsorted
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val sortedPlan = SortPlanner.ensureSortedPlanWithSolved(inputPlan, io, context)

      // Then
      sortedPlan should equal(inputPlan)
      context.planningAttributes.solveds.get(sortedPlan.id) should equal(context.planningAttributes.solveds.get(inputPlan.id))
    }
  }
}
