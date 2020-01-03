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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions.{Contains, Equals}
import org.neo4j.cypher.internal.v3_5.util.Cardinality
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class CartesianProductsOrValueJoinsTest
  extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  val planA = allNodesScan("a")
  val planB = allNodesScan("b")
  val planC = allNodesScan("c")

  private def allNodesScan(n: String, planningAttributes: PlanningAttributes = PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders)): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph(patternNodes = Set((n))))
    val cardinality = Cardinality(0)
    val res = AllNodesScan(n, Set.empty)
    planningAttributes.solveds.set(res.id, solved)
    planningAttributes.cardinalities.set(res.id, cardinality)
    planningAttributes.providedOrders.set(res.id, ProvidedOrder.empty)
    res
  }

  test("should plan cartesian product between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b")),
      input = (planningAttributes: PlanningAttributes) => Set(
          PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", planningAttributes)),
          PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", planningAttributes))),
      expectedPlans =
        List(planA, planB).permutations.map { l =>
          val (a, b) = (l.head, l(1))
          CartesianProduct(
            planA,
            planB
          )
        }.toSeq: _*)
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = (planningAttributes: PlanningAttributes) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", planningAttributes)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", planningAttributes)),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), allNodesScan("c", planningAttributes))),
      expectedPlans =
        List(planA, planB, planC).permutations.map { l =>
          val (a, b, c) = (l.head, l(1), l(2))
          CartesianProduct(
            b,
            CartesianProduct(
              a,
              c
            )
          )
        }.toSeq : _*)
  }

  test("should plan cartesian product between lots of pattern nodes") {
    val chars = 'a' to 'z'
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = (planningAttributes: PlanningAttributes) => (chars map { x =>
        PlannedComponent(QueryGraph(patternNodes = Set(x.toString)), allNodesScan(x.toString, planningAttributes))
      }).toSet,
      assertion = (x: LogicalPlan) => {
        val leaves = x.leaves
        leaves.toSet should equal((chars map { x =>
          PlannedComponent(QueryGraph(patternNodes = Set(x.toString)), allNodesScan(x.toString))
        }).map(_.plan).toSet)
        leaves.size should equal(chars.size)
      }
    )
  }

  test("should plan hash join between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(Equals(prop("a", "id"), prop("b", "id"))(pos))),
      input = (planningAttributes: PlanningAttributes) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", planningAttributes)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", planningAttributes))),
      expectedPlans =
        List((planA, "a"), (planB, "b")).permutations.map { l =>
          val ((a, aName), (b, bName)) = (l.head, l(1))
            ValueHashJoin(a, b, Equals(prop(aName, "id"), prop(bName, "id"))(pos))
        }.toSeq : _*)
  }

  test("should plan hash joins between 3 pattern nodes") {
    val eq1 = Equals(prop("b", "id"), prop("a", "id"))(pos)
    val eq2 = Equals(prop("b", "id"), prop("c", "id"))(pos)
    val eq3 = Equals(prop("a", "id"), prop("c", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Seq(eq1, eq2, eq3))),
      input = (planningAttributes: PlanningAttributes) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", planningAttributes)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", planningAttributes)),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), allNodesScan("c", planningAttributes))),
      expectedPlans =
        List((planA, "a"), (planB, "b"), (planC, "c")).permutations.flatMap { l =>
          val ((a, aName), (b, bName), (c, cName)) = (l.head, l(1), l(2))
          // permutate equals order
          List(prop(bName, "id"), prop(cName, "id")).permutations.map { l2 =>
            val (prop1, prop2) = (l2.head, l2(1))
            Selection(Seq(Equals(prop(aName, "id"), prop2)(pos)),
              ValueHashJoin(a,
                ValueHashJoin(b, c, Equals(prop(bName, "id"), prop(cName, "id"))(pos)), Equals(prop(aName, "id"), prop1)(pos)))
          }
        }.toSeq : _*)
  }

  test("should recognize value joins") {
    // given WHERE x.id = z.id
    val lhs = prop("x", "id")
    val rhs = prop("z", "id")
    val equalityComparison = Equals(lhs, rhs)(pos)
    val selections = Selections.from(equalityComparison)

    // when
    val result = cartesianProductsOrValueJoins.valueJoins(Seq(equalityComparison))

    // then
    result should equal(Set(equalityComparison))
  }

  test("if one side is a literal, it's not a value join") {
    // given WHERE x.id = 42
    val equalityComparison = propEquality("x","id", 42)

    // when
    val result = cartesianProductsOrValueJoins.valueJoins(Seq(equalityComparison))

    // then
    result should be(empty)
  }

  test("if both lhs and rhs come from the same variable, it's not a value join") {
    // given WHERE x.id1 = x.id2
    val lhs = prop("x", "id1")
    val rhs = prop("x", "id2")
    val equalityComparison = Equals(lhs, rhs)(pos)
    val selections = Selections.from(equalityComparison)

    // when
    val result = cartesianProductsOrValueJoins.valueJoins(Seq(equalityComparison))

    // then
    result should be(empty)
  }

  test("combination of predicates is not a problem") {
    // given WHERE x.id1 = z.id AND x.id1 = x.id2 AND x.id2 = 42
    val x_id1 = prop("x", "id1")
    val x_id2 = prop("x", "id2")
    val z_id = prop("z", "id")
    val lit = literalInt(42)

    val pred1 = Equals(x_id1, x_id2)(pos)
    val pred2 = Equals(x_id1, z_id)(pos)
    val pred3 = Equals(x_id2, lit)(pos)

    // when
    val result = cartesianProductsOrValueJoins.valueJoins(Seq(pred1, pred2, pred3))

    // then
    result should be(Set(pred2))
  }

  test("find predicates that depend on two different qgs is possible") {
    // given WHERE n.prop CONTAINS x.prop
    val nProp = prop("n", "prop")
    val xProp = prop("x", "prop")

    val predicate1 = Contains(nProp, xProp)(pos) -> Array("n", "x")
    val predicate2 = propEquality("n", "prop", 42) -> Array("n")

    val idsFromLeft = Set("n")
    val idsFromRight = Set("x")

    // when
    val result = cartesianProductsOrValueJoins.predicatesDependendingOnBothSides(Array(predicate1, predicate2), idsFromLeft, idsFromRight)

    // then
    result should be(List(predicate1._1))
  }

  private def testThis(graph: QueryGraph, input: (PlanningAttributes) => Set[PlannedComponent], assertion: LogicalPlan => Unit): Unit = {
    new given {
      qg = graph
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 3000.0
        case _ => 100.0
      }
    }.withLogicalPlanningContext { (cfg, context) =>
      val kit = context.config.toKit(InterestingOrder.empty, context)

      var plans: Set[PlannedComponent] = input(context.planningAttributes)
      while (plans.size > 1) {
        plans = cartesianProductsOrValueJoins(plans, cfg.qg, InterestingOrder.empty, context, kit, SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]))
      }

      val result = plans.head.plan

      assertion(result)
    }
  }

  private def testThis(graph: QueryGraph, input: (PlanningAttributes) => Set[PlannedComponent], expectedPlans: LogicalPlan*): Unit =
    testThis(graph, input, (result: LogicalPlan) => expectedPlans should contain(result))
}
