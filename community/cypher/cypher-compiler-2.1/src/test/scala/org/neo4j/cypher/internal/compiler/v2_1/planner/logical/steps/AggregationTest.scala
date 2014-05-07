/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.functions.{Has, Count, Id, Max}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

class AggregationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("MATCH a RETURN count(*)") {
    val qg = QueryGraph(
      patternNodes = Set("a"),
      aggregatingProjections = Map("c" -> CountStar()(DummyPosition(1)))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val plan = newMockedQueryPlan("a")
    aggregation(plan) should equal(
      QueryPlan(
        Projection(
          Aggregation(
            plan.plan,
            Map.empty,
            Map("  AGGREGATION1" -> CountStar()_)
          ),
          Map("c" -> ident("  AGGREGATION1"))
        ),
        qg
      )
    )
  }

  test("MATCH a RETURN a, max(id(a)) as c") {
    val aggrExp: FunctionInvocation = Max.invoke(Id.invoke(ident("n"))(pos))(DummyPosition(1))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      aggregatingProjections = Map("c" -> aggrExp),
      projections = Map("a" -> ident("a"))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val plan = newMockedQueryPlan("a")
    aggregation(plan) should equal(
      QueryPlan(
        Projection(
          Aggregation(
            plan.plan,
            Map("a" -> ident("a")),
            Map("  AGGREGATION1" -> aggrExp)
          ),
          Map("a" -> ident("a"), "c" -> ident("  AGGREGATION1"))
        ),
        qg
      )
    )
  }

  test("MATCH (n) RETURN n, count(has(n.prop))") {
    val aggregationFunction = Count.invoke(Has.invoke(Property(ident("n"), PropertyKeyName("prop") _) _)(pos))(DummyPosition(1))

    implicit val context = newMockedLogicalPlanContext(
      newMockedPlanContext,
      queryGraph = QueryGraph(
        patternNodes = Set[IdName]("n"),
        aggregatingProjections = Map("c" -> aggregationFunction),
        projections = Map("n" -> ident("n"))
      )
    )

    val plan = newMockedQueryPlan("n")
    val expectedPlan = Projection(
      Aggregation(
        plan.plan,
        Map("n" -> ident("n")),
        Map[String, Expression]("  AGGREGATION1" -> aggregationFunction)
      ),
      Map("n" -> ident("n"), "c" -> ident("  AGGREGATION1"))
    )

    aggregation(plan) should equal(QueryPlan(expectedPlan, context.queryGraph))
  }

  test("MATCH (n) RETURN n + 1 as m, count(has(n.prop))") {
    val aggregationFunction = Count.invoke(Has.invoke(Property(ident("n"), PropertyKeyName("prop") _) _)(pos))(DummyPosition(1))
    val addExpr = Add(ident("n"), SignedIntegerLiteral("1")_)_

    implicit val context = newMockedLogicalPlanContext(
      newMockedPlanContext,
      queryGraph = QueryGraph(
        patternNodes = Set[IdName]("n"),
        aggregatingProjections = Map("c" -> aggregationFunction),
        projections = Map("m" -> addExpr)
      )
    )

    val plan = newMockedQueryPlan("n")
    val expectedPlan = Projection(
      Aggregation(
        plan.plan,
        Map("m" -> addExpr),
        Map[String, Expression]("  AGGREGATION1" -> aggregationFunction)
      ),
      Map("m" -> ident("m"), "c" -> ident("  AGGREGATION1"))
    )

    aggregation(plan) should equal(QueryPlan(expectedPlan, context.queryGraph))
  }


  test("MATCH (n) RETURN n, count(has(n.prop)) + 1") {
    val aggregationFunction = Count.invoke(Has.invoke(Property(ident("n"), PropertyKeyName("prop") _) _)(pos))(DummyPosition(1))

    implicit val context = newMockedLogicalPlanContext(
      newMockedPlanContext,
      queryGraph = QueryGraph(
        patternNodes = Set[IdName]("n"),
        aggregatingProjections = Map("c" -> Add(aggregationFunction, SignedIntegerLiteral("1")_)_),
        projections = Map("n" -> ident("n"))
      )
    )

    val plan = newMockedQueryPlan("n")
    val expectedPlan = Projection(
      Aggregation(
        plan.plan,
        Map("n" -> ident("n")),
        Map[String, Expression]("  AGGREGATION1" -> aggregationFunction)
      ),
      Map("n" -> ident("n"), "c" -> Add(ident("  AGGREGATION1"), SignedIntegerLiteral("1")_)_)
    )

    aggregation(plan) should equal(QueryPlan(expectedPlan, context.queryGraph))
  }

  test("MATCH (n)-[r]->(m) RETURN n, m, count(has(r.prop)) + 1") {
    val aggregationFunction = Count.invoke(Has.invoke(Property(ident("r"), PropertyKeyName("prop") _) _)(pos))(DummyPosition(1))

    implicit val context = newMockedLogicalPlanContext(
      newMockedPlanContext,
      queryGraph = QueryGraph(
        patternNodes = Set[IdName]("n", "m", "r"),
        aggregatingProjections = Map("c" -> Add(aggregationFunction, SignedIntegerLiteral("1")_)_),
        projections = Map("n" -> ident("n"), "m" -> ident("m"))
      )
    )

    val plan = newMockedQueryPlan("n", "m", "r")
    val expectedPlan = Projection(
      Aggregation(
        plan.plan,
        Map("n" -> ident("n"), "m" -> ident("m")),
        Map[String, Expression]("  AGGREGATION1" -> aggregationFunction)
      ),
      Map("n" -> ident("n"), "m" -> ident("m"), "c" -> Add(ident("  AGGREGATION1"), SignedIntegerLiteral("1")_)_)
    )

    aggregation(plan) should equal(QueryPlan(expectedPlan, context.queryGraph))
  }

  test("MATCH n-->x RETURN n, collect(x)") {

    val aggregationFunction = Count.invoke(Has.invoke(Property(ident("r"), PropertyKeyName("prop") _) _)(pos))(DummyPosition(1))

    implicit val context = newMockedLogicalPlanContext(
      newMockedPlanContext,
      queryGraph = QueryGraph(
        patternNodes = Set[IdName]("n", "m", "r"),
        aggregatingProjections = Map("c" -> Add(aggregationFunction, SignedIntegerLiteral("1")_)_),
        projections = Map("n" -> ident("n"), "m" -> ident("m"))
      )
    )

    val plan = newMockedQueryPlan("n", "m", "r")
    val expectedPlan = Projection(
      Aggregation(
        plan.plan,
        Map("n" -> ident("n"), "m" -> ident("m")),
        Map[String, Expression]("  AGGREGATION1" -> aggregationFunction)
      ),
      Map("n" -> ident("n"), "m" -> ident("m"), "c" -> Add(ident("  AGGREGATION1"), SignedIntegerLiteral("1")_)_)
    )

    aggregation(plan) should equal(QueryPlan(expectedPlan, context.queryGraph))
  }
}
