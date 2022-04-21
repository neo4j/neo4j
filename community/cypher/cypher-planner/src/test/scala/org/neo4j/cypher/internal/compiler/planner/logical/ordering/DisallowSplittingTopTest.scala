/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DisallowSplittingTopTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should not demote order in query with only limits") {
    val query =
      RegularSinglePlannerQuery()
        .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))
        .withTail(RegularSinglePlannerQuery()
          .withHorizon(RegularQueryProjection().withPagination(limitPagination(2))))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(None)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with order by in self") {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with order by in self and order by in tail") {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y")))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with order by in self and order by and limit in tail") {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y"))
          .withHorizon(RegularQueryProjection().withPagination(limitPagination(1))))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with order by in tail") {
    val query =
      RegularSinglePlannerQuery()
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y")))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.TailOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with limit in self and order by in tail") {
    val query =
      RegularSinglePlannerQuery()
        .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y")))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.TailOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should not demote order in query with order by in tail and order by and limit in tail of tail") {
    val query =
      RegularSinglePlannerQuery()
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("x"))
          .withTail(RegularSinglePlannerQuery()
            .withInterestingOrder(requiredOrder("y"))
            .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.TailOrderBy))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(false)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should demote order in query with order by and limit in self, but not when planning horizon") {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))
        .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderByLimit))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(true)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test(
    "should demote order in query with order by and limit in self and order by in tail, but not when planning horizon"
  ) {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))
        .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y")))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderByLimit))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(true)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test(
    "should demote order in query with order by and limit in self and order by and limit in tail, but not when planning horizon"
  ) {
    val query =
      RegularSinglePlannerQuery()
        .withInterestingOrder(requiredOrder("x"))
        .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y"))
          .withHorizon(RegularQueryProjection().withPagination(limitPagination(1))))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.SelfOrderByLimit))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(true)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(false)
  }

  test("should demote order in query with order by and limit in tail, also when planning horizon") {
    val query =
      RegularSinglePlannerQuery()
        .withTail(RegularSinglePlannerQuery()
          .withInterestingOrder(requiredOrder("y"))
          .withHorizon(RegularQueryProjection().withPagination(limitPagination(1))))

    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.TailOrderByLimit))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(true)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(true)
  }

  test(
    "should demote order in query with order by and limit in tail and order by and limit in tail of tail, also when planning horizon"
  ) {
    val query =
      RegularSinglePlannerQuery()
        .withTail(
          RegularSinglePlannerQuery()
            .withInterestingOrder(requiredOrder("x"))
            .withHorizon(RegularQueryProjection().withPagination(limitPagination(1)))
            .withTail(RegularSinglePlannerQuery()
              .withInterestingOrder(requiredOrder("y"))
              .withHorizon(RegularQueryProjection().withPagination(limitPagination(1))))
        )
    DisallowSplittingTop.requiredOrderOrigin(query)
      .shouldEqual(Some(DisallowSplittingTop.TailOrderByLimit))

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = false, disallowSplittingTop = true)
      .shouldEqual(true)

    DisallowSplittingTop.demoteRequiredOrderToInterestingOrder(query, isHorizon = true, disallowSplittingTop = true)
      .shouldEqual(true)
  }

  private def limitPagination(i: Int) =
    QueryPagination().withLimit(Some(Limit(literal(i))(InputPosition.NONE)))

  private def requiredOrder(varName: String) =
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor(varName)))

}
