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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory.asc
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory.desc
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ImmutableProvidedOrdersTest extends CypherFunSuite {

  test("immutable provided order") {
    assertImmutableWorks()
    assertImmutableWorks(0 -> asc(varFor("x")))
    assertImmutableWorks(1 -> asc(varFor("x")))
    assertImmutableWorks(
      3 -> asc(varFor("a")),
      4 -> asc(varFor("a")),
      7 -> desc(varFor("b")),
      13 -> asc(varFor("c")),
      100 -> desc(varFor("d"))
    )
  }

  private def assertImmutableWorks(orders: (Int, ProvidedOrder)*): Unit = {
    val mutable = new ProvidedOrders
    orders.foreach { case (id, o) => mutable.set(Id(id), o) }
    val immutable = ImmutablePlanningAttributes.ProvidedOrders(mutable)

    mutable.iterator.foreach { case (id, o) =>
      immutable.isDefinedAt(id) shouldBe true
      immutable.get(id) shouldBe o
    }
    immutable.toMutable.toSeq shouldBe mutable.toSeq
  }
}
