/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.ir.v3_5

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite {

  test("should append provided order") {
    val left = ProvidedOrder(Seq(ProvidedOrder.Asc("a"), ProvidedOrder.Asc("b")))
    val right = ProvidedOrder(Seq(ProvidedOrder.Asc("c"), ProvidedOrder.Asc("d")))
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder(Seq(ProvidedOrder.Asc("a"), ProvidedOrder.Asc("b")))
    val right = ProvidedOrder(Seq())
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder(Seq())
    val right = ProvidedOrder(Seq(ProvidedOrder.Asc("c"), ProvidedOrder.Asc("d")))
    val empty = ProvidedOrder(Seq())
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder(Seq(ProvidedOrder.Asc("a"), ProvidedOrder.Asc("b"), ProvidedOrder.Asc("c"), ProvidedOrder.Asc("d")))

    left.upToExcluding(Set("x")).columns should be(left.columns)

    left.upToExcluding(Set("a")).columns should be(Seq.empty)

    left.upToExcluding(Set("c")).columns should be(left.columns.slice(0, 2))

    ProvidedOrder.empty.upToExcluding(Set("c")).columns should be(Seq.empty)
  }

  test("Empty required order satisfied by anything") {
    RequiredOrder.empty.satisfiedBy(ProvidedOrder.empty) should be(true)
    RequiredOrder.empty.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x")))) should be(true)
    RequiredOrder.empty.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Desc("x")))) should be(true)
    RequiredOrder.empty.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Asc("y")))) should be(true)
    RequiredOrder.empty.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Desc("x"), ProvidedOrder.Desc("y")))) should be(true)
  }

  test("Single property required order satisfied by matching provided order") {
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x")))) should be(true)
  }

  test("Single property required order satisfied by longer provided order") {
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Asc("y")))) should be(true)
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Desc("y")))) should be(true)
  }

  test("Single property required order not satisfied by mismatching provided order") {
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("y")))) should be(false)
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Desc("x")))) should be(false)
    RequiredOrder(Seq(("x", AscColumnOrder))).satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("y"), ProvidedOrder.Asc("x")))) should be(false)
  }

  test("Multi property required order satisfied only be matching provided order") {
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", DescColumnOrder),
      ("z", AscColumnOrder)
    ))
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x")))) should be(false)
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Desc("y")))) should be(false)
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Desc("y"), ProvidedOrder.Asc("z")))) should be(true)
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Desc("z"), ProvidedOrder.Asc("y")))) should be(false)
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Desc("y"), ProvidedOrder.Desc("z")))) should be(false)
    requiredOrder.satisfiedBy(ProvidedOrder(Seq(ProvidedOrder.Asc("x"), ProvidedOrder.Asc("y"), ProvidedOrder.Desc("z")))) should be(false)
  }
}
