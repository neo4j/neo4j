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
package org.neo4j.cypher.internal.ir.v3_5

import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite {

  test("should append provided order") {
    val left = ProvidedOrder.asc("a").asc("b")
    val right = ProvidedOrder.asc("c").asc("d")
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder.asc("a").asc("b")
    val right = ProvidedOrder.empty
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder.empty
    val right = ProvidedOrder.asc("c").asc("d")
    val empty = ProvidedOrder.empty
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder.asc("a").asc("b").asc("c").asc("d")

    left.upToExcluding(Set("x")).columns should be(left.columns)

    left.upToExcluding(Set("a")).columns should be(Seq.empty)

    left.upToExcluding(Set("c")).columns should be(left.columns.slice(0, 2))

    ProvidedOrder.empty.upToExcluding(Set("c")).columns should be(Seq.empty)
  }

  test("Empty required order satisfied by anything") {
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.empty) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc("x")) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc("x")) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc("x").asc("y")) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc("x").desc("y")) should be(true)
  }

  test("Single property required order satisfied by matching provided order") {
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.asc("x")) should be(true)
  }

  test("Single property required order satisfied by longer provided order") {
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.asc("x").asc("y")) should be(true)
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.asc("x").desc("y")) should be(true)
  }

  test("Single property required order not satisfied by mismatching provided order") {
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.asc("y")) should be(false)
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.desc("x")) should be(false)
    InterestingOrder.asc("x").satisfiedBy(ProvidedOrder.asc("y").asc("x")) should be(false)
  }

  test("Multi property required order satisfied only be matching provided order") {
    val interestingOrder = InterestingOrder.asc("x").desc("y").asc("z")

    interestingOrder.satisfiedBy(ProvidedOrder.asc("x")) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc("x").desc("y")) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc("x").desc("y").asc("z")) should be(true)
    interestingOrder.satisfiedBy(ProvidedOrder.asc("x").desc("z").asc("y")) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc("x").desc("y").desc("z")) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc("x").asc("y").desc("z")) should be(false)
  }
}
