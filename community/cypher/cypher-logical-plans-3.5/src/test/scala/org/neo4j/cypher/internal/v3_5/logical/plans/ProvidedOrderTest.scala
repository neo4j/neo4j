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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite {

  test("should append provided order") {
    val left = ProvidedOrder(Seq(Ascending("a"), Ascending("b")))
    val right = ProvidedOrder(Seq(Ascending("c"), Ascending("d")))
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder(Seq(Ascending("a"), Ascending("b")))
    val right = ProvidedOrder(Seq())
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder(Seq())
    val right = ProvidedOrder(Seq(Ascending("c"), Ascending("d")))
    val empty = ProvidedOrder(Seq())
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder(Seq(Ascending("a"), Ascending("b"), Ascending("c"), Ascending("d")))

    left.upToExcluding(Set("x")).columns should be(left.columns)

    left.upToExcluding(Set("a")).columns should be(Seq.empty)

    left.upToExcluding(Set("c")).columns should be(left.columns.slice(0, 2))

    ProvidedOrder.empty.upToExcluding(Set("c")).columns should be(Seq.empty)
  }
}
