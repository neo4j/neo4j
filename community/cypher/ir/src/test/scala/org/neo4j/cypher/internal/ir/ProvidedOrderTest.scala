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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should append provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.empty
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder.empty
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    val empty = ProvidedOrder.empty
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder
      .asc(varFor("a"))
      .asc(varFor("b"))
      .asc(varFor("c"))
      .desc(prop("d", "prop"))
      .desc(add(literalInt(10), prop("e", "prop")))

    left.upToExcluding(Set("x")).columns should be(left.columns)

    left.upToExcluding(Set("a")).columns should be(Seq.empty)

    left.upToExcluding(Set("c")).columns should be(left.columns.slice(0, 2))
    left.upToExcluding(Set("d")).columns should be(left.columns.slice(0, 3))
    left.upToExcluding(Set("e")).columns should be(left.columns.slice(0, 4))

    ProvidedOrder.empty.upToExcluding(Set("c")).columns should be(Seq.empty)
  }
}
