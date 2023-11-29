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
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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

    left.upToExcluding(Set(v"x")).columns should be(left.columns)

    left.upToExcluding(Set(v"a")).columns should be(Seq.empty)

    left.upToExcluding(Set(v"c")).columns should be(left.columns.slice(0, 2))
    left.upToExcluding(Set(v"d")).columns should be(left.columns.slice(0, 3))
    left.upToExcluding(Set(v"e")).columns should be(left.columns.slice(0, 4))

    ProvidedOrder.empty.upToExcluding(Set(v"c")).columns should be(Seq.empty)
  }

  test("should find common prefixes") {
    val mt = ProvidedOrder.empty
    val a = ProvidedOrder.asc(varFor("a"))
    val b = ProvidedOrder.asc(varFor("b"))
    val ab = a.asc(varFor("b"))
    val ac = a.asc(varFor("c"))
    val abc = ab.asc(varFor("c"))
    val abd = ab.asc(varFor("d"))

    mt.commonPrefixWith(mt) should be(mt)
    a.commonPrefixWith(mt) should be(mt)
    mt.commonPrefixWith(a) should be(mt)

    a.commonPrefixWith(a) should be(a)
    a.commonPrefixWith(b) should be(mt)
    a.commonPrefixWith(ab) should be(a)

    ab.commonPrefixWith(a) should be(a)
    ab.commonPrefixWith(ab) should be(ab)
    ab.commonPrefixWith(abc) should be(ab)
    ab.commonPrefixWith(ac) should be(a)

    abc.commonPrefixWith(abd) should be(ab)
    abc.commonPrefixWith(ab) should be(ab)
    abc.commonPrefixWith(ac) should be(a)
  }
}
