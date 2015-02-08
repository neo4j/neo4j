/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import commands._
import commands.ReturnItem
import commands.SortItem
import expressions._
import expressions.CountStar
import expressions.Property
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType.PropertyKey

class ReattachAliasedExpressionsTest extends Assertions {
  @Test
  def rewriteOrderByAfterAliasedColumn() {
    // start a=node(1) return a, count(*) order by COUNT(*)

    val q = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Identifier("newAlias"), ascending = true)).
      returns(ReturnItem(Property(Identifier("a"), PropertyKey("x")), "newAlias"))

    val expected = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Property(Identifier("a"), PropertyKey("x")), ascending = true)).
      returns(ReturnItem(Property(Identifier("a"), PropertyKey("x")), "newAlias"))

    assert(ReattachAliasedExpressions(q) === expected)
  }

  @Test
  def rewriteHavingAfterAliasedColumn() {
    // start a=node(1) return count(*) as foo order by foo

    val q = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Identifier("foo"), ascending = true)).
      returns(ReturnItem(CountStar(), "foo"))

    val expected = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(CountStar(), ascending = true)).
      returns(ReturnItem(CountStar(), "foo"))
    val result = ReattachAliasedExpressions(q)

    assert(result === expected)
  }

  @Test
  def rewriteQueryWithWITH() {
    // START x = node(0) WITH x RETURN count(x) as foo ORDER BY foo

    val secondQ = Query.
      start().
      orderBy(SortItem(Identifier("foo"), ascending = true)).
      returns(ReturnItem(Count(Identifier("x")), "foo", renamed = true))

    val q = Query.
      start(NodeById("x", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("x"), "x"))

    val expected2ndQ = Query.
      start().
      orderBy(SortItem(Count(Identifier("x")), ascending = true)).
      returns(ReturnItem(Count(Identifier("x")), "foo", renamed = true))

    val expected = Query.
      start(NodeById("x", 1)).
      tail(expected2ndQ).
      returns(ReturnItem(Identifier("x"), "x"))


    val result = ReattachAliasedExpressions(q)

    assert(result === expected)
  }
}