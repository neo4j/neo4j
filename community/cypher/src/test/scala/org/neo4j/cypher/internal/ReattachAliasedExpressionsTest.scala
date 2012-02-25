/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal

import commands._
import org.scalatest.Assertions
import org.junit.Test

class ReattachAliasedExpressionsTest extends Assertions {
  @Test
  def rewriteOrderByAfterAliasedColumn() {
    // start a=node(1) return a, count(*) order by COUNT(*)

    val q = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Entity("newAlias"), true)).
      returns(ReturnItem(Property("a", "x"), "newAlias"))

    val expected = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Property("a", "x"), true)).
      returns(ReturnItem(Property("a", "x"), "newAlias"))

    assert(ReattachAliasedExpressions(q) === expected)
  }

  @Test
  def rewriteHavingAfterAliasedColumn() {
    // start a=node(1) return count(*) as foo order by foo

    val q = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(Entity("foo"), true)).
      returns(ReturnItem(CountStar(), "foo"))

    val expected = Query.
      start(NodeById("a", 1)).
      orderBy(SortItem(CountStar(), true)).
      returns(ReturnItem(CountStar(), "foo"))
    val result = ReattachAliasedExpressions(q)

    assert(result === expected)
  }
}