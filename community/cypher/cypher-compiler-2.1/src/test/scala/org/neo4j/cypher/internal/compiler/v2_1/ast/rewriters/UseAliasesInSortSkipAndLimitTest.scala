/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.Rewriter

class UseAliasesInSortSkipAndLimitTest extends CypherFunSuite with RewriteTest {
  def rewriterUnderTest: Rewriter = useAliasesInSortSkipAndLimit

  // NOTE: we suppose that all the return items are aliased here

  test("should not inline when no aggregations - match n return n.prop as `n.prop` order by n.prop") {
    assertRewrite(
      "match n return n.prop as `n.prop` order by n.prop",
      "match n return n.prop as `n.prop` order by n.prop"
    )
  }

  test("match n return n as `n`, count(*) as `count(*)` order by count(*)") {
    assertRewrite(
      "match n return n as `n`, count(*) as `count(*)` order by count(*)",
      "match n return n as `n`, count(*) as `count(*)` order by `count(*)`"
    )
  }

  test("match n return n.c as `n.c`, count(*) as `count(*)` order by n.c") {
    assertRewrite(
      "match n return n.c as `n.c`, count(*) as `count(*)` order by n.c",
      "match n return n.c as `n.c`, count(*) as `count(*)` order by `n.c`"
    )
  }

  test("should not inline when no aggregations - match n with n.prop as `n.prop` order by n.prop return *") {
    assertRewrite(
      "match n with n.prop as `n.prop` order by n.prop return *",
      "match n with n.prop as `n.prop` order by n.prop return *"
    )
  }

  test("match n with n as `n`, count(*) as `count(*)` order by count(*) return *") {
    assertRewrite(
      "match n with n as `n`, count(*) as `count(*)` order by count(*) return *",
      "match n with n as `n`, count(*) as `count(*)` order by `count(*)` return *"
    )
  }

  test("match n with n.c as `n.c`, count(*) as `count(*)` order by n.c return *") {
    assertRewrite(
      "match n with n.c as `n.c`, count(*) as `count(*)` order by n.c return *",
      "match n with n.c as `n.c`, count(*) as `count(*)` order by `n.c` return *"
    )
  }
}
