/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.Rewriter

class HoistExpressionsInClosingClausesTest extends CypherFunSuite with RewriteTest {
  override def rewriterUnderTest: Rewriter = hoistExpressionsInClosingClauses

  test("does hoist when no aggregation or distinct is found") {
    assertRewrite(
      "MATCH n RETURN n.prop AS x ORDER BY n.prop",
      "MATCH n RETURN n.prop AS x ORDER BY x"
    )
  }

  test("does hoist when no aggregation or distinct is found in WITH") {
    assertRewrite(
      "MATCH n WITH n.prop AS x ORDER BY n.prop RETURN x",
      "MATCH n WITH n.prop AS x ORDER BY x RETURN x"
    )
  }

  test("does hoist when aggregating and ORDER BY grouping key expression") {
    assertRewrite(
      "MATCH n RETURN n.prop, count(*) ORDER BY n.prop",
      "MATCH n RETURN n.prop, count(*) ORDER BY `n.prop`"
    )
  }

  test("does hoist from WITH when aggregating and ORDER BY grouping key expression") {
    assertRewrite(
      "MATCH n WITH n.prop, count(*) AS count ORDER BY n.prop RETURN count",
      "MATCH n WITH n.prop, count(*) AS count ORDER BY `n.prop` RETURN count"
    )
  }

  test("does hoist when aggregating and WHERE with grouping key expression") {
    assertRewrite(
      "MATCH n WITH n.prop, count(*) AS count WHERE n.prop <> 42 RETURN count",
      "MATCH n WITH n.prop, count(*) AS count WHERE `n.prop` <> 42 RETURN count"
    )
  }

  test("does hoist when aggregating with an alias and ORDER BY grouping key expression") {
    assertRewrite(
      "MATCH n RETURN n.prop AS foo, count(*) ORDER BY n.prop",
      "MATCH n RETURN n.prop AS foo, count(*) ORDER BY foo"
    )
  }

  test("does hoist when aggregating with an alias and WHERE with grouping key expression") {
    assertRewrite(
      "MATCH n WITH n.prop AS foo, count(*) WHERE n.prop <> 42 RETURN foo",
      "MATCH n WITH n.prop AS foo, count(*) WHERE foo <> 42 RETURN foo"
    )
  }

  test("does hoist when distinct is used") {
    assertRewrite(
      "MATCH n RETURN DISTINCT n.prop ORDER BY n.prop",
      "MATCH n RETURN DISTINCT n.prop ORDER BY `n.prop`"
    )
  }

  test("does hoist even when ordering contains a projected expression as a sub expression") {
    assertRewrite(
      "MATCH n RETURN DISTINCT n.prop ORDER BY n.prop + 2",
      "MATCH n RETURN DISTINCT n.prop ORDER BY `n.prop` + 2"
    )
  }

  test("does not hoist when a subexpression is used in ORDER BY") {
    assertIsNotRewritten(
      "MATCH n RETURN DISTINCT n.prop + 2 as x ORDER BY n.prop"
    )
  }

  test("does not hoist when conflicting aliases are introduced") {
    assertIsNotRewritten(
      "MATCH n RETURN DISTINCT n.prop as n ORDER BY n.prop"
    )
  }

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

  test("start n=node(0,1,2,3) return n.division, max(n.age) order by max(n.age)") {
    assertRewrite(
      "start n=node(0,1,2,3) return n.division, max(n.age) order by max(n.age)",
      "start n=node(0,1,2,3) return n.division, max(n.age) order by `max(n.age)`"
    )
  }

  test("start a=node(*) return a, count(*) order by COUNT(*)") {
    assertRewrite(
      "start a=node(*) return a, count(*) order by COUNT(*)",
      "start a=node(*) return a, count(*) order by `count(*)`"
    )
  }

  test("MATCH a, b RETURN b.prop, collect(a.prop) ORDER BY length(collect(a.prop)), b.prop") {
    assertRewrite(
      "MATCH a, b RETURN b.prop as `b.prop`, collect(a.prop) as `collect(a.prop)` ORDER BY length(collect(a.prop)), b.prop",
      "MATCH a, b RETURN b.prop as `b.prop`, collect(a.prop) as `collect(a.prop)` ORDER BY length(`collect(a.prop)`), `b.prop`"
    )
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = rewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }
}
