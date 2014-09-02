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
import org.neo4j.cypher.internal.compiler.v2_2._

class ProjectFreshSortExpressionsTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = projectFreshSortExpressions

  test("does not project ORDER BY expressions that are simple identifiers") {
    assertRewrite(
      "match n with n.prop AS prop ORDER BY prop RETURN prop",
      "match n with n.prop AS prop WITH prop WITH prop ORDER BY prop RETURN prop")
  }

  test("match n RETURN n ORDER BY n.prop") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop",
      "match n WITH n WITH n, n.prop AS `  FRESHID28` RETURN n ORDER BY `  FRESHID28`")
  }

  test("match n RETURN n ORDER BY n.prop LIMIT 2") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop LIMIT 2",
      "match n WITH n WITH n, n.prop AS `  FRESHID28` RETURN n ORDER BY `  FRESHID28` LIMIT 2")
  }

  test("match n RETURN n ORDER BY n.prop SKIP 5") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop SKIP 5",
      "match n WITH n WITH n, n.prop AS `  FRESHID28` RETURN n ORDER BY `  FRESHID28` SKIP 5")
  }

  test("match n WITH n ORDER BY n.prop RETURN n") {
    assertRewrite(
      "match n WITH n ORDER BY n.prop RETURN n",
      "match n WITH n WITH n, n.prop AS `  FRESHID26` WITH n ORDER BY `  FRESHID26` RETURN n")
  }

  test("match n WITH n.prop, count(n) ORDER BY length(n.prop) RETURN `n.prop`") {
    assertRewrite(
      "match n WITH n.prop, count(n) ORDER BY length(`n.prop`) RETURN `n.prop`",
      "match n WITH n.prop, count(n) WITH `n.prop` AS `n.prop`, `count(n)` AS `count(n)`, length(`n.prop`) AS `  FRESHID39` WITH `n.prop` AS `n.prop`, `count(n)` AS `count(n)` ORDER BY `  FRESHID39` RETURN `n.prop`")
  }

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    assertRewrite(
      "MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel",
      "MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c WITH r2, c WITH r2, c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel")
  }

  test("MATCH a RETURN a.count ORDER BY `a.count` SKIP 10 LIMIT 10") {
    assertRewrite(
      "MATCH a RETURN a.count ORDER BY `a.count` SKIP 10 LIMIT 10",
      "MATCH a WITH a.count AS `a.count` WITH `a.count` AS `a.count` RETURN `a.count` AS `a.count` ORDER BY `a.count` SKIP 10 LIMIT 10")
  }

  test("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4") {
    assertRewrite(
      "MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4",
      "MATCH foo WITH foo.bar AS x WITH x RETURN x ORDER BY x DESC LIMIT 4")
  }

  test("MATCH foo RETURN {meh} AS x ORDER BY x.prop DESC LIMIT 4") {
    assertRewrite(
      "MATCH foo RETURN {meh} AS x ORDER BY x.prop DESC LIMIT 4",
      "MATCH foo WITH {meh} AS x WITH x, x.prop AS `  FRESHID39` RETURN x ORDER BY `  FRESHID39` DESC LIMIT 4")
  }

  test("match n return n order by n.name ASC skip 2") {
    assertRewrite(
      "match n return n order by n.name ASC skip 2",
      "match n with n with n, n.name AS `  FRESHID28` return n order by `  FRESHID28` ASC skip 2"
    )
  }

  test("match x RETURN DISTINCT x as otherName ORDER BY x.name") {
    assertRewrite(
      "match x RETURN DISTINCT x as otherName ORDER BY x.name",
      "match x WITH x as otherName WITH otherName, otherName.name AS `  FRESHID50` RETURN DISTINCT otherName AS otherName ORDER BY `  FRESHID50`"
    )
  }

  test("match x RETURN x as otherName ORDER BY x.name + otherName.name") {
    assertRewrite(
      "match x RETURN x.prop as otherName ORDER BY x.name + otherName",
      "match x WITH x, x.prop as otherName WITH otherName, x.name + otherName AS `  FRESHID51` RETURN otherName AS otherName ORDER BY `  FRESHID51`"
    )
  }

  test("MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel") {
    assertRewrite(
      "MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel",
      "MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c WITH a, r, b, c WITH a, r, b, c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel"
    )
  }

  test("match n where id(n) IN [0,1,2,3] return n.division, max(n.age) order by max(n.age)") {
    assertRewrite(
      "match n where id(n) IN [0,1,2,3] return n.division, max(n.age) order by max(n.age)",
      "match n where id(n) IN [0,1,2,3] with n.division, max(n.age) with `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` order by `max(n.age)`"
    )
  }

  override protected def parseForRewriting(queryText: String) =
    super.parseForRewriting(queryText)
      .endoRewrite(hoistExpressionsInClosingClauses)
      .endoRewrite(aliasReturnItems)
}
