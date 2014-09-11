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

  test("dont adjust WITH without ORDER BY or WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n AS n
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |RETURN n
      """.stripMargin)
  }

  test("duplicate WITH containing ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)
  }

  test("duplicate WITH containing WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop WHERE prop > 2
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop WHERE prop > 2
        |RETURN prop
      """.stripMargin)
  }

  test("preserve DISTINCT on first WITH") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)

    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE prop > 2
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop WHERE prop > 2
        |RETURN prop
      """.stripMargin)
  }

  test("carry SKIP and LIMIT with ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop SKIP 2 LIMIT 5
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop SKIP 2 LIMIT 5
        |RETURN prop
      """.stripMargin)
  }

  test("carry SKIP and LIMIT with WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop SKIP 2 LIMIT 5 WHERE prop > 2
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop SKIP 2 LIMIT 5 WHERE prop > 2
        |RETURN prop
      """.stripMargin)
  }

  test("keep WHERE with ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop WHERE prop > 2
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop WHERE prop > 2
        |RETURN prop
      """.stripMargin)
  }

  test("match n RETURN n ORDER BY n.prop") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop",
      "match n WITH n WITH n AS n, n.prop AS `  FRESHID28` RETURN n AS n ORDER BY `  FRESHID28`")
  }

  test("match n RETURN n ORDER BY n.prop LIMIT 2") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop LIMIT 2",
      "match n WITH n WITH n AS n, n.prop AS `  FRESHID28` RETURN n AS n ORDER BY `  FRESHID28` LIMIT 2")
  }

  test("match n RETURN n ORDER BY n.prop SKIP 5") {
    assertRewrite(
      "match n RETURN n ORDER BY n.prop SKIP 5",
      "match n WITH n WITH n AS n, n.prop AS `  FRESHID28` RETURN n AS n ORDER BY `  FRESHID28` SKIP 5")
  }

  test("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4") {
    assertRewrite(
      "MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4",
      "MATCH foo WITH foo.bar AS x WITH x AS x RETURN x AS x ORDER BY x DESC LIMIT 4")
  }

  test("MATCH foo RETURN {meh} AS x ORDER BY x.prop DESC LIMIT 4") {
    assertRewrite(
      "MATCH foo RETURN {meh} AS x ORDER BY x.prop DESC LIMIT 4",
      "MATCH foo WITH {meh} AS x WITH x AS x, x.prop AS `  FRESHID39` RETURN x AS x ORDER BY `  FRESHID39` DESC LIMIT 4")
  }

  test("match n return n order by n.name ASC skip 2") {
    assertRewrite(
      "match n return n order by n.name ASC skip 2",
      "match n with n with n AS n, n.name AS `  FRESHID28` return n AS n order by `  FRESHID28` ASC skip 2"
    )
  }

  test("match x RETURN DISTINCT x as otherName ORDER BY x.name") {
    assertRewrite(
      "match x RETURN DISTINCT x as otherName ORDER BY x.name",
      "match x WITH x AS x, x as otherName WITH otherName AS otherName, x.name AS `  FRESHID50` RETURN DISTINCT otherName AS otherName ORDER BY `  FRESHID50`"
    )
  }

  test("match x RETURN x as otherName ORDER BY x.name + otherName.name") {
    assertRewrite(
      "match x RETURN x.prop as otherName ORDER BY x.name + otherName",
      "match x WITH x AS x, x.prop as otherName WITH otherName AS otherName, x.name + otherName AS `  FRESHID51` RETURN otherName AS otherName ORDER BY `  FRESHID51`"
    )
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }
}
