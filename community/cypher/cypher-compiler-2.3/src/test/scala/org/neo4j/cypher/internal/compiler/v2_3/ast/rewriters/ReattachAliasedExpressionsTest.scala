/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ReattachAliasedExpressionsTest extends CypherFunSuite with RewriteTest {

  override def rewriterUnderTest: Rewriter = reattachAliasedExpressions

  test("MATCH a RETURN a.x AS newAlias ORDER BY newAlias") {
    assertRewrite(
      "MATCH a RETURN a.x AS newAlias ORDER BY newAlias",
      "MATCH a RETURN a.x AS newAlias ORDER BY a.x")
  }

  test("MATCH a RETURN count(*) AS foo ORDER BY foo") {
    assertRewrite(
      "MATCH a RETURN count(*) AS foo ORDER BY foo",
      "MATCH a RETURN count(*) AS foo ORDER BY count(*)")
  }

  test("MATCH a RETURN collect(a) AS foo ORDER BY length(foo)") {
    assertRewrite(
      "MATCH a RETURN collect(a) AS foo ORDER BY length(foo)",
      "MATCH a RETURN collect(a) AS foo ORDER BY length(collect(a))")
  }

  test("MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY foo") {
    assertRewrite(
      "MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY foo",
      "MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY count(x)")
  }

  test("MATCH a WITH a.x AS newAlias ORDER BY newAlias RETURN *") {
    assertRewrite(
      "MATCH a WITH a.x AS newAlias ORDER BY newAlias RETURN *",
      "MATCH a WITH a.x AS newAlias ORDER BY a.x RETURN *")
  }

  test("MATCH a WITH count(*) AS foo ORDER BY foo RETURN *") {
    assertRewrite(
      "MATCH a WITH count(*) AS foo ORDER BY foo RETURN *",
      "MATCH a WITH count(*) AS foo ORDER BY count(*) RETURN *")
  }

  test("MATCH x WITH x AS x WITH count(x) AS foo ORDER BY foo RETURN *") {
    assertRewrite(
      "MATCH x WITH x AS x WITH count(x) AS foo ORDER BY foo RETURN *",
      "MATCH x WITH x AS x WITH count(x) AS foo ORDER BY count(x) RETURN *")
  }

  test("MATCH x WITH x.prop as prop WHERE prop = 42 RETURN prop *") {
    assertIsNotRewritten( // The legacy planner does not want this to be done for WHERE clauses... *sigh*
      "MATCH x WITH x.prop as prop WHERE prop = 42 RETURN prop")
  }
}
