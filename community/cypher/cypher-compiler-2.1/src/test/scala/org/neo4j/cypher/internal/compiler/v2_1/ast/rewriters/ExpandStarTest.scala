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
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement

class ExpandStarTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = expandStar

  test("rewrites * in return") {
    assertRewrite(
      "match n return *",
      "match n return n")

    assertRewrite(
      "match n,c return *",
      "match n,c return c,n")

    assertRewrite(
      "match n-->c return *",
      "match n-->c return c,n")

    assertRewrite(
      "match n-[r]->c return *",
      "match n-[r]->c return c,n,r")

    assertRewrite(
      "create (n) return *",
      "create (n) return n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) return *",
      "match p = shortestPath((a)-[r*]->(x)) return a,p,r,x")
  }

  test("rewrites * in with") {
    assertRewrite(
      "match n with * return n",
      "match n with n return n")

    assertRewrite(
      "match n,c with * return n",
      "match n,c with c,n return n")

    assertRewrite(
      "match n-->c with * return n",
      "match n-->c with c,n return n")

    assertRewrite(
      "match n-[r]->c with * return n",
      "match n-[r]->c with c,n,r return n")

    assertRewrite(
      "create (n) with * return n",
      "create (n) with n return n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) with a,p,r,x return p")
  }

  test("symbol shadowing should be taken into account") {
    assertRewrite(
      "match a,x,y with a match b return *",
      "match a,x,y with a match b return a, b")
  }

  override protected def parseForRewriting(queryText: String) =
    super.parseForRewriting(queryText).endoRewrite(aliasReturnItems)
}
