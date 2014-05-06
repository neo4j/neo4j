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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement

class IsolateAggregationTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest = isolateAggregation

  test("does not rewrite things that should not be rewritten") {
    assertIsNotRewritten("MATCH n RETURN n")
    assertIsNotRewritten("MATCH n RETURN n, count(*), max(n.prop)")
  }

  test("MATCH (n) RETURN { name: n.name, count: count(*) }") {
    assertRewrite(
      "MATCH (n) RETURN { name: n.name, count: count(*) }",
      "MATCH (n) WITH n.name AS `  T$27`, count(*) AS `  T$40` RETURN { name: `  T$27`, count: `  T$40` } AS `{ name: n.name, count: count(*) }`")
  }

  test("MATCH n RETURN n.foo + count(*)") {
    assertRewrite(
      "MATCH n RETURN n.foo + count(*)",
      "MATCH n WITH n.foo AS `  T$17`, count(*) as `  T$23` RETURN `  T$17` + `  T$23` AS `n.foo + count(*)`")
  }

  test("MATCH n RETURN count(*)/60/42") {
    assertRewrite(
      "MATCH n RETURN count(*)/60/42",
      "MATCH n WITH count(*) as `  T$15`, 60 as `  T$24`, 42 as `  T$27` RETURN `  T$15`/`  T$24`/`  T$27` as `count(*)/60/42`")
  }

  test("MATCH n-->() RETURN (n)-->({k: count(*)})") {
    assertRewrite(
      "MATCH n-->() RETURN (n)-->({k: count(*)})",
      "MATCH n-->() WITH n, count(*) as `  T$31` RETURN (n)-->({k:`  T$31`}) as `(n)-->({k: count(*)})`")
  }

  test("MATCH n RETURN n.prop, n.foo + count(*)") {
    assertRewrite(
      "MATCH n RETURN n.prop, n.foo + count(*)",
      "MATCH n WITH n.prop as `  T$17`, n.foo as `  T$25`, count(*) as `  T$31` RETURN `  T$17` as `n.prop`, `  T$25` + `  T$31` AS `n.foo + count(*)`")
  }

  test("MATCH n RETURN n, count(n) + 3") {
    assertRewrite(
      "MATCH n RETURN n, count(n) + 3",
      "MATCH n WITH n, count(n) as `  T$18`, 3 as `  T$29` RETURN n, `  T$18` + `  T$29` as `count(n) + 3`")
  }

  test("MATCH n WITH 60/60/count(*) as x RETURN x") {
    assertRewrite(
      "MATCH n WITH 60/60/count(*) as x RETURN x",
      "MATCH n WITH 60/60 as `  T$15`, count(*) as `  T$19` WITH `  T$15`/`  T$19` as x RETURN x")
  }

  test("MATCH (a:Start)<-[:R]-(b) RETURN { foo:a.prop=42, bar:collect(b.prop2) }") {
    assertRewrite(
      "MATCH (a:Start)<-[:R]-(b) " +
      "RETURN { foo:a.prop=42, bar:collect(b.prop2) }",

      "MATCH (a:Start)<-[:R]-(b) " +
      "WITH a.prop=42 as `  T$45`, collect(b.prop2) as `  T$54` " +
      "RETURN { foo:`  T$45`, bar:`  T$54`} as `{ foo:a.prop=42, bar:collect(b.prop2) }`")
  }

  override protected def parseForRewriting(queryText: String): Statement = {
    super.parseForRewriting(queryText).typedRewrite[Statement](bottomUp(aliasReturnItems))
  }
}
