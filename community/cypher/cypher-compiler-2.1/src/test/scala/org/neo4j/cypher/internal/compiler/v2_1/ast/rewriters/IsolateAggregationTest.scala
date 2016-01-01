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

class IsolateAggregationTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest = isolateAggregation

  test("does not rewrite things that should not be rewritten") {
    assertIsNotRewritten("MATCH n RETURN n")
    assertIsNotRewritten("MATCH n RETURN n, count(*), max(n.prop)")
  }

  test("MATCH (n) RETURN { name: n.name, count: count(*) }") {
    assertRewrite(
      "MATCH (n) RETURN { name: n.name, count: count(*) }",
      "MATCH (n) WITH n.name AS `  AGGREGATION27`, count(*) AS `  AGGREGATION40` RETURN { name: `  AGGREGATION27`, count: `  AGGREGATION40` } AS `{ name: n.name, count: count(*) }`")
  }

  test("MATCH n RETURN n.foo + count(*)") {
    assertRewrite(
      "MATCH n RETURN n.foo + count(*)",
      "MATCH n WITH n.foo AS `  AGGREGATION17`, count(*) as `  AGGREGATION23` RETURN `  AGGREGATION17` + `  AGGREGATION23` AS `n.foo + count(*)`")
  }

  test("MATCH n RETURN count(*)/60/42") {
    assertRewrite(
      "MATCH n RETURN count(*)/60/42",
      "MATCH n WITH count(*) as `  AGGREGATION15`, 60 as `  AGGREGATION24`, 42 as `  AGGREGATION27` RETURN `  AGGREGATION15`/`  AGGREGATION24`/`  AGGREGATION27` as `count(*)/60/42`")
  }

  test("MATCH n-->() RETURN (n)-->({k: count(*)})") {
    assertRewrite(
      "MATCH n-->() RETURN (n)-->({k: count(*)})",
      "MATCH n-->() WITH n, count(*) as `  AGGREGATION31` RETURN (n)-->({k:`  AGGREGATION31`}) as `(n)-->({k: count(*)})`")
  }

  test("MATCH n RETURN n.prop, n.foo + count(*)") {
    assertRewrite(
      "MATCH n RETURN n.prop, n.foo + count(*)",
      "MATCH n WITH n.prop as `  AGGREGATION17`, n.foo as `  AGGREGATION25`, count(*) as `  AGGREGATION31` RETURN `  AGGREGATION17` as `n.prop`, `  AGGREGATION25` + `  AGGREGATION31` AS `n.foo + count(*)`")
  }

  test("MATCH n RETURN n, count(n) + 3") {
    assertRewrite(
      "MATCH n RETURN n, count(n) + 3",
      "MATCH n WITH n, count(n) as `  AGGREGATION18`, 3 as `  AGGREGATION29` RETURN n, `  AGGREGATION18` + `  AGGREGATION29` as `count(n) + 3`")
  }

  test("UNWIND [1,2,3] AS a RETURN reduce(y=0, x IN collect(a) | x) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN reduce(y=0, x IN collect(a) | x) AS z",
      "UNWIND [1,2,3] as a WITH 0 as `  AGGREGATION36`, collect(a) as `  AGGREGATION44` RETURN reduce(y=`  AGGREGATION36`, x IN `  AGGREGATION44` | x) as z")
  }

  test("UNWIND [1,2,3] AS a RETURN filter(x IN collect(a) WHERE x <> 0) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN filter(x IN collect(a) WHERE x <> 0) AS z",
      "UNWIND [1,2,3] as a WITH collect(a) as `  AGGREGATION39` RETURN filter(x IN `  AGGREGATION39` WHERE x <> 0) as z")
  }

  test("UNWIND [1,2,3] AS a RETURN extract(x IN collect(a) | x) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN extract(x IN collect(a) | x) AS z",
      "UNWIND [1,2,3] as a WITH collect(a) as `  AGGREGATION40` RETURN extract(x IN `  AGGREGATION40` | x) as z")
  }

  test("UNWIND [1,2,3] AS a RETURN [x IN collect(a) | x] AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN [x IN collect(a) | x] AS z",
      "UNWIND [1,2,3] as a WITH collect(a) as `  AGGREGATION33` RETURN [x IN `  AGGREGATION33` | x] as z")
  }


  test("MATCH n WITH 60/60/count(*) as x RETURN x") {
    assertRewrite(
      "MATCH n WITH 60/60/count(*) as x RETURN x",
      "MATCH n WITH 60/60 as `  AGGREGATION15`, count(*) as `  AGGREGATION19` WITH `  AGGREGATION15`/`  AGGREGATION19` as x RETURN x")
  }

  test("MATCH (a:Start)<-[:R]-(b) RETURN { foo:a.prop=42, bar:collect(b.prop2) }") {
    assertRewrite(
      "MATCH (a:Start)<-[:R]-(b) " +
      "RETURN { foo:a.prop=42, bar:collect(b.prop2) }",

      "MATCH (a:Start)<-[:R]-(b) " +
      "WITH a.prop=42 as `  AGGREGATION45`, collect(b.prop2) as `  AGGREGATION54` " +
      "RETURN { foo:`  AGGREGATION45`, bar:`  AGGREGATION54`} as `{ foo:a.prop=42, bar:collect(b.prop2) }`")
  }

  test("MATCH n RETURN count(*) + max(id(n)) as r") {
    assertRewrite(
      "MATCH n RETURN count(*) + max(id(n)) as r",
      "MATCH n WITH count(*) as `  AGGREGATION15`, max(id(n)) as `  AGGREGATION26` RETURN `  AGGREGATION15`+`  AGGREGATION26` as r")
  }

  override protected def parseForRewriting(queryText: String) =
    super.parseForRewriting(queryText).endoRewrite(aliasReturnItems)
}
