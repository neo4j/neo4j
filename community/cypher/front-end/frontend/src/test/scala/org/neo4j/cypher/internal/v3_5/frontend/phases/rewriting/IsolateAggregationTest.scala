/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_5.frontend.phases.rewriting

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.ast.semantics.SyntaxExceptionCreator
import org.neo4j.cypher.internal.v3_5.frontend.phases.{Monitors, isolateAggregation}
import org.neo4j.cypher.internal.v3_5.rewriting.RewriteTest
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.v3_5.util.inSequence
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class IsolateAggregationTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {
  val rewriterUnderTest = isolateAggregation.instance(new TestContext(mock[Monitors]))

  test("refers to renamed variable in where clause") {
    assertRewrite(
      """
        |MATCH (owner)
        |WITH owner, count(*) > 0 AS collected
        |WHERE (owner)-->()
        |RETURN owner
      """.stripMargin,
      """
        |MATCH (owner)
        |WITH owner AS `  AGGREGATION20`, count(*) AS `  AGGREGATION27`
        |WITH `  AGGREGATION20` AS owner, `  AGGREGATION27` > 0 AS collected
        |  WHERE (owner)-->()
        |RETURN owner AS owner
      """.stripMargin)
  }

  test("refers to renamed variable in order by clause") {
    assertRewrite(
      """
        |MATCH (owner)
        |WITH owner, count(*) > 0 AS collected
        |ORDER BY owner.foo
        |RETURN owner
      """.stripMargin,
      """
        |MATCH (owner)
        |WITH owner AS `  AGGREGATION20`, count(*) AS `  AGGREGATION27`
        |WITH `  AGGREGATION20` AS owner, `  AGGREGATION27` > 0 AS collected
        |  ORDER BY owner.foo
        |RETURN owner AS owner
      """.stripMargin)
  }

  test("does not rewrite things that should not be rewritten") {
    assertIsNotRewritten("MATCH n RETURN n AS n")
    assertIsNotRewritten("MATCH n RETURN n AS n, count(*) AS count, max(n.prop) AS max")
  }

  test("MATCH (n) RETURN { name: n.name, count: count(*) } AS result") {
    assertRewrite(
      "MATCH (n) RETURN { name: n.name, count: count(*) } AS result",
      "MATCH (n) WITH n.name AS `  AGGREGATION27`, count(*) AS `  AGGREGATION40` RETURN { name: `  AGGREGATION27`, count: `  AGGREGATION40` } AS result")
  }

  test("MATCH (n) RETURN n.foo + count(*) AS result") {
    assertRewrite(
      "MATCH (n) RETURN n.foo + count(*) AS result",
      "MATCH (n) WITH n.foo AS `  AGGREGATION19`, count(*) AS `  AGGREGATION25` RETURN `  AGGREGATION19` + `  AGGREGATION25` AS result")
  }

  test("MATCH (n) RETURN count(*)/60/42 AS result") {
    assertRewrite(
      "MATCH (n) RETURN count(*)/60/42 AS result",
      "MATCH (n) WITH count(*) AS `  AGGREGATION17` RETURN `  AGGREGATION17`/60/42 AS result")
  }

  test("WITH 60 as sixty, 42 as fortytwo MATCH (n) RETURN count(*)/sixty/fortytwo AS result") {
    assertRewrite(
      "WITH 60 as sixty, 42 as fortytwo MATCH (n) RETURN count(*)/sixty/fortytwo AS result",
      "WITH 60 as sixty, 42 as fortytwo MATCH (n) WITH count(*) AS `  AGGREGATION50`, sixty AS `  AGGREGATION59`, fortytwo AS `  AGGREGATION65` RETURN `  AGGREGATION50`/`  AGGREGATION59`/`  AGGREGATION65` AS result")
  }

  test("WITH 1 AS node, [] AS nodes1 RETURN ANY (n IN collect(distinct node) WHERE n IN nodes1) as count") {
    assertRewrite(
      """WITH 1 AS node, [] AS nodes1
        |RETURN ANY (n IN collect(distinct node) WHERE n IN nodes1) as count""".stripMargin,
      """WITH 1 AS node, [] AS nodes1
        |WITH collect(distinct node) AS `  AGGREGATION46`, nodes1 AS `  AGGREGATION80`
        |RETURN ANY (n IN `  AGGREGATION46` WHERE n IN `  AGGREGATION80`) as count""".stripMargin)
  }

  test("WITH 1 AS node, [] AS nodes1 RETURN NONE(n IN collect(distinct node) WHERE n IN nodes1) as count") {
    assertRewrite(
      """WITH 1 AS node, [] AS nodes1
        |RETURN NONE(n IN collect(distinct node) WHERE n IN nodes1) as count""".stripMargin,
      """WITH 1 AS node, [] AS nodes1
        |WITH collect(distinct node) AS `  AGGREGATION46`, nodes1 AS `  AGGREGATION80`
        |RETURN NONE(n IN `  AGGREGATION46` WHERE n IN `  AGGREGATION80`) as count""".stripMargin)
  }

  test("MATCH (n)-->() RETURN (n)-->({k: count(*)}) AS result") {
    assertRewrite(
      "MATCH (n)-->() RETURN (n)-->({k: count(*)}) AS result",
      "MATCH (n)-->() WITH n as `  AGGREGATION23`, count(*) AS `  AGGREGATION33` RETURN (`  AGGREGATION23`)-->({k:`  AGGREGATION33`}) AS result")
  }

  test("MATCH (n) RETURN n.prop AS prop, n.foo + count(*) AS count") {
    assertRewrite(
      "MATCH (n) RETURN n.prop AS prop, n.foo + count(*) AS count",
      "MATCH (n) WITH n.prop AS `  AGGREGATION19`, n.foo AS `  AGGREGATION35`, count(*) AS `  AGGREGATION41` RETURN `  AGGREGATION19` AS prop, `  AGGREGATION35` + `  AGGREGATION41` AS count")
  }

  test("MATCH (n) RETURN n AS n, count(n) + 3 AS count") {
    assertRewrite(
      "MATCH (n) RETURN n AS n, count(n) + 3 AS count",
      "MATCH (n) WITH n AS `  AGGREGATION17`, count(n) as `  AGGREGATION25`  RETURN `  AGGREGATION17` AS n, `  AGGREGATION25` + 3 AS count")
  }

  test("UNWIND [1,2,3] AS a RETURN reduce(y=0, x IN collect(a) | x) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN reduce(y=0, x IN collect(a) | x) AS z",
      "UNWIND [1,2,3] AS a WITH collect(a) AS `  AGGREGATION44` RETURN reduce(y=0, x IN `  AGGREGATION44` | x) AS z")
  }

  test("UNWIND [1,2,3] AS a RETURN filter(x IN collect(a) WHERE x <> 0) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN filter(x IN collect(a) WHERE x <> 0) AS z",
      "UNWIND [1,2,3] AS a WITH collect(a) AS `  AGGREGATION39` RETURN filter(x IN `  AGGREGATION39` WHERE x <> 0) AS z")
  }

  test("UNWIND [1,2,3] AS a RETURN extract(x IN collect(a) | x) AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN extract(x IN collect(a) | x) AS z",
      "UNWIND [1,2,3] AS a WITH collect(a) AS `  AGGREGATION40` RETURN extract(x IN `  AGGREGATION40` | x) AS z")
  }

  test("UNWIND [1,2,3] AS a RETURN [x IN collect(a) | x] AS z") {
    assertRewrite(
      "UNWIND [1,2,3] AS a RETURN [x IN collect(a) | x] AS z",
      "UNWIND [1,2,3] AS a WITH collect(a) AS `  AGGREGATION33` RETURN [x IN `  AGGREGATION33` | x] AS z")
  }

  test("MATCH (n) WITH 60/60/count(*) AS x RETURN x AS x") {
    assertRewrite(
      "MATCH (n) WITH 60/60/count(*) AS x RETURN x AS x",
      "MATCH (n) WITH count(*) AS `  AGGREGATION21` WITH 60/60/`  AGGREGATION21` AS x RETURN x AS x")
  }

  test("MATCH (a:Start)<-[:R]-(b) RETURN { foo:a.prop=42, bar:collect(b.prop2) } AS result") {
    assertRewrite(
      "MATCH (a:Start)<-[:R]-(b) " +
        "RETURN { foo:a.prop=42, bar:collect(b.prop2) } AS result",

      "MATCH (a:Start)<-[:R]-(b) " +
        "WITH a.prop=42 AS `  AGGREGATION45`, collect(b.prop2) AS `  AGGREGATION54` " +
        "RETURN { foo:`  AGGREGATION45`, bar:`  AGGREGATION54`} AS result")
  }

  test("MATCH (n) RETURN count(*) + max(id(n)) AS r") {
    assertRewrite(
      "MATCH (n) RETURN count(*) + max(id(n)) AS r",
      "MATCH (n) WITH count(*) AS `  AGGREGATION17`, max(id(n)) AS `  AGGREGATION28` RETURN `  AGGREGATION17`+`  AGGREGATION28` AS r")
  }

  test("MATCH (a) RETURN length(collect(a)) AS length") {
    assertRewrite(
      "MATCH (a) RETURN length(collect(a)) AS length",
      "MATCH (a) WITH collect(a) AS `  AGGREGATION24` RETURN length(`  AGGREGATION24`) AS length")
  }

  test("MATCH (a) RETURN count(a) > 0 AS bool") {
    assertRewrite(
      "MATCH (a) RETURN count(a) > 0 AS bool",
      "MATCH (a) WITH count(a) AS `  AGGREGATION17` RETURN `  AGGREGATION17` > 0 AS bool")
  }

  test("MATCH (a) RETURN count(a) > $param AS bool") {
    assertRewrite(
      "MATCH (a) RETURN count(a) > $param AS bool",
      "MATCH (a) WITH count(a) AS `  AGGREGATION17` RETURN `  AGGREGATION17` > $param AS bool")
  }

  test("should not introduce multiple return items for the same expression") {
    assertRewrite(
      "WITH 1 AS x, 2 AS y RETURN sum(x)*y AS a, sum(x)*y AS b",
      "WITH 1 AS x, 2 AS y WITH sum(x) as `  AGGREGATION27`, y as `  AGGREGATION34` RETURN `  AGGREGATION27`* `  AGGREGATION34` AS a, `  AGGREGATION27`*`  AGGREGATION34` AS b")
  }

  test("MATCH (a), (b) RETURN coalesce(a.prop, b.prop), b.prop, { x: count(b) }") {
    assertRewrite(
      """MATCH (a), (b)
        |RETURN coalesce(a.prop, b.prop), b.prop, { x: count(b) }""".stripMargin,
      """MATCH (a), (b)
        |WITH coalesce(a.prop, b.prop) AS `  AGGREGATION22`,
        |     b.prop AS `  AGGREGATION50`,
        |     count(b) AS `  AGGREGATION61`
        |RETURN `  AGGREGATION22` AS `coalesce(a.prop, b.prop)`,
        |       `  AGGREGATION50` AS `b.prop`,
        |       { x: `  AGGREGATION61` } AS `{ x: count(b) }`""".stripMargin)
  }

  test("should not extract expressions that do not contain on variables as implicit grouping key") {
    assertRewrite(
      """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
        |WITH user AS user, collect(friendship)[toInteger(rand() * count(friendship))] AS selectedFriendship
        |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin,
      """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
        |WITH user AS `  AGGREGATION59`, collect(friendship) AS `  AGGREGATION73`, count(friendship) AS `  AGGREGATION112`
        |WITH `  AGGREGATION59` AS user, `  AGGREGATION73`[toInteger(rand() * `  AGGREGATION112`)] AS selectedFriendship
        |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    )
  }

  override protected def parseForRewriting(queryText: String) = {
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    super.parseForRewriting(queryText).endoRewrite(inSequence(normalizeWithAndReturnClauses(mkException)))
  }
}
