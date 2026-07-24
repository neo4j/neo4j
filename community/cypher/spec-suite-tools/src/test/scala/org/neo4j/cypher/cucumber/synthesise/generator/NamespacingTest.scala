/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.synthesise.generator

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Try

class NamespacingTest extends CypherFunSuite {

  private val cypher25Parser = new CachingParser(CypherVersion.Cypher25)
  private val cypher5Parser = new CachingParser(CypherVersion.Cypher5)

  test("Cypher 25 wrap of a read query parses and contains the expected markers") {
    val wrapped = Namespacing.cypher25Wrap("MATCH (n) RETURN n.foo AS foo")
    withClue(s"wrapped query:\n$wrapped\n") {
      cypher25Parser.parse(wrapped)
      wrapped should include("FINISH")
      wrapped should include("NEXT")
      wrapped should include("CALL")
    }
  }

  test("Cypher 5 wrap of a read query parses and contains the expected markers") {
    val wrapped = Namespacing.cypher5Wrap("MATCH (n) RETURN n.foo AS foo", returning = true, connective = "UNION ALL")
    withClue(s"wrapped query:\n$wrapped\n") {
      cypher5Parser.parse(wrapped)
      wrapped should include("UNION ALL")
      wrapped should include("CALL")
    }
  }

  test("Cypher 25 wrap of an updating query parses") {
    val wrapped = Namespacing.cypher25Wrap("CREATE (x {p:1}) RETURN x.p AS p")
    withClue(s"wrapped query:\n$wrapped\n")(cypher25Parser.parse(wrapped))
  }

  test("Cypher 5 wrap of an updating query parses") {
    val wrapped =
      Namespacing.cypher5Wrap("CREATE (x {p:1}) RETURN x.p AS p", returning = true, connective = "UNION ALL")
    withClue(s"wrapped query:\n$wrapped\n")(cypher5Parser.parse(wrapped))
  }

  test("Cypher 5 wrap of a query using Cypher-25-only NEXT syntax does not parse") {
    val wrapped =
      Namespacing.cypher5Wrap("RETURN 1 AS a NEXT RETURN 2 AS b", returning = true, connective = "UNION ALL")
    withClue(s"wrapped query:\n$wrapped\n")(Try(cypher5Parser.parse(wrapped)).isFailure shouldBe true)
  }

  test("Cypher 25 wrap of the same query parses fine (NEXT is native Cypher 25 syntax)") {
    val wrapped = Namespacing.cypher25Wrap("RETURN 1 AS a NEXT RETURN 2 AS b")
    withClue(s"wrapped query:\n$wrapped\n")(cypher25Parser.parse(wrapped))
  }

  test("query with an unaliased non-variable return is rejected") {
    Namespacing.returnsAreSubqueryLegal(cypher25Parser.parse("MATCH (n) RETURN n.foo")) shouldBe false
  }

  test("queries with aliased, bare-variable, or star returns are accepted") {
    Namespacing.returnsAreSubqueryLegal(cypher25Parser.parse("MATCH (n) RETURN n.foo AS foo")) shouldBe true
    Namespacing.returnsAreSubqueryLegal(cypher25Parser.parse("MATCH (n) RETURN n")) shouldBe true
    Namespacing.returnsAreSubqueryLegal(cypher25Parser.parse("MATCH (n) RETURN *")) shouldBe true
  }

  test("Cypher 5 wrap uses RETURN * for a returning query and FINISH for a non-returning one") {
    Namespacing.cypher5Wrap("MATCH (n) RETURN n", returning = true, connective = "UNION ALL") should include("RETURN *")
    val nonReturning = Namespacing.cypher5Wrap("CREATE (x {p: 1})", returning = false, connective = "UNION ALL")
    nonReturning should include("FINISH")
    nonReturning should not include "RETURN *"
  }

  test("connectiveFor matches Q's top-level union kind (so we never mix UNION with UNION ALL)") {
    Namespacing.connectiveFor(cypher5Parser.parse("RETURN 1 AS x UNION RETURN 2 AS x")) shouldBe "UNION"
    Namespacing.connectiveFor(cypher5Parser.parse("RETURN 1 AS x UNION ALL RETURN 2 AS x")) shouldBe "UNION ALL"
    Namespacing.connectiveFor(cypher5Parser.parse("MATCH (n) RETURN n")) shouldBe "UNION ALL"
  }

  test("standalone procedure calls are rejected; in-query calls and plain queries are kept") {
    Namespacing.isNotStandaloneCall(cypher25Parser.parse("CALL db.labels")) shouldBe false
    Namespacing.isNotStandaloneCall(cypher25Parser.parse("CALL db.labels()")) shouldBe false
    Namespacing.isNotStandaloneCall(
      cypher25Parser.parse("MATCH (n) CALL db.labels() YIELD label RETURN n, label")
    ) shouldBe true
    Namespacing.isNotStandaloneCall(cypher25Parser.parse("MATCH (n) RETURN n")) shouldBe true
  }
}
