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

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LiteralReplacementTest extends CypherFunSuite  {

  import org.neo4j.cypher.internal.compiler.v2_3.parser.ParserFixture.parser

  test("should extract starts with patterns") {
    assertRewrite("RETURN x STARTS WITH 'Pattern' as X", "RETURN x STARTS WITH {`  AUTOSTRING0`} as X", Map("  AUTOSTRING0" -> "Pattern"))
  }

  test("should not extract literal dynamic property lookups") {
    assertDoesNotRewrite("MATCH n RETURN n[\"name\"]")
  }

  test("should extract literals in return clause") {
    assertRewrite(s"RETURN 1 as result", s"RETURN {`  AUTOINT0`} as result", Map("  AUTOINT0" -> 1))
    assertRewrite(s"RETURN 1.1 as result", s"RETURN {`  AUTODOUBLE0`} as result", Map("  AUTODOUBLE0" -> 1.1))
    assertRewrite(s"RETURN true as result", s"RETURN {`  AUTOBOOL0`} as result", Map("  AUTOBOOL0" -> true))
    assertRewrite(s"RETURN false as result", s"RETURN {`  AUTOBOOL0`} as result", Map("  AUTOBOOL0" -> false))
    assertRewrite("RETURN 'apa' as result", "RETURN {`  AUTOSTRING0`} as result", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("RETURN \"apa\" as result", "RETURN {`  AUTOSTRING0`} as result", Map("  AUTOSTRING0" -> "apa"))
  }

  test("should extract literals in match clause") {
    assertRewrite(s"MATCH ({a:1})", s"MATCH ({a:{`  AUTOINT0`}})", Map("  AUTOINT0" -> 1))
    assertRewrite(s"MATCH ({a:1.1})", s"MATCH ({a:{`  AUTODOUBLE0`}})", Map("  AUTODOUBLE0" -> 1.1))
    assertRewrite(s"MATCH ({a:true})", s"MATCH ({a:{`  AUTOBOOL0`}})", Map("  AUTOBOOL0" -> true))
    assertRewrite(s"MATCH ({a:false})", s"MATCH ({a:{`  AUTOBOOL0`}})", Map("  AUTOBOOL0" -> false))
    assertRewrite("MATCH ({a:'apa'})", "MATCH ({a:{`  AUTOSTRING0`}})", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("MATCH ({a:\"apa\"})", "MATCH ({a:{`  AUTOSTRING0`}})", Map("  AUTOSTRING0" -> "apa"))
  }

  test("should extract literals in skip clause") {
    assertRewrite(
      s"RETURN 0 as x SKIP 1 limit 2",
      s"RETURN {`  AUTOINT0`} as x SKIP {`  AUTOINT1`} LIMIT 2",
      Map("  AUTOINT0" -> 0, "  AUTOINT1" -> 1)
    )
  }

  test("should extract literals in create statement clause") {
    assertRewrite(
      "create (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'})",
      "create (a {a:{`  AUTOINT0`}, b:{`  AUTOSTRING1`}, c:{`  AUTOINT2`}, d:{`  AUTOSTRING3`}})",
      Map("  AUTOINT0"->0,"  AUTOSTRING1"->"name 0","  AUTOINT2"->10000000,"  AUTOSTRING3"->"a very long string 0")
    )
  }

  test("should extract literals in merge clause") {
    assertRewrite(
      s"MERGE (n {a:'apa'}) ON CREATE SET n.foo = 'apa' ON MATCH SET n.foo = 'apa'",
      s"MERGE (n {a:{`  AUTOSTRING0`}}) ON CREATE SET n.foo = {`  AUTOSTRING1`} ON MATCH SET n.foo = {`  AUTOSTRING2`}",
      Map("  AUTOSTRING0" -> "apa", "  AUTOSTRING1" -> "apa", "  AUTOSTRING2" -> "apa")
    )
  }

  test("should extract literals in multiple patterns") {
    assertRewrite(
      s"create (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (b {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (a)-[:KNOWS {since: 0}]->(b)",
      s"create (a {a:{`  AUTOINT0`}, b:{`  AUTOSTRING1`}, c:{`  AUTOINT2`}, d:{`  AUTOSTRING3`}}) create (b {a:{`  AUTOINT4`}, b:{`  AUTOSTRING5`}, c:{`  AUTOINT6`}, d:{`  AUTOSTRING7`}}) create (a)-[:KNOWS {since: {`  AUTOINT8`}}]->(b)",
      Map(
        "  AUTOINT0" -> 0, "  AUTOSTRING1" -> "name 0", "  AUTOINT2" -> 10000000, "  AUTOSTRING3" -> "a very long string 0",
        "  AUTOINT4" -> 0, "  AUTOSTRING5" -> "name 0", "  AUTOINT6" -> 10000000, "  AUTOSTRING7" -> "a very long string 0",
        "  AUTOINT8" -> 0
      )
    )
  }

  test("should not rewrite queries that already have params in them") {
    assertRewrite(
      "CREATE (a:Person {name:'Jakub', age:{age} })",
      "CREATE (a:Person {name:'Jakub', age:{age} })",
      Map.empty
    )
  }

  private def assertDoesNotRewrite(query: String): Unit = {
    assertRewrite(query, query, Map.empty)
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String, replacements: Map[String, Any]) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)

    val (rewriter, replacedLiterals) = literalReplacement(original)

    val result = original.rewrite(rewriter)
    assert(result === expected)
    assert(replacements === replacedLiterals)
  }
}
