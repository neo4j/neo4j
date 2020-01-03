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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{Forced, IfNoParameter, LiteralExtraction, literalReplacement}
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_5.expressions.Parameter

class LiteralReplacementTest extends CypherFunSuite  {

  import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parser

  test("should extract starts with patterns") {
    assertRewrite("RETURN x STARTS WITH 'Pattern' as X", "RETURN x STARTS WITH $`  AUTOSTRING0` as X", Map("  AUTOSTRING0" -> "Pattern"))
  }

  test("should not extract literal dynamic property lookups") {
    assertDoesNotRewrite("MATCH n RETURN n[\"name\"]")
  }

  test("should extract literals in return clause") {
    assertRewrite("RETURN 1 as result", "RETURN $`  AUTOINT0` as result", Map("  AUTOINT0" -> 1))
    assertRewrite("RETURN 1.1 as result", "RETURN $`  AUTODOUBLE0` as result", Map("  AUTODOUBLE0" -> 1.1))
    assertRewrite("RETURN 'apa' as result", "RETURN $`  AUTOSTRING0` as result", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("RETURN \"apa\" as result", "RETURN $`  AUTOSTRING0` as result", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("RETURN [1, 2, 3] as result", "RETURN $`  AUTOLIST0` as result", Map("  AUTOLIST0" -> Seq(1, 2, 3)))
  }

  test("should not extract boolean literals in return clause") {
    assertDoesNotRewrite(s"RETURN true as result")
    assertDoesNotRewrite(s"RETURN false as result")
  }

  test("should extract literals in match clause") {
    assertRewrite("MATCH ({a:1})", "MATCH ({a:$`  AUTOINT0`})", Map("  AUTOINT0" -> 1))
    assertRewrite("MATCH ({a:1.1})", "MATCH ({a:$`  AUTODOUBLE0`})", Map("  AUTODOUBLE0" -> 1.1))
    assertRewrite("MATCH ({a:'apa'})", "MATCH ({a:$`  AUTOSTRING0`})", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("MATCH ({a:\"apa\"})", "MATCH ({a:$`  AUTOSTRING0`})", Map("  AUTOSTRING0" -> "apa"))
    assertRewrite("MATCH (n) WHERE ID(n) IN [1, 2, 3]", "MATCH (n) WHERE ID(n) IN $`  AUTOLIST0`", Map("  AUTOLIST0" -> Seq(1, 2, 3)))
  }

  test("should not extract boolean literals in match clause") {
    assertDoesNotRewrite(s"MATCH ({a:true})")
    assertDoesNotRewrite(s"MATCH ({a:false})")
  }

  test("should extract literals in skip clause") {
    assertRewrite(
      s"RETURN 0 as x SKIP 1 limit 2",
      "RETURN $`  AUTOINT0` as x SKIP $`  AUTOINT1` LIMIT 2",
      Map("  AUTOINT0" -> 0, "  AUTOINT1" -> 1)
    )
  }

  test("should extract literals in create statement clause") {
    assertRewrite(
      "create (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'})",
      "create (a {a:$`  AUTOINT0`, b:$`  AUTOSTRING1`, c:$`  AUTOINT2`, d:$`  AUTOSTRING3`})",
      Map("  AUTOINT0"->0,"  AUTOSTRING1"->"name 0","  AUTOINT2"->10000000,"  AUTOSTRING3"->"a very long string 0")
    )
  }

  test("should extract literals in merge clause") {
    assertRewrite(
      s"MERGE (n {a:'apa'}) ON CREATE SET n.foo = 'apa' ON MATCH SET n.foo = 'apa'",
      "MERGE (n {a:$`  AUTOSTRING0`}) ON CREATE SET n.foo = $`  AUTOSTRING1` ON MATCH SET n.foo = $`  AUTOSTRING2`",
      Map("  AUTOSTRING0" -> "apa", "  AUTOSTRING1" -> "apa", "  AUTOSTRING2" -> "apa")
    )
  }

  test("should extract literals in multiple patterns") {
    assertRewrite(
      s"create (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (b {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (a)-[:KNOWS {since: 0}]->(b)",
      "create (a {a:$`  AUTOINT0`, b:$`  AUTOSTRING1`, c:$`  AUTOINT2`, d:$`  AUTOSTRING3`}) create (b {a:$`  AUTOINT4`, b:$`  AUTOSTRING5`, c:$`  AUTOINT6`, d:$`  AUTOSTRING7`}) create (a)-[:KNOWS {since: $`  AUTOINT8`}]->(b)",
      Map(
        "  AUTOINT0" -> 0, "  AUTOSTRING1" -> "name 0", "  AUTOINT2" -> 10000000, "  AUTOSTRING3" -> "a very long string 0",
        "  AUTOINT4" -> 0, "  AUTOSTRING5" -> "name 0", "  AUTOINT6" -> 10000000, "  AUTOSTRING7" -> "a very long string 0",
        "  AUTOINT8" -> 0
      )
    )
  }

  test("should not rewrite queries that already have params in them") {
    assertRewrite(
      "CREATE (a:Person {name:'Jakub', age:$age })",
      "CREATE (a:Person {name:'Jakub', age:$age })",
      Map.empty
    )
  }

  test("should rewrite queries that already have params in them if configured to") {
    assertRewrite(
      "CREATE (a:Person {name: 'Jakub', age: $age })",
      "CREATE (a:Person {name: $`  AUTOSTRING0`, age: $age })",
      Map("  AUTOSTRING0" -> "Jakub"),
      Forced
    )
  }

  test("should extract from procedure calls") {
    assertRewrite("CALL foo(12)", "CALL foo($`  AUTOINT0`)", Map("  AUTOINT0" -> 12))
  }

  test("should extract from UNWIND") {
    assertRewrite(
      "UNWIND [1, 2, 3] AS list RETURN list",
      "UNWIND $`  AUTOLIST0` AS list RETURN list",
      Map("  AUTOLIST0" -> Vector(1, 2, 3))
    )
  }

  private def assertDoesNotRewrite(query: String): Unit = {
    assertRewrite(query, query, Map.empty)
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String, replacements: Map[String, Any], extractLiterals: LiteralExtraction = IfNoParameter) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery).endoRewrite(fixParameterTypeExpectations)

    val (rewriter, replacedLiterals) = literalReplacement(original, extractLiterals)

    val result = original.rewrite(rewriter)
    assert(result === expected)
    assert(replacements === replacedLiterals)
  }

  private def fixParameterTypeExpectations = bottomUp(Rewriter.lift {
    case p@Parameter(name, _) if name.startsWith("  AUTOSTRING") => p.copy(parameterType = CTString)(p.position)
    case p@Parameter(name, _) if name.startsWith("  AUTOINT") => p.copy(parameterType = CTInteger)(p.position)
    case p@Parameter(name, _) if name.startsWith("  AUTODOUBLE") => p.copy(parameterType = CTFloat)(p.position)
    case p@Parameter(name, _) if name.startsWith("  AUTOLIST") => p.copy(parameterType = CTList(CTAny))(p.position)
  })
}
