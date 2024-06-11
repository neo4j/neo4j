/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LiteralReplacementTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should extract starts with patterns") {
    assertRewrite(
      "RETURN x STARTS WITH 'Pattern' as X",
      "RETURN x STARTS WITH $`  AUTOSTRING0` as X",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(7)) -> "Pattern")
    )
  }

  test("should not extract literal dynamic property lookups") {
    assertDoesNotRewrite("MATCH (n) RETURN n[\"name\"]")
  }

  test("should extract literals in return clause") {
    assertRewrite(
      "RETURN 1 as result",
      "RETURN $`  AUTOINT0` as result",
      Map(autoParameter("  AUTOINT0", CTInteger) -> 1)
    )
    assertRewrite(
      "RETURN 1.1 as result",
      "RETURN $`  AUTODOUBLE0` as result",
      Map(autoParameter("  AUTODOUBLE0", CTFloat) -> 1.1)
    )
    assertRewrite(
      "RETURN 'apa' as result",
      "RETURN $`  AUTOSTRING0` as result",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(3)) -> "apa")
    )
    assertRewrite(
      "RETURN \"apa\" as result",
      "RETURN $`  AUTOSTRING0` as result",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(3)) -> "apa")
    )
    assertRewrite(
      "RETURN [1, 2, 3] as result",
      "RETURN $`  AUTOLIST0` as result",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(3)) -> Seq(1, 2, 3))
    )
  }

  test("should extract literals in call clause") {
    assertRewrite(
      "CALL  { RETURN 1 as result } RETURN result",
      "CALL { RETURN $`  AUTOINT0` as result } RETURN result",
      Map(autoParameter("  AUTOINT0", CTInteger) -> 1)
    )
  }

  test("should not extract literals if configured to never extract") {
    assertRewrite("RETURN 1 as result", "RETURN 1 as result", Map.empty, extractLiterals = Never)
  }

  test("should not extract boolean literals in return clause") {
    assertDoesNotRewrite(s"RETURN true as result")
    assertDoesNotRewrite(s"RETURN false as result")
  }

  test("should not extract quantifier literals in quantified path patterns") {
    assertDoesNotRewrite(s"MATCH ((a)-->(b)){1, 2} RETURN *")
  }

  test("should not extract ANY n in path selector") {
    assertDoesNotRewrite(s"MATCH ANY 5 (a)-->(b) RETURN *")
  }

  test("should not extract ANY SHORTEST in path selector") {
    assertDoesNotRewrite(s"MATCH ANY SHORTEST PATHS (a)-->(b) RETURN *")
  }

  test("should not extract SHORTEST n in path selector") {
    assertDoesNotRewrite(s"MATCH SHORTEST 5 PATHS (a)-->(b) RETURN *")
  }

  test("should not extract SHORTEST n GROUPS in path selector") {
    assertDoesNotRewrite(s"MATCH SHORTEST 3 GROUPS (a)-->(b) RETURN *")
  }

  test("should extract literals in match clause") {
    assertRewrite("MATCH ({a:1})", "MATCH ({a:$`  AUTOINT0`})", Map(autoParameter("  AUTOINT0", CTInteger) -> 1))
    assertRewrite(
      "MATCH ({a:1.1})",
      "MATCH ({a:$`  AUTODOUBLE0`})",
      Map(autoParameter("  AUTODOUBLE0", CTFloat) -> 1.1)
    )
    assertRewrite(
      "MATCH ({a:'apa'})",
      "MATCH ({a:$`  AUTOSTRING0`})",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(3)) -> "apa")
    )
    assertRewrite(
      "MATCH ({a:\"apa\"})",
      "MATCH ({a:$`  AUTOSTRING0`})",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(3)) -> "apa")
    )
    assertRewrite(
      "MATCH (n) WHERE ID(n) IN [1, 2, 3]",
      "MATCH (n) WHERE ID(n) IN $`  AUTOLIST0`",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(3)) -> Seq(1, 2, 3))
    )
  }

  test("should not extract boolean literals in match clause") {
    assertDoesNotRewrite(s"MATCH ({a:true})")
    assertDoesNotRewrite(s"MATCH ({a:false})")
  }

  test("should not extract literals in limit or skip") {
    assertRewrite(
      s"RETURN 0 as x SKIP 1 LIMIT 2",
      s"RETURN $$`  AUTOINT0` as x SKIP 1 LIMIT 2",
      Map(autoParameter("  AUTOINT0", CTInteger) -> 0)
    )
  }

  test("should extract literals in create statement clause") {
    assertRewrite(
      "CREATE (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'})",
      "CREATE (a {a:$`  AUTOINT0`, b:$`  AUTOSTRING1`, c:$`  AUTOINT2`, d:$`  AUTOSTRING3`})",
      Map(
        autoParameter("  AUTOINT0", CTInteger) -> 0,
        autoParameter("  AUTOSTRING1", CTString, Some(6)) -> "name 0",
        autoParameter("  AUTOINT2", CTInteger) -> 10000000,
        autoParameter("  AUTOSTRING3", CTString, Some(20)) -> "a very long string 0"
      )
    )
  }

  test("should extract literals in insert statement clause") {
    assertRewrite(
      "INSERT (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'})",
      "INSERT (a {a:$`  AUTOINT0`, b:$`  AUTOSTRING1`, c:$`  AUTOINT2`, d:$`  AUTOSTRING3`})",
      Map(
        autoParameter("  AUTOINT0", CTInteger) -> 0,
        autoParameter("  AUTOSTRING1", CTString, Some(6)) -> "name 0",
        autoParameter("  AUTOINT2", CTInteger) -> 10000000,
        autoParameter("  AUTOSTRING3", CTString, Some(20)) -> "a very long string 0"
      )
    )
  }

  test("should extract literals in merge clause") {
    assertRewrite(
      s"MERGE (n {a:'apa'}) ON CREATE SET n.foo = 'apa' ON MATCH SET n.foo = 'apa'",
      s"MERGE (n {a:$$`  AUTOSTRING0`}) ON CREATE SET n.foo = $$`  AUTOSTRING1` ON MATCH SET n.foo = $$`  AUTOSTRING2`",
      Map(
        autoParameter("  AUTOSTRING0", CTString, Some(3)) -> "apa",
        autoParameter("  AUTOSTRING1", CTString, Some(3)) -> "apa",
        autoParameter("  AUTOSTRING2", CTString, Some(3)) -> "apa"
      )
    )
  }

  test("should extract literals in multiple patterns") {
    assertRewrite(
      s"create (a {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (b {a:0, b:'name 0', c:10000000, d:'a very long string 0'}) create (a)-[:KNOWS {since: 0}]->(b)",
      s"create (a {a:$$`  AUTOINT0`, b:$$`  AUTOSTRING1`, c:$$`  AUTOINT2`, d:$$`  AUTOSTRING3`}) create (b {a:$$`  AUTOINT4`, b:$$`  AUTOSTRING5`, c:$$`  AUTOINT6`, d:$$`  AUTOSTRING7`}) create (a)-[:KNOWS {since: $$`  AUTOINT8`}]->(b)",
      Map(
        autoParameter("  AUTOINT0", CTInteger) -> 0,
        autoParameter("  AUTOSTRING1", CTString, Some(6)) -> "name 0",
        autoParameter("  AUTOINT2", CTInteger) -> 10000000,
        autoParameter("  AUTOSTRING3", CTString, Some(20)) -> "a very long string 0",
        autoParameter("  AUTOINT4", CTInteger) -> 0,
        autoParameter("  AUTOSTRING5", CTString, Some(6)) -> "name 0",
        autoParameter("  AUTOINT6", CTInteger) -> 10000000,
        autoParameter("  AUTOSTRING7", CTString, Some(20)) -> "a very long string 0",
        autoParameter("  AUTOINT8", CTInteger) -> 0
      )
    )
  }

  test("should rewrite queries that already have params in them if so configured") {
    assertRewrite(
      "CREATE (a:Person {name:'Jakub', age:$age })",
      "CREATE (a:Person {name: $`  AUTOSTRING0`, age:$age })",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(5)) -> "Jakub"),
      extractLiterals = Forced
    )
  }

  test("should not rewrite queries that already have params in them if so configured") {
    assertRewrite(
      "CREATE (a:Person {name:'Jakub', age:$age })",
      "CREATE (a:Person {name:'Jakub', age:$age })",
      Map.empty,
      extractLiterals = IfNoParameter
    )
  }

  test("should rewrite queries that already have params in them if configured to") {
    assertRewrite(
      "CREATE (a:Person {name: 'Jakub', age: $age })",
      "CREATE (a:Person {name: $`  AUTOSTRING0`, age: $age })",
      Map(autoParameter("  AUTOSTRING0", CTString, Some(5)) -> "Jakub"),
      Forced
    )
  }

  test("should extract from procedure calls") {
    assertRewrite("CALL foo(12)", "CALL foo($`  AUTOINT0`)", Map(autoParameter("  AUTOINT0", CTInteger) -> 12))
  }

  test("should extract from UNWIND") {
    assertRewrite(
      "UNWIND [1, 2, 3] AS list RETURN list",
      "UNWIND $`  AUTOLIST0` AS list RETURN list",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(3)) -> Vector(1, 2, 3))
    )
  }

  test("should extract in the correct order from Ands") {
    assertRewrite(
      "MATCH (n) WHERE 10 < n.prop < 20 RETURN n",
      "MATCH (n) WHERE $`  AUTOINT0` < n.prop < $`  AUTOINT1` RETURN n",
      Map(
        autoParameter("  AUTOINT0", CTInteger) -> 10,
        autoParameter("  AUTOINT1", CTInteger) -> 20
      )
    )
  }

  test("should preserve type information for lists of strings") {
    assertRewrite(
      "MATCH ({a:['a', 'b']})",
      "MATCH ({a:$`  AUTOLIST0`})",
      Map(autoParameter("  AUTOLIST0", CTList(CTString), Some(2)) -> Seq("a", "b"))
    )
  }

  test("should not preserve type information for lists of other types") {
    assertRewrite(
      "MATCH ({a:['a', 1]})",
      "MATCH ({a:$`  AUTOLIST0`})",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(2)) -> Seq("a", 1))
    )
    assertRewrite(
      "MATCH ({a:[1, 2]})",
      "MATCH ({a:$`  AUTOLIST0`})",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(2)) -> Seq(1, 2))
    )
    assertRewrite(
      "MATCH ({a:[]})",
      "MATCH ({a:$`  AUTOLIST0`})",
      Map(autoParameter("  AUTOLIST0", CTList(CTAny), Some(0)) -> Seq())
    )
  }

  private def assertDoesNotRewrite(query: String): Unit = {
    assertRewrite(query, query, Map.empty)
  }

  private def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    replacements: Map[AutoExtractedParameter, Any],
    extractLiterals: LiteralExtractionStrategy = Forced
  ): Unit = {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val original = parse(originalQuery, exceptionFactory)
    val expected = parse(expectedQuery, exceptionFactory)

    val (rewriter, actuallyReplacedLiterals) = literalReplacement(original, extractLiterals)
    val expectedReplacedLiterals = replacements.map {
      case (n, v) => n -> literal(v)
    }

    val result = original.endoRewrite(rewriter).endoRewrite(removeAutoExtracted())
    assert(result === expected)
    assert(expectedReplacedLiterals === actuallyReplacedLiterals)
  }

  private def removeAutoExtracted() = bottomUp(Rewriter.lift {
    case p @ AutoExtractedParameter(name, _, _) => ExplicitParameter(name, CTAny)(p.position)
  })
}
