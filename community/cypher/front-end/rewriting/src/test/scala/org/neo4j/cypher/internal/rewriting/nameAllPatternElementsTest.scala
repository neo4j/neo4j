/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.parser.ParserFixture.parser
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class nameAllPatternElementsTest extends CypherFunSuite {

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val original = parser.parse(originalQuery, exceptionFactory)
    val expected = removeGeneratedNamesAndParamsOnTree(parser.parse(expectedQuery, exceptionFactory))
    val result = removeGeneratedNamesAndParamsOnTree(original.rewrite(nameAllPatternElements))

    assert(result === expected)
  }

  test("name all NodePatterns in Query") {
    assertRewrite(
      "MATCH (n)-[r:Foo]->() RETURN n",
      "MATCH (n)-[r:Foo]->(`  NODE19`) RETURN n")
  }

  test("name all RelationshipPatterns in Query") {
    assertRewrite(
      "MATCH (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n",
    "MATCH (n)-[`  REL9`:Foo]->(m) WHERE (n)-[`  REL31`:Bar]->(m) RETURN n")
  }

  test("rename unnamed varlength paths") {
    assertRewrite(
      "MATCH (n)-[:Foo*]->(m) RETURN n",
    "MATCH (n)-[`  REL9`:Foo*]->(m) RETURN n")
  }

  test("match (a) create (a)-[:X]->() return a") {
    assertRewrite(
      "match (a) create (a)-[:X]->() return a",
    "match (a) create (a)-[`  REL20`:X]->(`  NODE27`) return a")
  }

  test("merge (a) merge p = (a)-[:R]->() return p") {
    assertRewrite(
      "merge (a) merge p = (a)-[:R]->() return p",
    "merge (a) merge p = (a)-[`  REL23`:R]->(`  NODE30`) return p")
  }

  test("merge (a)-[:R]->() return a") {
    assertRewrite(
      "merge (a)-[:R]->() return a",
    "merge (a)-[`  REL9`:R]->(`  NODE16`) return a")
  }

  test("does not touch parameters") {
    assertRewrite(
      "MATCH (n)-[r:Foo]->($p) RETURN n",
    "MATCH (n)-[r:Foo]->(`  NODE19` $p) RETURN n")
  }

  test("names all unnamed var length relationships") {
    assertRewrite(
      "MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *",
    "MATCH (a:Artist)-[`  REL16`:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *")
  }

  test("should name all pattern elements in a pattern comprehension") {
    assertRewrite(
      "RETURN [()-->() | 'foo'] AS foo",
    "RETURN [(`  NODE8`)-[`  REL10`]->(`  NODE13`) | 'foo'] AS foo")
  }

  test("should name all pattern elements in a pattern expressions") {
    assertRewrite(
      "MATCH (a) WHERE (a)-[:R]->()",
    "MATCH (a) WHERE (a)-[`  REL19`:R]->(`  NODE26`)")
  }

  test("should name all pattern elements in a EXISTS") {
    assertRewrite(
      "MATCH (a) WHERE EXISTS { MATCH (a)-[:R]->() }",
    "MATCH (a) WHERE EXISTS { MATCH (a)-[`  REL34`:R]->(`  NODE41`) }")
  }

  test("should not change names of already named things") {
    val original = parser.parse("RETURN [p=(a)-[r]->(b) | 'foo'] AS foo", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === original)
  }

  test("should not name in shortest path expressions") {
    val original = parser.parse(
      """
        |MATCH (a:A), (b:B)
        |WITH shortestPath((a)-[:REL]->(b)) AS x
        |RETURN x AS x""".stripMargin, exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === original)
  }
}
