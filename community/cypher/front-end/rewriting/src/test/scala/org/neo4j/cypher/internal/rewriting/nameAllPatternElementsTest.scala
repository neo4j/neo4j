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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.parser.ParserFixture.parser
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class nameAllPatternElementsTest extends CypherFunSuite {

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  test("name all NodePatterns in Query") {
    val original = parser.parse("MATCH (n)-[r:Foo]->() RETURN n", exceptionFactory)
    val expected = parser.parse("MATCH (n)-[r:Foo]->(`  NODE20`) RETURN n", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("name all RelationshipPatterns in Query") {
    val original = parser.parse("MATCH (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n", exceptionFactory)
    val expected = parser.parse("MATCH (n)-[`  REL10`:Foo]->(m) WHERE (n)-[`  REL32`:Bar]->(m) RETURN n", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("rename unnamed varlength paths") {
    val original = parser.parse("MATCH (n)-[:Foo*]->(m) RETURN n", exceptionFactory)
    val expected = parser.parse("MATCH (n)-[`  REL10`:Foo*]->(m) RETURN n", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("match (a) create (a)-[:X]->() return a") {
    val original = parser.parse("match (a) create (a)-[:X]->() return a", exceptionFactory)
    val expected = parser.parse("match (a) create (a)-[`  REL21`:X]->(`  NODE28`) return a", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("merge (a) merge p = (a)-[:R]->() return p") {
    val original = parser.parse("merge (a) merge p = (a)-[:R]->() return p", exceptionFactory)
    val expected = parser.parse("merge (a) merge p = (a)-[`  REL24`:R]->(`  NODE31`) return p", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("merge (a)-[:R]->() return a") {
    val original = parser.parse("merge (a)-[:R]->() return a", exceptionFactory)
    val expected = parser.parse("merge (a)-[`  REL10`:R]->(`  NODE17`) return a", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("does not touch parameters") {
    val original = parser.parse("MATCH (n)-[r:Foo]->($p) RETURN n", exceptionFactory)
    val expected = parser.parse("MATCH (n)-[r:Foo]->(`  NODE20` $p) RETURN n", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("names all unnamed var length relationships") {
    val original = parser.parse("MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *", exceptionFactory)
    val expected = parser.parse("MATCH (a:Artist)-[`  REL17`:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("should name all pattern elements in a pattern comprehension") {
    val original = parser.parse("RETURN [()-->() | 'foo'] AS foo", exceptionFactory)
    val expected = parser.parse("RETURN [(`  NODE9`)-[`  REL11`]->(`  NODE14`) | 'foo'] AS foo", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("should name all pattern elements in a pattern expressions") {
    val original = parser.parse("MATCH (a) WHERE (a)-[:R]->()", exceptionFactory)
    val expected = parser.parse("MATCH (a) WHERE (a)-[`  REL20`:R]->(`  NODE27`)", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
  }

  test("should name all pattern elements in a EXISTS") {
    val original = parser.parse("MATCH (a) WHERE EXISTS { MATCH (a)-[:R]->() }", exceptionFactory)
    val expected = parser.parse("MATCH (a) WHERE EXISTS { MATCH (a)-[`  REL35`:R]->(`  NODE42`) }", exceptionFactory)

    val result = original.rewrite(nameAllPatternElements)
    assert(result === expected)
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
