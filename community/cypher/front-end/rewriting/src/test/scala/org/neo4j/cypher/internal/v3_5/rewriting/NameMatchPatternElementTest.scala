/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{nameMatchPatternElements, nameUpdatingClauses}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite


class NameMatchPatternElementTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.v3_5.parser.ParserFixture._

  test("name all NodePatterns in Query") {
    val original = parser.parse("MATCH (n)-[r:Foo]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->(`  UNNAMED20`) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("name all RelationshipPatterns in Query") {
    val original = parser.parse("MATCH (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")
    val expected = parser.parse("MATCH (n)-[`  UNNAMED10`:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("rename unnamed varlength paths") {
    val original = parser.parse("MATCH (n)-[:Foo*]->(m) RETURN n")
    val expected = parser.parse("MATCH (n)-[`  UNNAMED10`:Foo*]->(m) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("match (a) create unique (a)-[:X]->() return a") {
    val original = parser.parse("match (a) create unique p=(a)-[:X]->() return p")
    val expected = parser.parse("match (a) create unique p=(a)-[`  UNNAMED30`:X]->(`  UNNAMED37`) return p")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("match (a) create (a)-[:X]->() return a") {
    val original = parser.parse("match (a) create (a)-[:X]->() return a")
    val expected = parser.parse("match (a) create (a)-[`  UNNAMED21`:X]->(`  UNNAMED28`) return a")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("merge (a) merge p = (a)-[:R]->() return p") {
    val original = parser.parse("merge (a) merge p = (a)-[:R]->() return p")
    val expected = parser.parse("merge (a) merge p = (a)-[`  UNNAMED24`:R]->(`  UNNAMED31`) return p")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("merge (a)-[:R]->() return a") {
    val original = parser.parse("merge (a)-[:R]->() return a")
    val expected = parser.parse("merge (a)-[`  UNNAMED10`:R]->(`  UNNAMED17`) return a")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("does not touch parameters") {
    val original = parser.parse("MATCH (n)-[r:Foo]->({p}) RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->(`  UNNAMED20` {p}) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("names all unnamed var length relationships") {
    val original = parser.parse("MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *")
    val expected = parser.parse("MATCH (a:Artist)-[`  UNNAMED17`:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }
}
