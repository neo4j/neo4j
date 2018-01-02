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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NameMatchPatternElementTest extends CypherFunSuite {

  import parser.ParserFixture._

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

  test("match a create unique (a)-[:X]->() return a") {
    val original = parser.parse("match a create unique p=(a)-[:X]->() return p")
    val expected = parser.parse("match a create unique p=(a)-[`  UNNAMED28`:X]->(`  UNNAMED35`) return p")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("match a create (a)-[:X]->() return a") {
    val original = parser.parse("match a create (a)-[:X]->() return a")
    val expected = parser.parse("match a create (a)-[`  UNNAMED19`:X]->(`  UNNAMED26`) return a")

    val result = original.rewrite(nameUpdatingClauses)
    assert(result === expected)
  }

  test("merge (a) merge p = (a)-[:R]->() return p") {
    val original = parser.parse("merge (a) merge p = (a)-[:R]->() return p")
    val expected = parser.parse("merge (a) merge p = (a)-[`  UNNAMED24`:R]->(`  UNNAMED31`) return p")

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
