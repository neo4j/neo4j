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

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.commons.CypherFunSuite

class NameMatchPatternElementTest extends CypherFunSuite {

  import parser.ParserFixture._

  test("name all NodePatterns in Query" ) {
    val original = parser.parse("MATCH (n)-[r:Foo]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->(`  UNNAMED20`) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("name all RelationshipPatterns in Query") {
    val original = parser.parse("MATCH (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")
    val expected = parser.parse("MATCH (n)-[`  UNNAMED9`:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)
    assert(result === expected)
  }

  test("name all pattern predicates in a query") {
    val original = parser.parse("MATCH (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")
    val expected = parser.parse("MATCH (n)-[:Foo]->(m) WHERE (n)-[`  UNNAMED31`:Bar]->(m) RETURN n")

    val result = original.rewrite(namePatternPredicates)
    assert(result === expected)
  }

  test("don't rename unnamed varlength paths while the legacy planner is still around") {
    val original = parser.parse("MATCH (n)-[:Foo*]->(m) RETURN n")

    val result = original.rewrite(nameMatchPatternElements)

    assert(result === original)
  }

  test("rename unnamed varlength paths") {
    val original = parser.parse("MATCH (n)-[:Foo*]->(m) RETURN n")
    val expected = parser.parse("MATCH (n)-[`  UNNAMED9`:Foo*]->(m) RETURN n")

    val result = original.rewrite(nameVarLengthRelationships)
    assert(result === expected)
  }
}
