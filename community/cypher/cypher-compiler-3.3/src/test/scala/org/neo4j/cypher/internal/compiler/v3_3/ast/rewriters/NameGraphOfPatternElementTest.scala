/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.nameGraphOfPatternElements
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class NameGraphOfPatternElementTest extends CypherFunSuite {

  import parser.ParserFixture._

  test("name all node patterns in GRAPH OF") {
    val original = parser.parse("RETURN GRAPH OF (n)-[r:Foo]->() RETURN n")
    val expected = parser.parse("RETURN GRAPH OF (n)-[r:Foo]->(`  UNNAMED30`) RETURN n")

    val result = original.rewrite(nameGraphOfPatternElements)
    assert(result === expected)
  }

  test("name all relationship patterns in GRAPH OF") {
    val original = parser.parse("WITH 1 AS a GRAPH OF (n)-[:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")
    val expected = parser.parse("WITH 1 AS a GRAPH OF (n)-[`  UNNAMED25`:Foo]->(m) WHERE (n)-[:Bar]->(m) RETURN n")

    val result = original.rewrite(nameGraphOfPatternElements)
    assert(result === expected)
  }
}
