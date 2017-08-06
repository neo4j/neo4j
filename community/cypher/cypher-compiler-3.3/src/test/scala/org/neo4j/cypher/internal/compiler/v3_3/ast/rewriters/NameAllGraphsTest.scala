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
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.nameAllGraphs
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class NameAllGraphsTest extends CypherFunSuite {

  import parser.ParserFixture._

  test("do not name no graph") {
    val original = parser.parse("FROM - RETURN 1")
    val expected = parser.parse("FROM - RETURN 1")

    val result = original.rewrite(nameAllGraphs)
    assert(result === expected)
  }

  test("name named graphs") {
    val original = parser.parse("FROM foo RETURN 1")
    val expected = parser.parse("FROM foo RETURN 1")

    val result = original.rewrite(nameAllGraphs)
    assert(result === expected)
  }

  test("name new graph") {
    val original = parser.parse("FROM NEW GRAPH RETURN 1")
    val expected = parser.parse("FROM NEW GRAPH AS `  UNNAMED16` RETURN 1")

    val result = original.rewrite(nameAllGraphs)
    assert(result === expected)
  }

  test("name load graph") {
    val original = parser.parse("FROM GRAPH AT 'foo' RETURN 1")
    val expected = parser.parse("FROM GRAPH AT 'foo' AS `  UNNAMED21` RETURN 1")

    val result = original.rewrite(nameAllGraphs)
    assert(result === expected)
  }

  test("name copy graph") {
    val original = parser.parse("FROM COPY foo RETURN 1")
    val expected = parser.parse("FROM COPY foo AS `  UNNAMED15` RETURN 1")

    val result = original.rewrite(nameAllGraphs)
    assert(result === expected)
  }
}
