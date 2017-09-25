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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.normalizeGraphReturnItems
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite

class normalizeGraphReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  import parser.ParserFixture._

  test("do not rename source graph") {
    val original = parser.parse("FROM GRAPH foo AT 'url' WITH * SOURCE GRAPH RETURN 1")

    val result = original.rewrite(normalizeGraphReturnItems)
    assert(result === original)
  }

  test("do not rename TARGET graph") {
    val original = parser.parse("INTO GRAPH foo AT 'url' WITH * TARGET GRAPH RETURN 1")

    val result = original.rewrite(normalizeGraphReturnItems)
    assert(result === original)
  }

  test("name named graphs") {
    val original = parser.parse("FROM GRAPH foo RETURN 1")
    val expected = parser.parse("FROM GRAPH foo AS foo RETURN 1")

    val result = original.rewrite(normalizeGraphReturnItems)
    assert(result === expected)
  }

  test("name load graph") {
    // need to spell out ast here as there is no syntax for specifying generated symbols
    val original = parser.parse("FROM GRAPH AT 'url' RETURN 1")
    val expected = Query(None,
      SingleQuery(List(
        With(distinct = false,
          ReturnItems(includeExisting = true, List())(pos),
          GraphReturnItems(true,List(
            NewContextGraphs(
              GraphAtAs(
                GraphUrl(Right(StringLiteral("url")(pos)))(pos),
                Some(varFor("  FRESHID21")),
                generated = true)(pos),None)(pos)))(pos),None,None,None,None)(pos),
        Return(distinct = false,
          ReturnItems(includeExisting = false,List(
            UnaliasedReturnItem(SignedDecimalIntegerLiteral("1")(pos), "1")(pos)))(pos),None,None,None,None,Set())(pos)))(pos))(pos)

    val result = original.rewrite(normalizeGraphReturnItems)
    assert(result === expected)
  }
}
