/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_5._
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.rewriting.rewriters.normalizeGraphReturnItems
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.{SignedDecimalIntegerLiteral, StringLiteral}

class normalizeGraphReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  import org.opencypher.v9_0.parser.ParserFixture._

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
