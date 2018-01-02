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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ContainsNamedPathOnlyForShortestPathTest extends CypherFunSuite with AstConstructionTestSupport {
  private val condition: (Any => Seq[String]) = containsNamedPathOnlyForShortestPath

  test("happy when we have no named paths") {
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(ident("n")), Seq.empty, None, naked = false)(pos))))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(ident("n"), ident("n"))(pos)))(pos), None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) shouldBe empty
  }

  test("unhappy when we have a named path") {
    val namedPattern: NamedPatternPart = NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("n")), Seq.empty, None, naked = false)(pos)))(pos)
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(namedPattern))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(ident("n"), ident("n"))(pos)))(pos), None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) should equal(Seq(s"Expected none but found $namedPattern at position $pos"))
  }

  test("should allow named path for shortest path") {
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), ShortestPaths(NodePattern(Some(ident("n")), Seq.empty, None, naked = false)(pos), single = true)(pos))(pos)))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(ident("n"), ident("n"))(pos)))(pos), None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) shouldBe empty
  }
}
