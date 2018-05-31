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
package org.neo4j.cypher.internal.compiler.v3_5.ast.conditions

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions._

class ContainsNamedPathOnlyForShortestPathTest extends CypherFunSuite with AstConstructionTestSupport {
  private val condition: (Any => Seq[String]) = containsNamedPathOnlyForShortestPath

  test("happy when we have no named paths") {
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(varFor("n")), Seq.empty, None)(pos))))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos)))(pos), None, None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) shouldBe empty
  }

  test("unhappy when we have a named path") {
    val namedPattern: NamedPatternPart = NamedPatternPart(varFor("p"), EveryPath(NodePattern(Some(varFor("n")), Seq.empty, None)(pos)))(pos)
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(namedPattern))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos)))(pos), None, None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) should equal(Seq(s"Expected none but found $namedPattern at position $pos"))
  }

  test("should allow named path for shortest path") {
    val ast = Query(None, SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(NamedPatternPart(varFor("p"), ShortestPaths(NodePattern(Some(varFor("n")), Seq.empty, None)(pos), single = true)(pos))(pos)))(pos), Seq.empty, None)(pos),
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos)))(pos), None, None, None, None)(pos)
    ))(pos))(pos)

    condition(ast) shouldBe empty
  }
}
