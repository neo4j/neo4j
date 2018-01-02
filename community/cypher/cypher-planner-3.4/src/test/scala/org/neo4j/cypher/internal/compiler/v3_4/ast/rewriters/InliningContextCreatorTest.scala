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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_4.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class InliningContextCreatorTest extends CypherFunSuite with AstRewritingTestSupport {

  val identA  = varFor("a")
  val identB  = varFor("b")
  val identR  = varFor("r")
  val identP  = varFor("p")
  val identX1 = varFor("x1")
  val identX2 = varFor("x2")

  test("should not spoil aliased node variables") {
    val ast = parser.parse("match (a) with a as b match (b) return b")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should ignore named shortest paths") {
    val ast = parser.parse("match p = shortestPath((a)-[r]->(b)) return p")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }

  test("should not spoil aliased relationship variables") {
    val ast = parser.parse("match ()-[a]->() with a as b match ()-[b]->() return b")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should spoil all the variables when WITH has aggregations") {
    val ast = parser.parse("match (a)-[r]->(b) with a as `x1`, count(r) as `x2` return x1, x2")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }
}
