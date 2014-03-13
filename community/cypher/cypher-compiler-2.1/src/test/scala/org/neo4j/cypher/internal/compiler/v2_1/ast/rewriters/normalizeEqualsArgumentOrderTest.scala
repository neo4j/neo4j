/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.ast.{FunctionInvocation, SignedIntegerLiteral, Equals, Identifier}

class NormalizeEqualsArgumentOrderTest extends CypherFunSuite {

  val pos = DummyPosition(0)

  test("a = b rewritten to: a = b") {
    val input = Equals(Identifier("a")(pos), Identifier("b")(pos))(pos)

    normalizeEqualsArgumentOrder(input) should equal(Some(input))
  }

  test("12 = a rewritten to: a = 12") {
    val lhs = SignedIntegerLiteral("12")(pos)
    val rhs = Identifier("a")(pos)

    normalizeEqualsArgumentOrder(Equals(lhs, rhs)(pos)) should equal(Some(Equals(rhs, lhs)(pos)))
  }

  test("id(a) = id(b) rewritten to: id(a) = id(b)") {
    val input = Equals(id("a"), id("b"))(pos)

    normalizeEqualsArgumentOrder(input) should equal(Some(input))
  }

  test("23 = id(a) rewritten to: id(a) = 23") {
    val lhs = SignedIntegerLiteral("12")(pos)
    val rhs = id("a")

    normalizeEqualsArgumentOrder(Equals(lhs, rhs)(pos)) should equal(Some(Equals(rhs, lhs)(pos)))
  }

  private def id(name: String) = FunctionInvocation(Identifier("id")(pos), distinct = false, Array(Identifier(name)(pos)))(pos)
}
