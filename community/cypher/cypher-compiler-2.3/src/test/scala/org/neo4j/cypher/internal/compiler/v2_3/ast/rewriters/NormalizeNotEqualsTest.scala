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

import org.neo4j.cypher.internal.frontend.v2_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NormalizeNotEqualsTest extends CypherFunSuite {

  val pos = DummyPosition(0)
  val lhs: Expression = StringLiteral("42")(pos)
  val rhs: Expression = StringLiteral("42")(pos)

  test("notEquals  iff  not(equals)") {
    val notEquals = NotEquals(lhs, rhs)(pos)
    val output = notEquals.rewrite(normalizeNotEquals)
    val expected: Expression = Not(Equals(lhs, rhs)(pos))(pos)
    output should equal(expected)
  }

  test("should do nothing on other expressions") {
    val output = lhs.rewrite(normalizeNotEquals)
    output should equal(lhs)
  }
}
