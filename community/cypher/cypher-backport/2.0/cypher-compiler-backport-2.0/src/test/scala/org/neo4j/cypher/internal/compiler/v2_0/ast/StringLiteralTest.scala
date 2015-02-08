/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0.{SemanticError, SemanticState, DummyPosition}
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.commons.CypherFunSuite

class StringLiteralTest extends CypherFunSuite {
  test("has type CTString") {
    val literal = StringLiteral("foo")(DummyPosition(0))
    val result = literal.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    val expressionType = result.state.expressionType(literal).actual

    assert(expressionType === CTString.invariant)
  }

  test("should recognise URLs") {
    val literal = StringLiteral("http://foo.bar")(DummyPosition(4))
    val result = literal.checkURL(SemanticState.clean)
    assert(result.errors.isEmpty)
    assert(literal.asURL.getProtocol === "http")
    assert(literal.asURL.getHost === "foo.bar")
  }

  test("should recognise invalid URLs") {
    val literal = StringLiteral("foo.bar")(DummyPosition(4))
    val result = literal.checkURL(SemanticState.clean)
    assert(result.errors === Vector(SemanticError("invalid URL specified (no protocol: foo.bar)", DummyPosition(4))))
  }
}
