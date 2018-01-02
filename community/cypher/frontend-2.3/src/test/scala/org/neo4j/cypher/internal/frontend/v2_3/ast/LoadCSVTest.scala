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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticError, SemanticState}

class LoadCSVTest extends CypherFunSuite {

  val literalURL = StringLiteral("file:///tmp/foo.csv")(DummyPosition(4))
  val identifier = Identifier("a")(DummyPosition(4))

  test("cannot overwrite existing identifier") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, identifier, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Seq())
  }

  test("when expecting headers, the identifier has a map type") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, identifier, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTMap.invariant)
  }

  test("when not expecting headers, the identifier has a collection type") {
    val loadCSV = LoadCSV(withHeaders = false, literalURL, identifier, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTCollection(CTString).invariant)
  }

  test("should accept one-character wide field terminators") {
    val literal = StringLiteral("http://example.com/foo.csv")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, identifier, Some(StringLiteral("\t")(DummyPosition(0))))(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector.empty)
  }

  test("should reject more-than-one-character wide field terminators") {
    val literal = StringLiteral("http://example.com/foo.csv")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, identifier, Some(StringLiteral("  ")(DummyPosition(0))))(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector(SemanticError("CSV field terminator can only be one character wide", DummyPosition(0))))
  }
}
