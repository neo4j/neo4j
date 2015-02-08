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

class LoadCSVTest extends CypherFunSuite {
  val literalURL = StringLiteral("file:///tmp/foo.csv")(DummyPosition(4))
  val identifier = Identifier("a")(DummyPosition(4))

  test("cannot overwrite existing identifier") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, identifier, None, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Seq())
  }

  test("when expecting headers, the identifier has a map type") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, identifier, None, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTMap.invariant)
  }

  test("when not expecting headers, the identifier has a collection type") {
    val loadCSV = LoadCSV(withHeaders = false, literalURL, identifier, None, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTCollection(CTString).invariant)
  }

  test("should reject URLs that are not file://, http://, https://, ftp://") {
    val literal = StringLiteral("morsecorba://sos")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, identifier, None, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector(SemanticError("invalid URL specified (unknown protocol: morsecorba)", DummyPosition(4))))
  }

  test("should accept http:// URLs") {
    val literal = StringLiteral("http://example.com/foo.csv")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, identifier, None, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector.empty)
  }
}
