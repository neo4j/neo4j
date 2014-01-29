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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.scalatest.FunSuite
import org.neo4j.cypher.internal.compiler.v2_0.{SemanticState, DummyPosition}
import org.neo4j.cypher.internal.compiler.v2_0.symbols._

class LoadCsvTest extends FunSuite {
  val literal = StringLiteral("yo mama")(DummyPosition(4))
  val identifier = Identifier("a")(DummyPosition(4))

  test("cannot overwrite existing identifier") {
    val loadCsv = LoadCSV(true, literal, identifier, None, None)(DummyPosition(6))
    val result = loadCsv.semanticCheck(SemanticState.clean)
    assert(result.errors === Seq())
  }

  test("when expecting headers, the identifier has a map type") {
    val loadCsv = LoadCSV(true, literal, identifier, None, None)(DummyPosition(6))
    val result = loadCsv.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTMap.invariant)
  }

  test("when not expecting headers, the identifier has a collection type") {
    val loadCsv = LoadCSV(false, literal, identifier, None, None)(DummyPosition(6))
    val result = loadCsv.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(identifier).actual

    assert(expressionType === CTCollection(CTAny).invariant)
  }
}