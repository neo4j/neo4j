/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticError, SemanticState}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.{StringLiteral, Variable}

class LoadCSVTest extends CypherFunSuite {

  val literalURL = StringLiteral("file:///tmp/foo.csv")(DummyPosition(4))
  val variable = Variable("a")(DummyPosition(4))

  test("cannot overwrite existing variable") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, variable, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Seq())
  }

  test("when expecting headers, the variable has a map type") {
    val loadCSV = LoadCSV(withHeaders = true, literalURL, variable, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(variable).actual

    assert(expressionType === CTMap.invariant)
  }

  test("when not expecting headers, the variable has a list type") {
    val loadCSV = LoadCSV(withHeaders = false, literalURL, variable, None)(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    val expressionType = result.state.expressionType(variable).actual

    assert(expressionType === CTList(CTString).invariant)
  }

  test("should accept one-character wide field terminators") {
    val literal = StringLiteral("http://example.com/foo.csv")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, variable, Some(StringLiteral("\t")(DummyPosition(0))))(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector.empty)
  }

  test("should reject more-than-one-character wide field terminators") {
    val literal = StringLiteral("http://example.com/foo.csv")(DummyPosition(4))
    val loadCSV = LoadCSV(withHeaders = false, literal, variable, Some(StringLiteral("  ")(DummyPosition(0))))(DummyPosition(6))
    val result = loadCSV.semanticCheck(SemanticState.clean)
    assert(result.errors === Vector(SemanticError("CSV field terminator can only be one character wide", DummyPosition(0))))
  }
}
