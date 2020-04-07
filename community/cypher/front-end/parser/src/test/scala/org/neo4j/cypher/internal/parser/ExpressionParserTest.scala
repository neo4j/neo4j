/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions
import org.parboiled.scala.Rule1

class ExpressionParserTest
  extends ParserAstTest[expressions.Expression]
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[expressions.Expression] = Expression

  test("a ~ b") {
    yields(expressions.Equivalent(varFor("a"), varFor("b")))
  }

  test("[] ~ []") {
    yields(expressions.Equivalent(listOf(), listOf()))
  }

  test("thing CONTAINS 'a' + 'b'") {
    yields(expressions.Contains(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing STARTS WITH 'a' + 'b'") {
    yields(expressions.StartsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing ENDS WITH 'a' + 'b'") {
    yields(expressions.EndsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }
}
