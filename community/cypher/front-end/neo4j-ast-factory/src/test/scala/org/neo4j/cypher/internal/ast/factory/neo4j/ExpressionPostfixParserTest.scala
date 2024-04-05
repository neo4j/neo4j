/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral

class ExpressionPostfixParserTest extends AstParsingTestBase {

  test("v.ab") {
    parsesTo[Expression](prop("v", "ab"))
  }

  test("v.`ab`") {
    parsesTo[Expression](prop("v", "ab"))
  }

  test("v.`1b`") {
    parsesTo[Expression](prop("v", "1b"))
  }

  test("`v`.`1`") {
    parsesTo[Expression](prop("v", "1"))
  }

  test("v.ab.`c`.d.`e`.f.g") {
    parsesTo[Expression](prop(prop(prop(prop(prop(prop("v", "ab"), "c"), "d"), "e"), "f"), "g"))
  }

  test("return v.1") {
    failsParsing[Statements]
  }

  test("a[0]") {
    parsesTo[Expression](containerIndex(varFor("a"), 0))
  }

  test("a[0][1][2][3][4][5][6][7]") {
    parsesTo[Expression](
      Range(0, 8).foldLeft[Expression](varFor("a")) { case (acc, i) => containerIndex(acc, i) }
    )
  }

  test(s"a[${Long.MaxValue}]") {
    parsesTo[Expression](containerIndex(varFor("a"), literal(Long.MaxValue)))
  }

  test(s"a[${Long.MaxValue}0]") {
    parsesTo[Expression](containerIndex(varFor("a"), SignedDecimalIntegerLiteral(Long.MaxValue.toString + "0")(pos)))
  }

  test("a[-1]") {
    parsesTo[Expression](containerIndex(varFor("a"), -1))
  }

  test("a.[]") {
    failsParsing[Expression]
  }

  test("a[0..1]") {
    parsesTo[Expression](sliceFull(varFor("a"), literal(0), literal(1)))
  }

  test("a[..1]") {
    parsesTo[Expression](sliceTo(varFor("a"), literal(1)))
  }

  test("a[0..]") {
    parsesTo[Expression](sliceFrom(varFor("a"), literal(0)))
  }

  test("a[..]") {
    parsesTo[Expression](ListSlice(varFor("a"), None, None)(pos))
  }
}
