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

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable

class FloatLiteralParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("float literals fail to parse in expressions") {
    parsing[Expression]("NaN") shouldGive NaNLiteral
    parsing[Expression]("nan") shouldGive NaNLiteral
    parsing[Expression]("nAn") shouldGive NaNLiteral
    parsing[Expression]("Inf") shouldGive InfinityLiteral
    parsing[Expression]("inf") shouldGive InfinityLiteral
    parsing[Expression]("Infinity") shouldGive InfinityLiteral
    parsing[Expression]("infinity") shouldGive InfinityLiteral

    parsing[Expression]("-infinity") shouldGive unarySubtract(InfinityLiteral)
    parsing[Expression]("-inf") shouldGive unarySubtract(InfinityLiteral)
    parsing[Expression]("1 - infinity") shouldGive subtract(literalInt(1), InfinityLiteral)
    parsing[Expression]("infinity > 0") shouldGive greaterThan(InfinityLiteral, literalInt(0))
    parsing[Expression]("CASE WHEN NaN THEN infinity END") shouldGive caseExpression(
      None,
      None,
      (NaNLiteral, InfinityLiteral)
    )
    parsing[Expression]("{inf: infinity, nan: NaN}") shouldGive mapOf(("inf", InfinityLiteral), ("nan", NaNLiteral))
    parsing[Expression]("[inf, Infinity, NaN]") shouldGive listOf(InfinityLiteral, InfinityLiteral, NaNLiteral)
  }

  test("float literals parse as a variable name") {
    parsing[Variable]("NaN") shouldGive varFor("NaN")
    parsing[Variable]("nan") shouldGive varFor("nan")
    parsing[Variable]("nAn") shouldGive varFor("nAn")
    parsing[Variable]("Inf") shouldGive varFor("Inf")
    parsing[Variable]("inf") shouldGive varFor("inf")
    parsing[Variable]("Infinity") shouldGive varFor("Infinity")
    parsing[Variable]("infinity") shouldGive varFor("infinity")
  }
}
