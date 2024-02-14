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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.expressions.Expression

class FloatLiteralParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("float literals fail to parse in expressions") {
    "NaN" should parseTo[Expression](NaNLiteral)
    "nan" should parseTo[Expression](NaNLiteral)
    "nAn" should parseTo[Expression](NaNLiteral)
    "Inf" should parseTo[Expression](InfinityLiteral)
    "inf" should parseTo[Expression](InfinityLiteral)
    "Infinity" should parseTo[Expression](InfinityLiteral)
    "infinity" should parseTo[Expression](InfinityLiteral)

    "-infinity" should parseTo[Expression](unarySubtract(InfinityLiteral))
    "-inf" should parseTo[Expression](unarySubtract(InfinityLiteral))
    "1 - infinity" should parseTo[Expression](subtract(literalInt(1), InfinityLiteral))
    "infinity > 0" should parseTo[Expression](greaterThan(InfinityLiteral, literalInt(0)))
    "CASE WHEN NaN THEN infinity END" should parseTo[Expression](NotAntlr)(caseExpression(
      None,
      None,
      (NaNLiteral, InfinityLiteral)
    ))
    "{inf: infinity, nan: NaN}" should parseTo[Expression](mapOf(
      ("inf", InfinityLiteral),
      ("nan", NaNLiteral)
    ))
    "[inf, Infinity, NaN]" should parseTo[Expression](NotAntlr)(listOf(InfinityLiteral, InfinityLiteral, NaNLiteral))
  }
}
