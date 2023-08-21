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

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

class FloatLiteralParserTest extends ParserSyntaxTreeBase[ParserRuleContext, ASTNode] {

  test("float literals fail to parse in expressions") {
    implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
    implicit val antlrRule: AntlrRule[Cst.Expression] = AntlrRule.Expression

    parsing("NaN") shouldGive NaNLiteral
    parsing("nan") shouldGive NaNLiteral
    parsing("nAn") shouldGive NaNLiteral
    parsing("Inf") shouldGive InfinityLiteral
    parsing("inf") shouldGive InfinityLiteral
    parsing("Infinity") shouldGive InfinityLiteral
    parsing("infinity") shouldGive InfinityLiteral

    parsing("-infinity") shouldGive unarySubtract(InfinityLiteral)
    parsing("-inf") shouldGive unarySubtract(InfinityLiteral)
    parsing("1 - infinity") shouldGive subtract(literalInt(1), InfinityLiteral)
    parsing("infinity > 0") shouldGive greaterThan(InfinityLiteral, literalInt(0))
    parsing("CASE WHEN NaN THEN infinity END") shouldGive caseExpression(None, None, (NaNLiteral, InfinityLiteral))
    parsing("{inf: infinity, nan: NaN}") shouldGive mapOf(("inf", InfinityLiteral), ("nan", NaNLiteral))
    parsing("[inf, Infinity, NaN]") shouldGive listOf(InfinityLiteral, InfinityLiteral, NaNLiteral)
  }

  test("float literals parse as a variable name") {
    implicit val javaccRule: JavaccRule[Variable] = JavaccRule.Variable
    implicit val antlrRule: AntlrRule[Cst.Variable] = AntlrRule.Variable

    parsing("NaN") shouldGive varFor("NaN")
    parsing("nan") shouldGive varFor("nan")
    parsing("nAn") shouldGive varFor("nAn")
    parsing("Inf") shouldGive varFor("Inf")
    parsing("inf") shouldGive varFor("inf")
    parsing("Infinity") shouldGive varFor("Infinity")
    parsing("infinity") shouldGive varFor("infinity")
  }
}
