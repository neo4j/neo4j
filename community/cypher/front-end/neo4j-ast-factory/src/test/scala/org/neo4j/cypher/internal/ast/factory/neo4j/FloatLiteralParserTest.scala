/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.util.ASTNode

class FloatLiteralParserTest extends ParserSyntaxTreeBase[ParserRuleContext, ASTNode] {

  test("float literals fail to parse in expressions") {
    implicit val javaccRule = JavaccRule.Expression
    implicit val antlrRule = AntlrRule.Expression

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
    implicit val javaccRule = JavaccRule.Variable
    implicit val antlrRule = AntlrRule.Variable

    parsing("NaN") shouldGive varFor("NaN")
    parsing("nan") shouldGive varFor("nan")
    parsing("nAn") shouldGive varFor("nAn")
    parsing("Inf") shouldGive varFor("Inf")
    parsing("inf") shouldGive varFor("inf")
    parsing("Infinity") shouldGive varFor("Infinity")
    parsing("infinity") shouldGive varFor("infinity")
  }
}
