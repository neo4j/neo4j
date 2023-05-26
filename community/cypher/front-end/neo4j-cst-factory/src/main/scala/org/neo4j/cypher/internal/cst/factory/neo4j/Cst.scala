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
package org.neo4j.cypher.internal.cst.factory.neo4j

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.parser.CypherParser.CallClauseContext
import org.neo4j.cypher.internal.parser.CypherParser.CaseExpressionContext
import org.neo4j.cypher.internal.parser.CypherParser.ClauseContext
import org.neo4j.cypher.internal.parser.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.parser.CypherParser.FunctionInvocationContext
import org.neo4j.cypher.internal.parser.CypherParser.ListComprehensionContext
import org.neo4j.cypher.internal.parser.CypherParser.MapLiteralContext
import org.neo4j.cypher.internal.parser.CypherParser.MapProjectionContext
import org.neo4j.cypher.internal.parser.CypherParser.MatchClauseContext
import org.neo4j.cypher.internal.parser.CypherParser.NodePatternContext
import org.neo4j.cypher.internal.parser.CypherParser.NumberLiteralContext
import org.neo4j.cypher.internal.parser.CypherParser.ParameterContext
import org.neo4j.cypher.internal.parser.CypherParser.ParenthesizedPathContext
import org.neo4j.cypher.internal.parser.CypherParser.PatternComprehensionContext
import org.neo4j.cypher.internal.parser.CypherParser.PatternContext
import org.neo4j.cypher.internal.parser.CypherParser.QuantifierContext
import org.neo4j.cypher.internal.parser.CypherParser.RelationshipPatternContext
import org.neo4j.cypher.internal.parser.CypherParser.StatementContext
import org.neo4j.cypher.internal.parser.CypherParser.StringLiteralContext
import org.neo4j.cypher.internal.parser.CypherParser.SubqueryClauseContext
import org.neo4j.cypher.internal.parser.CypherParser.UseClauseContext
import org.neo4j.cypher.internal.parser.CypherParser.VariableContext

abstract class Cst[+T <: ParserRuleContext](val ctx: T) {
  val parsingErrors: List[Exception]
}

object Cst {
  type CallClause = CallClauseContext
  type CaseExpression = CaseExpressionContext
  type Clause = ClauseContext
  type MatchClause = MatchClauseContext
  type Expression = ExpressionContext
  type FunctionInvocation = FunctionInvocationContext
  type ListComprehension = ListComprehensionContext
  type MapLiteral = MapLiteralContext
  type MapProjection = MapProjectionContext
  type NodePattern = NodePatternContext
  type NumberLiteral = NumberLiteralContext
  type Parameter = ParameterContext
  type ParenthesizedPath = ParenthesizedPathContext
  type PatternComprehension = PatternComprehensionContext
  type Quantifier = QuantifierContext
  type RelationshipPattern = RelationshipPatternContext
  type Pattern = PatternContext
  type Statement = StatementContext
  type UseClause = UseClauseContext
  type Statements = StatementContext
  type StringLiteral = StringLiteralContext
  type SubqueryClause = SubqueryClauseContext
  type Variable = VariableContext
}
