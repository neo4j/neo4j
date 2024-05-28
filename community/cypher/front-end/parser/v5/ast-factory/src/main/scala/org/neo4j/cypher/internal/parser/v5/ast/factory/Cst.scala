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
package org.neo4j.cypher.internal.parser.v5.ast.factory

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.CallClauseContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.CaseExpressionContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.ClauseContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.ExpressionContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.FunctionInvocationContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.ListComprehensionContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.LiteralContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.MapContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.MapProjectionContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.MatchClauseContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.NodePatternContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.NumberLiteralContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.ParameterContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.ParenthesizedPathContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.PatternComprehensionContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.PatternContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.QuantifierContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.RelationshipPatternContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.StatementContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.StatementsContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.StringLiteralContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.SubqueryClauseContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.UseClauseContext
import org.neo4j.cypher.internal.parser.v5.CypherParser.VariableContext
import org.neo4j.cypher.internal.util.ASTNode

abstract class Cst[+T <: ParserRuleContext](val ctx: T) {
  val parsingErrors: List[Exception]
  def ast: Option[ASTNode]
}

object Cst {
  type CallClause = CallClauseContext
  type CaseExpression = CaseExpressionContext
  type Clause = ClauseContext
  type MatchClause = MatchClauseContext
  type Expression = ExpressionContext
  type FunctionInvocation = FunctionInvocationContext
  type ListComprehension = ListComprehensionContext
  type Map = MapContext
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
  type Statements = StatementsContext
  type StringLiteral = StringLiteralContext
  type SubqueryClause = SubqueryClauseContext
  type Variable = VariableContext
  type Literal = LiteralContext
}
