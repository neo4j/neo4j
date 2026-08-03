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
package org.neo4j.cypher.internal.parser.v25.ast.factory

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.VocabularyImpl
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.DatabaseNameRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.GraphPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.IdentifierRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.InterpolatedStringLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpression1Rule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NodePatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NumberLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ParameterRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.RelationshipPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.StringLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.VariableRule
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser

import java.util

class Cypher25ErrorStrategyConf extends CypherErrorStrategy.Conf {

  override def vocabulary: VocabularyImpl = Cypher25Parser.VOCABULARY.asInstanceOf[VocabularyImpl]

  override def ignoredTokens: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    Token.EPSILON,
    Cypher25Parser.SEMICOLON,
    // Feature flag based exclusions:
    Cypher25Parser.DEFINE, // Feature flagged keyword, remove when DEFINE is released.
    Cypher25Parser.OBFUSCATION
  )

  override def customTokenDisplayNames: Map[Int, String] = Map(
    Cypher25Parser.DOT_METRIC -> "'DOT'",
    Cypher25Parser.SPACE -> "' '",
    Cypher25Parser.SINGLE_LINE_COMMENT -> "'//'",
    Cypher25Parser.DECIMAL_DOUBLE -> "a float value",
    Cypher25Parser.UNSIGNED_DECIMAL_INTEGER -> "an integer value",
    Cypher25Parser.UNSIGNED_HEX_INTEGER -> "a hexadecimal integer value",
    Cypher25Parser.UNSIGNED_OCTAL_INTEGER -> "an octal integer value",
    Cypher25Parser.EXTENDED_IDENTIFIER -> "an identifier",
    Cypher25Parser.IDENTIFIER -> "an identifier",
    Cypher25Parser.ARROW_LINE -> "'-'",
    Cypher25Parser.ARROW_LEFT_HEAD -> "'<'",
    Cypher25Parser.ARROW_RIGHT_HEAD -> "'>'",
    Cypher25Parser.MULTI_LINE_COMMENT -> "'/*'",
    Cypher25Parser.STRING_LITERAL1 -> "a string value",
    Cypher25Parser.STRING_LITERAL2 -> "a string value",
    Cypher25Parser.INTERPOLATED_START_SINGLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_END_SINGLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_EXPR_START_SINGLE -> "'{'",
    Cypher25Parser.INTERPOLATED_TEXT_SINGLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_START_DOUBLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_END_DOUBLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_EXPR_START_DOUBLE -> "'{'",
    Cypher25Parser.INTERPOLATED_TEXT_DOUBLE -> "an interpolated string value",
    Cypher25Parser.INTERPOLATED_UNEXPECTED_RCURLY_SINGLE -> "'}'",
    Cypher25Parser.INTERPOLATED_UNEXPECTED_RCURLY_DOUBLE -> "'}'",
    Cypher25Parser.ESCAPED_SYMBOLIC_NAME -> "an identifier",
    Cypher25Parser.ALL_SHORTEST_PATHS -> "'allShortestPaths'",
    Cypher25Parser.SHORTEST_PATH -> "'shortestPath'",
    Cypher25Parser.LIMITROWS -> "'LIMIT'",
    Cypher25Parser.SKIPROWS -> "'SKIP'",
    Cypher25Parser.ALLREDUCE -> "'allReduce'",
    Cypher25Parser.LCURLY -> "'{'",
    Token.EOF -> "<EOF>"
  )

  override def ruleGroups: Map[Int, CypherErrorStrategy.CypherRuleGroup] = {
    Map(
      Cypher25Parser.RULE_expression -> ExpressionRule,
      Cypher25Parser.RULE_expression1 -> ExpressionRule,
      Cypher25Parser.RULE_expression2 -> ExpressionRule,
      Cypher25Parser.RULE_expression3 -> ExpressionRule,
      Cypher25Parser.RULE_expression4 -> ExpressionRule,
      Cypher25Parser.RULE_expression5 -> ExpressionRule,
      Cypher25Parser.RULE_expression6 -> ExpressionRule,
      Cypher25Parser.RULE_expression7 -> ExpressionRule,
      Cypher25Parser.RULE_expression8 -> ExpressionRule,
      Cypher25Parser.RULE_expression9 -> ExpressionRule,
      Cypher25Parser.RULE_expression10 -> ExpressionRule,
      Cypher25Parser.RULE_expression11 -> ExpressionRule,
      Cypher25Parser.RULE_stringLiteral -> StringLiteralRule,
      Cypher25Parser.RULE_interpolatedStringLiteral -> InterpolatedStringLiteralRule,
      Cypher25Parser.RULE_interpolatedStringLiteralSingle -> InterpolatedStringLiteralRule,
      Cypher25Parser.RULE_interpolatedStringLiteralDouble -> InterpolatedStringLiteralRule,
      Cypher25Parser.RULE_numberLiteral -> NumberLiteralRule,
      Cypher25Parser.RULE_parameter -> ParameterRule,
      Cypher25Parser.RULE_variable -> VariableRule,
      Cypher25Parser.RULE_symbolicVariableNameString -> VariableRule,
      Cypher25Parser.RULE_unescapedSymbolicVariableNameString -> VariableRule,
      Cypher25Parser.RULE_escapedSymbolicNameString -> VariableRule,
      Cypher25Parser.RULE_symbolicAliasName -> DatabaseNameRule,
      Cypher25Parser.RULE_pattern -> GraphPatternRule,
      Cypher25Parser.RULE_symbolicNameString -> IdentifierRule,
      Cypher25Parser.RULE_escapedSymbolicNameString -> IdentifierRule,
      Cypher25Parser.RULE_unescapedSymbolicNameString -> IdentifierRule,
      Cypher25Parser.RULE_labelExpression -> LabelExpressionRule,
      Cypher25Parser.RULE_relationshipPattern -> RelationshipPatternRule,
      Cypher25Parser.RULE_nodePattern -> NodePatternRule,
      Cypher25Parser.RULE_labelExpression1 -> LabelExpression1Rule
    )
  }

  override def errorCharTokenType: Int = Cypher25Parser.ErrorChar

  override def ruleNames: Array[String] = Cypher25Parser.ruleNames
}
