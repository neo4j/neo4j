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
package org.neo4j.cypher.internal.parser.v5.ast.factory.ast

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.VocabularyImpl
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.DatabaseNameRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.GraphPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.IdentifierRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpression1Rule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NodePatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NumberLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ParameterRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.RelationshipPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.StringLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.VariableRule
import org.neo4j.cypher.internal.parser.v5.CypherParser

import java.util

class Cypher5ErrorStrategyConf extends CypherErrorStrategy.Conf {

  override def vocabulary: VocabularyImpl = CypherParser.VOCABULARY.asInstanceOf[VocabularyImpl]

  override def ignoredTokens: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    Token.EPSILON,
    CypherParser.SEMICOLON
  )

  override def customTokenDisplayNames: Map[Int, String] = Map(
    CypherParser.SPACE -> "' '",
    CypherParser.SINGLE_LINE_COMMENT -> "'//'",
    CypherParser.DECIMAL_DOUBLE -> "a float value",
    CypherParser.UNSIGNED_DECIMAL_INTEGER -> "an integer value",
    CypherParser.UNSIGNED_HEX_INTEGER -> "a hexadecimal integer value",
    CypherParser.UNSIGNED_OCTAL_INTEGER -> "an octal integer value",
    CypherParser.IDENTIFIER -> "an identifier",
    CypherParser.ARROW_LINE -> "'-'",
    CypherParser.ARROW_LEFT_HEAD -> "'<'",
    CypherParser.ARROW_RIGHT_HEAD -> "'>'",
    CypherParser.MULTI_LINE_COMMENT -> "'/*'",
    CypherParser.STRING_LITERAL1 -> "a string value",
    CypherParser.STRING_LITERAL2 -> "a string value",
    CypherParser.ESCAPED_SYMBOLIC_NAME -> "an identifier",
    CypherParser.ALL_SHORTEST_PATHS -> "'allShortestPaths'",
    CypherParser.SHORTEST_PATH -> "'shortestPath'",
    CypherParser.LIMITROWS -> "'LIMIT'",
    CypherParser.SKIPROWS -> "'SKIP'",
    Token.EOF -> "<EOF>"
  )

  override def ruleGroups: Map[Int, CypherErrorStrategy.CypherRuleGroup] = {
    Map(
      CypherParser.RULE_expression -> ExpressionRule,
      CypherParser.RULE_expression1 -> ExpressionRule,
      CypherParser.RULE_expression2 -> ExpressionRule,
      CypherParser.RULE_expression3 -> ExpressionRule,
      CypherParser.RULE_expression4 -> ExpressionRule,
      CypherParser.RULE_expression5 -> ExpressionRule,
      CypherParser.RULE_expression6 -> ExpressionRule,
      CypherParser.RULE_expression7 -> ExpressionRule,
      CypherParser.RULE_expression8 -> ExpressionRule,
      CypherParser.RULE_expression9 -> ExpressionRule,
      CypherParser.RULE_expression10 -> ExpressionRule,
      CypherParser.RULE_expression11 -> ExpressionRule,
      CypherParser.RULE_stringLiteral -> StringLiteralRule,
      CypherParser.RULE_numberLiteral -> NumberLiteralRule,
      CypherParser.RULE_parameter -> ParameterRule,
      CypherParser.RULE_variable -> VariableRule,
      CypherParser.RULE_symbolicAliasName -> DatabaseNameRule,
      CypherParser.RULE_pattern -> GraphPatternRule,
      CypherParser.RULE_symbolicNameString -> IdentifierRule,
      CypherParser.RULE_escapedSymbolicNameString -> IdentifierRule,
      CypherParser.RULE_unescapedSymbolicNameString -> IdentifierRule,
      CypherParser.RULE_symbolicLabelNameString -> IdentifierRule,
      CypherParser.RULE_unescapedLabelSymbolicNameString -> IdentifierRule,
      CypherParser.RULE_labelExpression -> LabelExpressionRule,
      CypherParser.RULE_relationshipPattern -> RelationshipPatternRule,
      CypherParser.RULE_nodePattern -> NodePatternRule,
      CypherParser.RULE_labelExpression1 -> LabelExpression1Rule
    )
  }

  override def errorCharTokenType: Int = CypherParser.ErrorChar
}
