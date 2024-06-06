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
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser

import java.util

class Cypher5ErrorStrategyConf extends CypherErrorStrategy.Conf {

  override def vocabulary: VocabularyImpl = Cypher5Parser.VOCABULARY.asInstanceOf[VocabularyImpl]

  override def ignoredTokens: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    Token.EPSILON,
    Cypher5Parser.SEMICOLON
  )

  override def customTokenDisplayNames: Map[Int, String] = Map(
    Cypher5Parser.SPACE -> "' '",
    Cypher5Parser.SINGLE_LINE_COMMENT -> "'//'",
    Cypher5Parser.DECIMAL_DOUBLE -> "a float value",
    Cypher5Parser.UNSIGNED_DECIMAL_INTEGER -> "an integer value",
    Cypher5Parser.UNSIGNED_HEX_INTEGER -> "a hexadecimal integer value",
    Cypher5Parser.UNSIGNED_OCTAL_INTEGER -> "an octal integer value",
    Cypher5Parser.IDENTIFIER -> "an identifier",
    Cypher5Parser.ARROW_LINE -> "'-'",
    Cypher5Parser.ARROW_LEFT_HEAD -> "'<'",
    Cypher5Parser.ARROW_RIGHT_HEAD -> "'>'",
    Cypher5Parser.MULTI_LINE_COMMENT -> "'/*'",
    Cypher5Parser.STRING_LITERAL1 -> "a string value",
    Cypher5Parser.STRING_LITERAL2 -> "a string value",
    Cypher5Parser.ESCAPED_SYMBOLIC_NAME -> "an identifier",
    Cypher5Parser.ALL_SHORTEST_PATHS -> "'allShortestPaths'",
    Cypher5Parser.SHORTEST_PATH -> "'shortestPath'",
    Cypher5Parser.LIMITROWS -> "'LIMIT'",
    Cypher5Parser.SKIPROWS -> "'SKIP'",
    Token.EOF -> "<EOF>"
  )

  override def ruleGroups: Map[Int, CypherErrorStrategy.CypherRuleGroup] = {
    Map(
      Cypher5Parser.RULE_expression -> ExpressionRule,
      Cypher5Parser.RULE_expression1 -> ExpressionRule,
      Cypher5Parser.RULE_expression2 -> ExpressionRule,
      Cypher5Parser.RULE_expression3 -> ExpressionRule,
      Cypher5Parser.RULE_expression4 -> ExpressionRule,
      Cypher5Parser.RULE_expression5 -> ExpressionRule,
      Cypher5Parser.RULE_expression6 -> ExpressionRule,
      Cypher5Parser.RULE_expression7 -> ExpressionRule,
      Cypher5Parser.RULE_expression8 -> ExpressionRule,
      Cypher5Parser.RULE_expression9 -> ExpressionRule,
      Cypher5Parser.RULE_expression10 -> ExpressionRule,
      Cypher5Parser.RULE_expression11 -> ExpressionRule,
      Cypher5Parser.RULE_stringLiteral -> StringLiteralRule,
      Cypher5Parser.RULE_numberLiteral -> NumberLiteralRule,
      Cypher5Parser.RULE_parameter -> ParameterRule,
      Cypher5Parser.RULE_variable -> VariableRule,
      Cypher5Parser.RULE_symbolicAliasName -> DatabaseNameRule,
      Cypher5Parser.RULE_pattern -> GraphPatternRule,
      Cypher5Parser.RULE_symbolicNameString -> IdentifierRule,
      Cypher5Parser.RULE_escapedSymbolicNameString -> IdentifierRule,
      Cypher5Parser.RULE_unescapedSymbolicNameString -> IdentifierRule,
      Cypher5Parser.RULE_symbolicLabelNameString -> IdentifierRule,
      Cypher5Parser.RULE_unescapedLabelSymbolicNameString -> IdentifierRule,
      Cypher5Parser.RULE_labelExpression -> LabelExpressionRule,
      Cypher5Parser.RULE_relationshipPattern -> RelationshipPatternRule,
      Cypher5Parser.RULE_nodePattern -> NodePatternRule,
      Cypher5Parser.RULE_labelExpression1 -> LabelExpression1Rule
    )
  }

  override def errorCharTokenType: Int = Cypher5Parser.ErrorChar

  override def ruleNames: Array[String] = Cypher5Parser.ruleNames
}
