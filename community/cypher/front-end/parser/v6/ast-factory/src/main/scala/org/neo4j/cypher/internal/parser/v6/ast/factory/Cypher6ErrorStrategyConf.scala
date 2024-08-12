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
package org.neo4j.cypher.internal.parser.v6.ast.factory

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
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser

import java.util

class Cypher6ErrorStrategyConf extends CypherErrorStrategy.Conf {

  override def vocabulary: VocabularyImpl = Cypher6Parser.VOCABULARY.asInstanceOf[VocabularyImpl]

  override def ignoredTokens: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    Token.EPSILON,
    Cypher6Parser.SEMICOLON
  )

  override def customTokenDisplayNames: Map[Int, String] = Map(
    Cypher6Parser.SPACE -> "' '",
    Cypher6Parser.SINGLE_LINE_COMMENT -> "'//'",
    Cypher6Parser.DECIMAL_DOUBLE -> "a float value",
    Cypher6Parser.UNSIGNED_DECIMAL_INTEGER -> "an integer value",
    Cypher6Parser.UNSIGNED_HEX_INTEGER -> "a hexadecimal integer value",
    Cypher6Parser.UNSIGNED_OCTAL_INTEGER -> "an octal integer value",
    Cypher6Parser.EXTENDED_IDENTIFIER -> "an identifier",
    Cypher6Parser.IDENTIFIER -> "an identifier",
    Cypher6Parser.ARROW_LINE -> "'-'",
    Cypher6Parser.ARROW_LEFT_HEAD -> "'<'",
    Cypher6Parser.ARROW_RIGHT_HEAD -> "'>'",
    Cypher6Parser.MULTI_LINE_COMMENT -> "'/*'",
    Cypher6Parser.STRING_LITERAL1 -> "a string value",
    Cypher6Parser.STRING_LITERAL2 -> "a string value",
    Cypher6Parser.ESCAPED_SYMBOLIC_NAME -> "an identifier",
    Cypher6Parser.ALL_SHORTEST_PATHS -> "'allShortestPaths'",
    Cypher6Parser.SHORTEST_PATH -> "'shortestPath'",
    Cypher6Parser.LIMITROWS -> "'LIMIT'",
    Cypher6Parser.SKIPROWS -> "'SKIP'",
    Token.EOF -> "<EOF>"
  )

  override def ruleGroups: Map[Int, CypherErrorStrategy.CypherRuleGroup] = {
    Map(
      Cypher6Parser.RULE_expression -> ExpressionRule,
      Cypher6Parser.RULE_expression1 -> ExpressionRule,
      Cypher6Parser.RULE_expression2 -> ExpressionRule,
      Cypher6Parser.RULE_expression3 -> ExpressionRule,
      Cypher6Parser.RULE_expression4 -> ExpressionRule,
      Cypher6Parser.RULE_expression5 -> ExpressionRule,
      Cypher6Parser.RULE_expression6 -> ExpressionRule,
      Cypher6Parser.RULE_expression7 -> ExpressionRule,
      Cypher6Parser.RULE_expression8 -> ExpressionRule,
      Cypher6Parser.RULE_expression9 -> ExpressionRule,
      Cypher6Parser.RULE_expression10 -> ExpressionRule,
      Cypher6Parser.RULE_expression11 -> ExpressionRule,
      Cypher6Parser.RULE_stringLiteral -> StringLiteralRule,
      Cypher6Parser.RULE_numberLiteral -> NumberLiteralRule,
      Cypher6Parser.RULE_parameter -> ParameterRule,
      Cypher6Parser.RULE_variable -> VariableRule,
      Cypher6Parser.RULE_symbolicAliasName -> DatabaseNameRule,
      Cypher6Parser.RULE_pattern -> GraphPatternRule,
      Cypher6Parser.RULE_symbolicNameString -> IdentifierRule,
      Cypher6Parser.RULE_escapedSymbolicNameString -> IdentifierRule,
      Cypher6Parser.RULE_unescapedSymbolicNameString -> IdentifierRule,
      Cypher6Parser.RULE_symbolicLabelNameString -> IdentifierRule,
      Cypher6Parser.RULE_unescapedLabelSymbolicNameString -> IdentifierRule,
      Cypher6Parser.RULE_labelExpression -> LabelExpressionRule,
      Cypher6Parser.RULE_relationshipPattern -> RelationshipPatternRule,
      Cypher6Parser.RULE_nodePattern -> NodePatternRule,
      Cypher6Parser.RULE_labelExpression1 -> LabelExpression1Rule
    )
  }

  override def errorCharTokenType: Int = Cypher6Parser.ErrorChar

  override def ruleNames: Array[String] = Cypher6Parser.ruleNames
}
