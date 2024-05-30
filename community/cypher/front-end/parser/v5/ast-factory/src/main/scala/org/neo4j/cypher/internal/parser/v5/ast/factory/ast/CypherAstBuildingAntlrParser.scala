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

import org.antlr.v4.runtime.Lexer
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.TokenStream
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.ast.AntlrAstParser
import org.neo4j.cypher.internal.parser.ast.AstBuildingAntlrParser
import org.neo4j.cypher.internal.parser.ast.SyntaxChecker
import org.neo4j.cypher.internal.parser.v5.CypherParser
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5SyntaxChecker
import org.neo4j.cypher.internal.parser.v5.ast.factory.CypherAstLexer
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger

final class CypherAstParser(
  query: String,
  override val exceptionFactory: CypherExceptionFactory,
  notificationLogger: Option[InternalNotificationLogger]
) extends AntlrAstParser[CypherAstBuildingAntlrParser] {

  override def statements(): Statements = parse(_.statements())
  override def expression(): Expression = parse(_.expression())

  override def syntaxException(message: String, position: InputPosition): RuntimeException = {
    exceptionFactory.syntaxException(message, position)
  }

  override protected def newParser(tokens: TokenStream): CypherAstBuildingAntlrParser =
    new CypherAstBuildingAntlrParser(tokens, exceptionFactory, notificationLogger)

  override protected def newLexer(fullTokens: Boolean): Lexer = CypherAstLexer.fromString(query, fullTokens)
  override protected def errorStrategyConf: CypherErrorStrategy.Conf = new Cypher5ErrorStrategyConf
}

/**
 * Antlr based parser that parse Cypher 5 syntax and builds Neo4j AST.
 */
final protected class CypherAstBuildingAntlrParser(
  input: TokenStream,
  exceptionFactory: CypherExceptionFactory,
  notificationLogger: Option[InternalNotificationLogger]
) extends CypherParser(input) with AstBuildingAntlrParser {

  removeErrorListeners() // Avoid printing errors to stdout

  override def createSyntaxChecker(): SyntaxChecker = new Cypher5SyntaxChecker(exceptionFactory)
  override def createAstBuilder(): ParseTreeListener = new AstBuilder(notificationLogger, exceptionFactory)

  override def isSafeToFreeChildren(ctx: ParserRuleContext): Boolean = ctx.getRuleIndex match {
    case CypherParser.RULE_allPrivilegeTarget               => false
    case CypherParser.RULE_allPrivilegeType                 => false
    case CypherParser.RULE_alterAliasDriver                 => false
    case CypherParser.RULE_alterAliasPassword               => false
    case CypherParser.RULE_alterAliasProperties             => false
    case CypherParser.RULE_alterAliasTarget                 => false
    case CypherParser.RULE_alterAliasUser                   => false
    case CypherParser.RULE_alterDatabaseAccess              => false
    case CypherParser.RULE_alterDatabaseTopology            => false
    case CypherParser.RULE_comparisonExpression6            => false
    case CypherParser.RULE_constraintType                   => false
    case CypherParser.RULE_createIndex                      => false
    case CypherParser.RULE_extendedCaseAlternative          => false
    case CypherParser.RULE_extendedWhen                     => false
    case CypherParser.RULE_functionName                     => false
    case CypherParser.RULE_functionArgument                 => false
    case CypherParser.RULE_procedureName                    => false
    case CypherParser.RULE_procedureArgument                => false
    case CypherParser.RULE_roleNames                        => false
    case CypherParser.RULE_userNames                        => false
    case CypherParser.RULE_globPart                         => false
    case CypherParser.RULE_lookupIndexRelPattern            => false
    case CypherParser.RULE_nonEmptyNameList                 => false
    case CypherParser.RULE_password                         => false
    case CypherParser.RULE_postFix                          => false
    case CypherParser.RULE_propertyList                     => false
    case CypherParser.RULE_symbolicAliasName                => false
    case CypherParser.RULE_symbolicAliasNameOrParameter     => false
    case CypherParser.RULE_symbolicNameString               => false
    case CypherParser.RULE_unescapedLabelSymbolicNameString => false
    case CypherParser.RULE_unescapedSymbolicNameString      => false
    case _                                                  => true
  }
}
