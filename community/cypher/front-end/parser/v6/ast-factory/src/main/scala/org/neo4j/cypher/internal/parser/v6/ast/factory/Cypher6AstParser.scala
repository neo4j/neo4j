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
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger

final class Cypher6AstParser(
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

  override protected def newLexer(fullTokens: Boolean): Lexer = Cypher6AstLexer.fromString(query, fullTokens)
  override protected def errorStrategyConf: CypherErrorStrategy.Conf = new Cypher6ErrorStrategyConf
}

/**
 * Antlr based parser that parse Cypher 6 syntax and builds Neo4j AST.
 */
final protected class CypherAstBuildingAntlrParser(
  input: TokenStream,
  exceptionFactory: CypherExceptionFactory,
  notificationLogger: Option[InternalNotificationLogger]
) extends Cypher6Parser(input) with AstBuildingAntlrParser {

  removeErrorListeners() // Avoid printing errors to stdout

  override def createSyntaxChecker(): SyntaxChecker = new Cypher6SyntaxChecker(exceptionFactory)
  override def createAstBuilder(): ParseTreeListener = new Cypher6AstBuilder(notificationLogger, exceptionFactory)

  override def isSafeToFreeChildren(ctx: ParserRuleContext): Boolean = ctx.getRuleIndex match {
    case Cypher6Parser.RULE_allPrivilegeTarget               => false
    case Cypher6Parser.RULE_allPrivilegeType                 => false
    case Cypher6Parser.RULE_alterAliasDriver                 => false
    case Cypher6Parser.RULE_alterAliasPassword               => false
    case Cypher6Parser.RULE_alterAliasProperties             => false
    case Cypher6Parser.RULE_alterAliasTarget                 => false
    case Cypher6Parser.RULE_alterAliasUser                   => false
    case Cypher6Parser.RULE_alterDatabaseAccess              => false
    case Cypher6Parser.RULE_alterDatabaseTopology            => false
    case Cypher6Parser.RULE_comparisonExpression6            => false
    case Cypher6Parser.RULE_constraintType                   => false
    case Cypher6Parser.RULE_createIndex                      => false
    case Cypher6Parser.RULE_extendedCaseAlternative          => false
    case Cypher6Parser.RULE_extendedWhen                     => false
    case Cypher6Parser.RULE_functionName                     => false
    case Cypher6Parser.RULE_functionArgument                 => false
    case Cypher6Parser.RULE_procedureName                    => false
    case Cypher6Parser.RULE_procedureArgument                => false
    case Cypher6Parser.RULE_roleNames                        => false
    case Cypher6Parser.RULE_userNames                        => false
    case Cypher6Parser.RULE_globPart                         => false
    case Cypher6Parser.RULE_lookupIndexRelPattern            => false
    case Cypher6Parser.RULE_nonEmptyNameList                 => false
    case Cypher6Parser.RULE_password                         => false
    case Cypher6Parser.RULE_postFix                          => false
    case Cypher6Parser.RULE_propertyList                     => false
    case Cypher6Parser.RULE_symbolicAliasName                => false
    case Cypher6Parser.RULE_symbolicAliasNameOrParameter     => false
    case Cypher6Parser.RULE_aliasName                        => false
    case Cypher6Parser.RULE_databaseName                     => false
    case Cypher6Parser.RULE_symbolicNameString               => false
    case Cypher6Parser.RULE_unescapedLabelSymbolicNameString => false
    case Cypher6Parser.RULE_unescapedSymbolicNameString      => false
    case _                                                   => true
  }
}
