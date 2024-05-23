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
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.TokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.CypherAstLexer
import org.neo4j.cypher.internal.cst.factory.neo4j.Cypher5SyntaxChecker
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.ast.BaseAstParser
import org.neo4j.cypher.internal.parser.ast.SyntaxChecker
import org.neo4j.cypher.internal.parser.lexer.UnicodeEscapeReplacementReader
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.kernel.api.exceptions.Status.HasStatus

import scala.util.control.NonFatal

/**
 * Parses Neo4j AST using antlr. Fails fast. Optimised for memory by removing the parse as we go.
 */
final class CypherAstParser private (
  input: TokenStream,
  override val exceptionFactory: CypherExceptionFactory,
  override val notificationLogger: Option[InternalNotificationLogger]
) extends CypherParser(input) with BaseAstParser {

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

object CypherAstParser {
  final val DEBUG = false

  def parseStatements(
    query: String,
    exceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger]
  ): Statement = {
    val statements =
      try {
        parse(query, exceptionFactory, notificationLogger, _.statements()).ast[Statements]()
      } catch {
        case e: HasStatus => throw e
        // Other errors which come from the parser should not be exposed to the user
        case e: Exception => throw new CypherExecutionException(s"Failed to parse query `$query`.", e)
      }

    if (statements.size() == 1) {
      statements.statements.head
    } else {
      throw exceptionFactory.syntaxException(
        s"Expected exactly one statement per query but got: ${statements.size()}",
        InputPosition.NONE
      )
    }
  }

  def parse[T <: AstRuleCtx](
    query: String,
    exceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger],
    f: CypherAstParser => T
  ): T = {
    val listener = new SyntaxErrorListener(exceptionFactory)
    val tokens = preparsedTokens(query, listener, exceptionFactory, fullTokens = false)
    val parser = new CypherAstParser(tokens, exceptionFactory, notificationLogger)

    // Try parsing with PredictionMode.SLL first (faster but might fail on some syntax)
    // See https://github.com/antlr/antlr4/blob/dev/doc/faq/general.md#why-is-my-expression-parser-slow
    parser.getInterpreter.setPredictionMode(PredictionMode.SLL)

    // Use bail error strategy to fail fast and avoid recovery attempts
    parser.setErrorHandler(new BailErrorStrategy)

    try {
      doParse(parser, listener, f)
    } catch {
      case NonFatal(_) =>
        // The fast route failed, now try again with full error handling and prediction mode

        // Reset parser and token stream
        // We do not reuse the TokenStream because we need `fullTokens = true` for better error handling
        parser.setInputStream(preparsedTokens(query, listener, exceptionFactory, fullTokens = true))

        // Slower but correct prediction.
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)

        // CypherErrorStrategy allows us to get the correct error messages in case we still fail
        parser.setErrorHandler(new CypherErrorStrategy(new Cypher5ErrorStrategyConf))
        parser.addErrorListener(listener)

        doParse(parser, listener, f)
    }
  }

  private def doParse[T <: AstRuleCtx](
    parser: CypherAstParser,
    listener: SyntaxErrorListener,
    f: CypherAstParser => T
  ): T = {
    val result = f(parser)

    // Throw syntax checker errors
    if (parser.syntaxChecker().errors.nonEmpty) {
      throw parser.syntaxChecker().errors.reduce(Exceptions.chain)
    }

    // Throw any syntax errors
    if (listener.syntaxErrors.nonEmpty) {
      throw listener.syntaxErrors.reduce(Exceptions.chain)
    }

    result
  }

  private def preparsedTokens(
    cypher: String,
    listener: SyntaxErrorListener,
    exceptionFactory: CypherExceptionFactory,
    fullTokens: Boolean
  ) =
    try {
      val lexer = CypherAstLexer.fromString(cypher, fullTokens)
      lexer.removeErrorListeners()
      lexer.addErrorListener(listener)
      new CommonTokenStream(lexer)
    } catch {
      case e: UnicodeEscapeReplacementReader.InvalidUnicodeLiteral =>
        throw exceptionFactory.syntaxException(e.getMessage, InputPosition(e.offset, e.line, e.col))
    }
}
