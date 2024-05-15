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

import org.antlr.v4.runtime.ANTLRErrorStrategy
import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.TokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.CypherAstLexer
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxChecker
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.CypherAstParser.DEBUG
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.CypherParser
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
class CypherAstParser private (
  input: TokenStream,
  createAst: Boolean,
  exceptionFactory: CypherExceptionFactory,
  notificationLogger: Option[InternalNotificationLogger]
) extends CypherParser(input) {
  // These could be added using `addParseListener` too, but this is faster
  private[this] var astBuilder: ParseTreeListener = _
  private[this] var checker: SyntaxChecker = _
  private[this] var hasFailed: Boolean = false
  private[this] var bailErrors: Boolean = false

  removeErrorListeners() // Avoid printing errors to stdout

  override def exitRule(): Unit = {
    val localCtx = _ctx
    super.exitRule()

    if (bailErrors) {
      // In this mode we care more about speed than correct error handling
      checker.exitEveryRule(localCtx)
      astBuilder.exitEveryRule(localCtx)
    } else if (!hasFailed) {
      // Here we care about correct error handling.
      // Stop on failures to not hide the cause of an error with sequent exceptions

      if (checker.check(localCtx)) buildAstWithErrorHandling(localCtx)
      else hasFailed = true
    }

    // Save memory by removing the parse tree as we go.
    // Some listeners access children of children so we only do this at certain safe points
    // where we know the children are not needed anymore for ast building or syntax checker.
    localCtx.getRuleIndex match {
      case CypherParser.RULE_allPrivilegeTarget               =>
      case CypherParser.RULE_allPrivilegeType                 =>
      case CypherParser.RULE_alterAliasDriver                 =>
      case CypherParser.RULE_alterAliasPassword               =>
      case CypherParser.RULE_alterAliasProperties             =>
      case CypherParser.RULE_alterAliasTarget                 =>
      case CypherParser.RULE_alterAliasUser                   =>
      case CypherParser.RULE_alterDatabaseAccess              =>
      case CypherParser.RULE_alterDatabaseTopology            =>
      case CypherParser.RULE_comparisonExpression6            =>
      case CypherParser.RULE_constraintType                   =>
      case CypherParser.RULE_createIndex                      =>
      case CypherParser.RULE_extendedCaseAlternative          =>
      case CypherParser.RULE_extendedWhen                     =>
      case CypherParser.RULE_functionName                     =>
      case CypherParser.RULE_functionArgument                 =>
      case CypherParser.RULE_procedureName                    =>
      case CypherParser.RULE_procedureArgument                =>
      case CypherParser.RULE_roleNames                        =>
      case CypherParser.RULE_userNames                        =>
      case CypherParser.RULE_globPart                         =>
      case CypherParser.RULE_lookupIndexRelPattern            =>
      case CypherParser.RULE_nonEmptyNameList                 =>
      case CypherParser.RULE_password                         =>
      case CypherParser.RULE_postFix                          =>
      case CypherParser.RULE_propertyList                     =>
      case CypherParser.RULE_symbolicAliasName                =>
      case CypherParser.RULE_symbolicAliasNameOrParameter     =>
      case CypherParser.RULE_symbolicNameString               =>
      case CypherParser.RULE_unescapedLabelSymbolicNameString =>
      case CypherParser.RULE_unescapedSymbolicNameString      =>
      case _                                                  => localCtx.children = null
    }

  }

  private def buildAstWithErrorHandling(ctx: ParserRuleContext): Unit = {
    try {
      astBuilder.exitEveryRule(ctx)
      if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} AST=${ctx.asInstanceOf[AstRuleCtx].ast}")
    } catch {
      case NonFatal(e) =>
        if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} FAILED! $e")
        hasFailed = true
        throw e
    }
  }

  override def createTerminalNode(parent: ParserRuleContext, t: Token): TerminalNode = {
    t match {
      case ct: TerminalNode => ct
      case _                => super.createTerminalNode(parent, t)
    }
  }

  override def reset(): Unit = {
    super.reset()
    astBuilder = if (createAst) new AstBuilder(notificationLogger, exceptionFactory) else NoOpParseTreeListener
    checker = new SyntaxChecker(exceptionFactory)
    hasFailed = false
  }

  override def addParseListener(listener: ParseTreeListener): Unit = throw new UnsupportedOperationException()

  override def setErrorHandler(handler: ANTLRErrorStrategy): Unit = {
    super.setErrorHandler(handler)
    bailErrors = handler.isInstanceOf[BailErrorStrategy]
  }

  override def notifyErrorListeners(offendingToken: Token, msg: String, e: RecognitionException): Unit = {
    hasFailed = true
    super.notifyErrorListeners(offendingToken, msg, e)
  }

  def syntaxChecker(): SyntaxChecker = checker

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
    val parser = new CypherAstParser(tokens, true, exceptionFactory, notificationLogger)

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
    if (parser.syntaxChecker().hasErrors) {
      throw parser.syntaxChecker().getErrors.reduce(Exceptions.chain)
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

object NoOpParseTreeListener extends ParseTreeListener {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
