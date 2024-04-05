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
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.ReplaceUnicodeEscapeSequences
import org.neo4j.cypher.internal.cst.factory.neo4j.DefaultCypherToken
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxChecker
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.CypherAstParser.DEBUG
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.internal.helpers.Exceptions

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
      checker.check(localCtx)
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
    val ruleIndex = localCtx.getRuleIndex
    if (ruleIndex == CypherParser.RULE_expression || ruleIndex == CypherParser.RULE_clause) {
      localCtx.children = null
    }
  }

  private def buildAstWithErrorHandling(ctx: ParserRuleContext): Unit = {
    try {
      astBuilder.exitEveryRule(ctx)
      if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} AST=${ctx.asInstanceOf[AstRuleCtx].ast}")
    } catch {
      case e: Exception =>
        if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} FAILED! $e")
        hasFailed = true
        throw e
    }
  }

  override def createTerminalNode(parent: ParserRuleContext, t: Token): TerminalNode = {
    t match {
      case ct: DefaultCypherToken => ct
      case _                      => super.createTerminalNode(parent, t)
    }
  }

  override def reset(): Unit = {
    super.reset()
    astBuilder = if (createAst) new AstBuilder(notificationLogger) else NoOpParseTreeListener
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
  ): Statements =
    parse(query, exceptionFactory, notificationLogger, _.statements()).ast[Statements]()

  def parse[T <: AstRuleCtx](
    query: String,
    exceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger],
    f: CypherAstParser => T
  ): T = {
    val tokens = preparsedTokens(query)
    val parser = new CypherAstParser(tokens, true, exceptionFactory, notificationLogger)

    // Try parsing with PredictionMode.SLL first (faster but might fail on some syntax)
    // See https://github.com/antlr/antlr4/blob/dev/doc/faq/general.md#why-is-my-expression-parser-slow
    parser.getInterpreter.setPredictionMode(PredictionMode.SLL)

    // Use bail error strategy to fail fast and avoid recovery attempts
    parser.setErrorHandler(new BailErrorStrategy)

    try {
      doParse(parser, f)
    } catch {
      case _: Exception =>
        // The fast route failed, now try again with full error handling and prediction mode

        // Reset parser and token stream
        tokens.seek(0)
        parser.reset()

        // Slower but correct prediction.
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)

        // CypherErrorStrategy allows us to get the correct error messages in case we still fail
        parser.setErrorHandler(new CypherErrorStrategy)
        parser.addErrorListener(new SyntaxErrorListener(exceptionFactory))

        doParse(parser, f)
    }
  }

  private def doParse[T <: AstRuleCtx](
    parser: CypherAstParser,
    f: CypherAstParser => T
  ): T = {
    val result = f(parser)

    // Throw syntax checker errors
    if (parser.syntaxChecker().hasErrors) {
      throw parser.syntaxChecker().getErrors.reduce(Exceptions.chain)
    }

    // Throw any syntax errors
    if (!parser.getErrorListeners.isEmpty) {
      val errorListener = parser.getErrorListeners.get(0).asInstanceOf[SyntaxErrorListener]
      if (errorListener.syntaxErrors.nonEmpty) {
        throw errorListener.syntaxErrors.reduce(Exceptions.chain)
      }
    }

    result
  }

  private def preparsedTokens(cypher: String) = new CommonTokenStream(ReplaceUnicodeEscapeSequences.fromString(cypher))
}

object NoOpParseTreeListener extends ParseTreeListener {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
