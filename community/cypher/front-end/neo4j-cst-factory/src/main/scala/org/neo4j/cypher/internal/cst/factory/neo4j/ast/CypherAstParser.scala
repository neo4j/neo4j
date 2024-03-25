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
import org.neo4j.internal.helpers.Exceptions

/**
 * Parses Neo4j AST using antlr. Fails fast. Optimised for memory by removing the parse as we go.
 */
class CypherAstParser private (input: TokenStream, createAst: Boolean) extends CypherParser(input) {
  // These could be added using `addParseListener` too, but this is faster
  private[this] var astBuilder: ParseTreeListener = _
  private[this] var syntaxChecker: SyntaxChecker = _

  removeErrorListeners() // Avoid printing errors to stdout

  override def exitRule(): Unit = {
    val localCtx = _ctx
    super.exitRule()

    if (_syntaxErrors == 0) {
      syntaxChecker.exitEveryRule(localCtx)
      if (syntaxChecker.hasErrors) {
        throw syntaxChecker.getErrors.reduce(Exceptions.chain)
      }

      astBuilder.exitEveryRule(localCtx)
      if (DEBUG) println(s"Exit ${localCtx.getClass.getSimpleName} AST=${localCtx.asInstanceOf[AstRuleCtx].ast}")

      // Save memory by removing the parse tree as we go.
      // Some listeners access children of children so we only do this at certain safe points
      // where we know the children are not needed anymore for ast building or syntax checker.
      val ruleIndex = localCtx.getRuleIndex
      if (ruleIndex == CypherParser.RULE_expression || ruleIndex == CypherParser.RULE_clause) {
        localCtx.children = null
      }

      // Throw exception if EOF is not reached
      if (_ctx == null && !matchedEOF && getTokenStream.LA(1) != Token.EOF) {
        throw eofNotReached(localCtx)
      }
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
    astBuilder = if (createAst) new AstBuilder else NoOpParseTreeListener
    syntaxChecker = new SyntaxChecker
  }

  override def addParseListener(listener: ParseTreeListener): Unit = throw new UnsupportedOperationException()

  private def eofNotReached(ctx: ParserRuleContext): Exception = {
    val stop = Option(ctx).flatMap(c => Option(c.stop))
    val tokenText = stop.map(_.getText)
    val tokenLine = stop.map(_.getLine)
    val tokenColumn = stop.map(_.getCharPositionInLine)
    // TODO Exception type and message
    new RuntimeException(
      s"did not read all input, it stopped at token $tokenText at line $tokenLine, column $tokenColumn"
    )
  }
}

object CypherAstParser {
  final val DEBUG = false

  def parseStatements(query: String, exceptionFactory: CypherExceptionFactory): Statements =
    parse(query, exceptionFactory, _.statements()).ast[Statements]()

  def parse[T <: AstRuleCtx](query: String, exceptionFactory: CypherExceptionFactory, f: CypherAstParser => T): T = {
    val tokens = preparsedTokens(query)
    val parser = new CypherAstParser(tokens, true)
    try {
      // Try parsing with PredictionMode.SLL first (faster but might fail on some syntax)
      // See https://github.com/antlr/antlr4/blob/dev/doc/faq/general.md#why-is-my-expression-parser-slow
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)

      // Use bail error strategy to fail fast and avoid recovery attempts
      parser.setErrorHandler(new BailErrorStrategy)

      f(parser)
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
        val errorListener = new SyntaxErrorListener(exceptionFactory)
        parser.addErrorListener(errorListener)

        val result = f(parser)
        if (errorListener.syntaxErrors.nonEmpty) {
          throw errorListener.syntaxErrors.reduce(Exceptions.chain)
        }
        result
    }
  }

  def apply(query: String): CypherAstParser = new CypherAstParser(preparsedTokens(query), true)
  def apply(input: TokenStream): CypherAstParser = new CypherAstParser(input, true)

  // Only needed during development
  def withoutAst(query: String): CypherAstParser = new CypherAstParser(preparsedTokens(query), false)

  private def preparsedTokens(cypher: String) = new CommonTokenStream(ReplaceUnicodeEscapeSequences.fromString(cypher))
}

object NoOpParseTreeListener extends ParseTreeListener {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
