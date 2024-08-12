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
package org.neo4j.cypher.internal.parser.ast

import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.Lexer
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.TokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.SyntaxErrorListener
import org.neo4j.cypher.internal.parser.lexer.CypherToken
import org.neo4j.cypher.internal.parser.lexer.UnicodeEscapeReplacementReader
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.internal.helpers.Exceptions

import scala.util.control.NonFatal

/** Helper trait for all antlr based [[AstParser]]s. */
trait AntlrAstParser[P <: AstBuildingAntlrParser] extends AstParser {
  protected def newParser(tokens: TokenStream): P
  protected def newLexer(fullTokens: Boolean): Lexer
  protected def exceptionFactory: CypherExceptionFactory
  protected def errorStrategyConf: CypherErrorStrategy.Conf

  final def parse[AST <: AnyRef](f: P => AstRuleCtx): AST = {
    val listener = new SyntaxErrorListener(exceptionFactory)
    val parser = newParser(preparsedTokens(listener, fullTokens = false))

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
        parser.setInputStream(preparsedTokens(listener, fullTokens = true))

        // Slower but correct prediction.
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)

        // CypherErrorStrategy allows us to get the correct error messages in case we still fail
        parser.setErrorHandler(new CypherErrorStrategy(errorStrategyConf))
        parser.addErrorListener(listener)

        doParse(parser, listener, f)
    }
  }

  final private def doParse[CTX <: AstRuleCtx, AST <: AnyRef](
    parser: P,
    listener: SyntaxErrorListener,
    f: P => CTX
  ): AST = {
    val result = f(parser)

    // Throw syntax checker errors
    if (parser.syntaxChecker().errors.nonEmpty) {
      throw parser.syntaxChecker().errors.reduce(Exceptions.chain)
    }

    // Throw any syntax errors
    if (listener.syntaxErrors.nonEmpty) {
      throw listener.syntaxErrors.reduce(Exceptions.chain)
    }

    if (!parseReachedEof(parser)) {
      throw exceptionFactory.syntaxException(
        s"Invalid input '${parser.getCurrentToken.getText}'",
        position(parser.getCurrentToken)
      )
    }

    result.ast[AST]()
  }

  final private def parseReachedEof(parser: P): Boolean =
    parser.isMatchedEOF || parser.getCurrentToken.getType == Token.EOF

  final private def preparsedTokens(listener: SyntaxErrorListener, fullTokens: Boolean): TokenStream =
    try {
      val lexer = newLexer(fullTokens)
      lexer.removeErrorListeners()
      lexer.addErrorListener(listener)
      new CommonTokenStream(lexer)
    } catch {
      case e: UnicodeEscapeReplacementReader.InvalidUnicodeLiteral =>
        throw exceptionFactory.syntaxException(e.getMessage, InputPosition(e.offset, e.line, e.column))
    }

  private def position(token: Token): InputPosition = token match {
    case cypherToken: CypherToken => cypherToken.position()
    case _                        => InputPosition(token.getStartIndex, token.getLine, token.getCharPositionInLine + 1)
  }
}
