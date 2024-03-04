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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.factory.neo4j.ReplaceUnicodeEscapeSequences
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxChecker
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.CypherAstParser.DEBUG
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser

/**
 * Parses Neo4j AST using antlr. Fails fast. Optimised for memory by removing the parse as we go.
 */
// TODO Can be pooled and reused with `setInputStream`, investigate if its worth the risks
// TODO Parsing mode
class CypherAstParser private (input: TokenStream, createAst: Boolean) extends CypherParser(input) {
  private[this] var astBuilder: ParseTreeListener = _
  private[this] var syntaxChecker: SyntaxChecker = _
  private[this] var errorListener: SyntaxErrorListener = _

  removeErrorListeners() // Avoid printing errors to stdout
  addErrorListener(errorListener) // TODO Is this necessary when we have BailErrorStrategy?
  setErrorHandler(new BailErrorStrategy) // Is this the right choice?

  override def exitRule(): Unit = {
    val localCtx = _ctx
    super.exitRule()

    if (localCtx.exception == null) {
      // These could be added using `addParseListener` too, but this is faster
      syntaxChecker.exitEveryRule(localCtx)
      // TODO!
      if (syntaxChecker.getErrors.nonEmpty) {
        throw syntaxChecker.getErrors.next
      }

      astBuilder.exitEveryRule(localCtx)
      if (DEBUG) println(s"Exit ${localCtx.getClass.getSimpleName} AST=${localCtx.asInstanceOf[AstRuleCtx].ast}")

      // TODO Save memory by removing the parse tree as we go.
      // localCtx.children = null
      // Alternatively we could use setBuildParseTree(false) which would be even more efficient,
      // but requires changes to the listeners (work without accessing the children) and grammar (label everything).

      // Throw exception if EOF is not reached
      if (_ctx == null) {
        // TODO hides other failures sometimes
        throwIfEofNotReached(localCtx)
      }
    } else {
      if (DEBUG) localCtx.exception.printStackTrace()
    }
  }

  override def reset(): Unit = {
    super.reset()
    astBuilder = if (createAst) new AstBuilder else NoOpParseTreeListener
    syntaxChecker = new SyntaxChecker
    errorListener = new SyntaxErrorListener
  }

  override def addParseListener(listener: ParseTreeListener): Unit = throw new UnsupportedOperationException()

  // TODO Tests for this
  private def throwIfEofNotReached(ctx: ParserRuleContext): Unit = {
    if (!(matchedEOF || getTokenStream.LA(1) == Token.EOF)) {
      // TODO can be null

      val stop = Option(ctx).flatMap(c => Option(c.stop))
      val tokenText = stop.map(_.getText)
      val tokenLine = stop.map(_.getLine)
      val tokenColumn = stop.map(_.getCharPositionInLine)
      // TODO Exception type and message
      throw new RuntimeException(
        s"did not read all input, it stopped at token $tokenText at line $tokenLine, column $tokenColumn"
      )
    }
  }
}

object CypherAstParser {
  final val DEBUG = false

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
