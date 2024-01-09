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
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.TokenStream
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.cst.factory.neo4j.CypherInputStream
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxChecker
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener
import org.neo4j.cypher.internal.parser.CypherLexer
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

  // Saves significant amounts of memory during parsing.
  // One alternative is to build parse tree, but manually clear children as we go.
  setBuildParseTree(false)

  removeErrorListeners() // Avoid printing errors to stdout
  addErrorListener(errorListener) // TODO Is this necessary when we have BailErrorStrategy?
  setErrorHandler(new BailErrorStrategy) // Is this the right choice?

  override def exitRule(): Unit = {
    val localCtx = _ctx
    super.exitRule()

    if (localCtx.exception == null) {
      // These could be added using `addParseListener` too, but this is faster
      syntaxChecker.exitEveryRule(localCtx)
      astBuilder.exitEveryRule(localCtx)

      // Throw exception if EOF is not reached
      if (_ctx == null) {
        throwIfEofNotReached(localCtx)
      }
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
    if (!matchedEOF) {
      // TODO can be null
      val tokenText = ctx.stop.getText
      val tokenLine = ctx.stop.getLine
      val tokenColumn = ctx.stop.getCharPositionInLine
      // TODO Exception type
      throw new RuntimeException(
        s"did not read all input, it stopped at token $tokenText at line $tokenLine, column $tokenColumn"
      )
    }
  }
}

object CypherAstParser {

  def apply(query: String): CypherAstParser = new CypherAstParser(toStream(query), true)
  def apply(input: TokenStream): CypherAstParser = new CypherAstParser(input, true)

  private def toStream(cypher: String) =
    new CommonTokenStream(new CypherLexer(CharStreams.fromStream(new CypherInputStream(cypher))))
}

object NoOpParseTreeListener extends ParseTreeListener {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
