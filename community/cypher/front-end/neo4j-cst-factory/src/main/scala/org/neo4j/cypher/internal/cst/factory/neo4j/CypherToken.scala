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
package org.neo4j.cypher.internal.cst.factory.neo4j

import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonToken
import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.RuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.TokenFactory
import org.antlr.v4.runtime.TokenSource
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.misc.Pair
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.ParseTreeVisitor
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.factory.neo4j.CypherAstLexer
import org.neo4j.cypher.internal.util.InputPosition

import java.util

/**
 * Implementation of [[Token]] that provides [[position()]] to retrieve correct [[InputPosition]]s.
 */
trait CypherToken extends Token {

  /**
   * See [[CypherAstLexer#inputPosition]] for caveats!
   */
  def position(): InputPosition = cypherLexer.inputPosition(getStartIndex, getLine, getCharPositionInLine)

  /**
   * See [[CypherAstLexer#inputOffset]] for caveats!
   */
  def inputOffset(parserOffset: Int): Int = cypherLexer.inputOffset(parserOffset)

  @inline private def cypherLexer: CypherAstLexer = getTokenSource.asInstanceOf[CypherAstLexer]
}

object CypherToken {

  def factory(fullTokens: Boolean): TokenFactory[CypherToken] = {
    if (fullTokens) FullCypherTokenFactory else CypherTokenFactory
  }
}

/**
 * A slimmer implementation of [[Token]].
 * Implements both [[CypherToken]] and [[TerminalNode]] as a memory optimisation.
 * Note, do not support some methods!
 */
final private class ThinCypherToken(
  source: Pair[TokenSource, CharStream],
  override val getType: Int,
  override val getChannel: Int,
  override val getStartIndex: Int,
  override val getStopIndex: Int,
  override val getLine: Int,
  override val getCharPositionInLine: Int
) extends CypherToken with TerminalNode {

  override def getText: String = {
    val input = getInputStream
    if (input == null) null
    else if (getStopIndex < input.size) input.getText(Interval.of(getStartIndex, getStopIndex))
    else "<EOF>"
  }

  override def getTokenSource: TokenSource = source.a
  override def getInputStream: CharStream = source.b
  override def getTokenIndex: Int = -1 // Not supported

  override def getSymbol: Token = this
  override def getParent: ParseTree = null // Not supported
  override def getChild(i: Int): ParseTree = null // Not supported
  override def setParent(parent: RuleContext): Unit = {} // Not supported
  override def accept[T](visitor: ParseTreeVisitor[_ <: T]): T = visitor.visitTerminal(this)
  override def toStringTree(parser: Parser): String = toString
  override def getSourceInterval: Interval = new Interval(getTokenIndex, getTokenIndex)
  override def getPayload: AnyRef = this
  override def getChildCount: Int = 0 // Not supported
  override def toStringTree: String = getText
}

/** Implementation of [[CypherToken]] that supports all methods,
 *  including [[org.antlr.v4.runtime.WritableToken]].
 *  Needed to get better syntax error messages in some cases. */
private class FullCypherToken(
  src: Pair[TokenSource, CharStream],
  typ: Int,
  ch: Int,
  start: Int,
  stop: Int
) extends CommonToken(src, typ, ch, start, stop) with CypherToken

case class OffsetTable(offsets: Array[Int], start: Int) {
  override def toString: String = s"OffsetTable(${util.Arrays.toString(offsets)}, $start)"
}

object CypherTokenFactory extends TokenFactory[CypherToken] {
  private type Src = Pair[TokenSource, CharStream]

  override def create(src: Src, typ: Int, txt: String, ch: Int, start: Int, stop: Int, line: Int, charPos: Int)
    : CypherToken = new ThinCypherToken(src, typ, ch, start, stop, line, charPos)

  override def create(typ: Int, text: String): CypherToken =
    new ThinCypherToken(null, typ, -1, -1, -1, -1, -1)
}

// Similar to CommonTokenFactory.create
object FullCypherTokenFactory extends TokenFactory[CypherToken] {
  private type Src = Pair[TokenSource, CharStream]

  override def create(src: Src, typ: Int, txt: String, ch: Int, start: Int, stop: Int, line: Int, charPos: Int)
    : CypherToken = {
    val token = new FullCypherToken(src, typ, ch, start, stop)
    token.setLine(line)
    token.setCharPositionInLine(charPos)
    if (txt != null) token.setText(txt)
    token
  }

  override def create(typ: Int, text: String): CypherToken = {
    val token = new FullCypherToken(null, typ, -1, -1, -1)
    token.setText(text)
    token
  }
}
