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
package org.neo4j.cypher.internal.parser.v5.ast.factory

import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherErrorStrategy
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.ast.util.Util.rangePos
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser
import org.neo4j.cypher.internal.parser.v5.Cypher5ParserListener
import org.neo4j.cypher.internal.parser.v5.ast.factory.LiteralBuilder.cypherStringToString
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

trait LiteralBuilder extends Cypher5ParserListener {

  protected def exceptionFactory: CypherExceptionFactory

  override def exitLiteral(ctx: Cypher5Parser.LiteralContext): Unit = {
    ctx.ast = ctx.children.get(0) match {
      case rule: AstRuleCtx => rule.ast
      case token: TerminalNode => token.getSymbol.getType match {
          case Cypher5Parser.TRUE                         => True()(pos(ctx))
          case Cypher5Parser.FALSE                        => False()(pos(ctx))
          case Cypher5Parser.INF | Cypher5Parser.INFINITY => Infinity()(pos(ctx))
          case Cypher5Parser.NAN                          => NaN()(pos(ctx))
          case Cypher5Parser.NULL                         => Null()(pos(ctx))
        }
      case other => throw new IllegalStateException(s"Unexpected child $other")
    }
  }

  final override def exitNumberLiteral(ctx: Cypher5Parser.NumberLiteralContext): Unit = {
    ctx.ast = lastChild[TerminalNode](ctx).getSymbol.getType match {
      case Cypher5Parser.UNSIGNED_DECIMAL_INTEGER => SignedDecimalIntegerLiteral(ctx.getText)(pos(ctx))
      case Cypher5Parser.DECIMAL_DOUBLE           => DecimalDoubleLiteral(ctx.getText)(pos(ctx))
      case Cypher5Parser.UNSIGNED_HEX_INTEGER     => SignedHexIntegerLiteral(ctx.getText)(pos(ctx))
      case Cypher5Parser.UNSIGNED_OCTAL_INTEGER   => SignedOctalIntegerLiteral(ctx.getText)(pos(ctx))
    }
  }

  final override def exitSignedIntegerLiteral(
    ctx: Cypher5Parser.SignedIntegerLiteralContext
  ): Unit = {
    ctx.ast = if (ctx.MINUS() != null) {
      SignedDecimalIntegerLiteral("-" + ctx.UNSIGNED_DECIMAL_INTEGER().getText)(pos(ctx))
    } else {
      SignedDecimalIntegerLiteral(ctx.UNSIGNED_DECIMAL_INTEGER().getText)(pos(ctx))
    }
  }

  final override def exitListLiteral(
    ctx: Cypher5Parser.ListLiteralContext
  ): Unit = {
    ctx.ast = ListLiteral(astSeq[Expression](ctx.expression()))(pos(ctx))
  }

  override def exitStringLiteral(ctx: Cypher5Parser.StringLiteralContext): Unit = {
    val text = ctx.start.getInputStream.getText(new Interval(ctx.start.getStartIndex + 1, ctx.stop.getStopIndex - 1))
    ctx.ast = StringLiteral(cypherStringToString(text, pos(ctx), exceptionFactory))(rangePos(ctx))
  }
}

object LiteralBuilder {

  /*
   * This is the recommended way to handle escape sequences
   * by the antlr faq:
   *
   * > It's easier and more efficient to return original
   * > input string and then use a small function to rewrite
   * > the string later during a parse tree walk...
   *
   * https://github.com/antlr/antlr4/blob/dev/doc/faq/lexical.md#how-do-i-replace-escape-characters-in-string-tokens
   */
  final def cypherStringToString(input: String, p: InputPosition, exceptionFactory: CypherExceptionFactory): String = {
    var pos = input.indexOf('\\')
    if (pos == -1) {
      input
    } else {
      var start = 0
      val length = input.length
      var builder: java.lang.StringBuilder = null
      while (pos != -1) {
        if (pos == length - 1)
          throw exceptionFactory.syntaxException(CypherErrorStrategy.quoteMismatchErrorMessage, p)
        val replacement: Char = input.charAt(pos + 1) match {
          case 't'  => '\t'
          case 'b'  => '\b'
          case 'n'  => '\n'
          case 'r'  => '\r'
          case 'f'  => '\f'
          case '\'' => '\''
          case '"'  => '"'
          case '\\' => '\\'
          case _    => Char.MinValue
        }
        if (replacement != Char.MinValue) {
          if (builder == null) builder = new java.lang.StringBuilder(input.length)
          builder.append(input, start, pos).append(replacement)
          start = pos + 2
        }
        pos = input.indexOf('\\', pos + 2)
      }
      if (builder == null || builder.isEmpty) input
      else if (start < input.length) builder.append(input, start, input.length).toString
      else builder.toString
    }
  }
}
