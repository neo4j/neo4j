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

import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.LiteralBuilder.cypherStringToString
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions._
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener

trait LiteralBuilder extends CypherParserListener {

  override def exitLiteral(ctx: CypherParser.LiteralContext): Unit = {
    ctx.ast = ctx.children.get(0) match {
      case rule: AstRuleCtx => rule.ast
      case token: TerminalNode => token.getSymbol.getType match {
          case CypherParser.TRUE     => True()(pos(ctx))
          case CypherParser.FALSE    => False()(pos(ctx))
          case CypherParser.INFINITY => Infinity()(pos(ctx))
          case CypherParser.NAN      => NaN()(pos(ctx))
          case CypherParser.NULL     => Null()(pos(ctx))
        }
      case other => throw new IllegalStateException(s"Unexpected child $other")
    }
  }

  final override def exitNumberLiteral(ctx: CypherParser.NumberLiteralContext): Unit = {
    ctx.ast = lastChild[TerminalNode](ctx).getSymbol.getType match {
      case CypherParser.UNSIGNED_DECIMAL_INTEGER => SignedDecimalIntegerLiteral(ctx.getText)(pos(ctx))
      case CypherParser.DECIMAL_DOUBLE           => DecimalDoubleLiteral(ctx.getText)(pos(ctx))
      case CypherParser.UNSIGNED_HEX_INTEGER     => SignedHexIntegerLiteral(ctx.getText)(pos(ctx))
      case CypherParser.UNSIGNED_OCTAL_INTEGER   => SignedOctalIntegerLiteral(ctx.getText)(pos(ctx))
    }
  }

  final override def exitSignedIntegerLiteral(
    ctx: CypherParser.SignedIntegerLiteralContext
  ): Unit = {}

  final override def exitListLiteral(
    ctx: CypherParser.ListLiteralContext
  ): Unit = {
    ctx.ast = ListLiteral(astSeq[Expression](ctx.expression()))(pos(ctx))
  }

  override def exitStringLiteral(ctx: CypherParser.StringLiteralContext): Unit = {
    val text = ctx.start.getInputStream.getText(new Interval(ctx.start.getStartIndex + 1, ctx.stop.getStopIndex - 1))
    ctx.ast = StringLiteral(cypherStringToString(text))(pos(ctx), pos(ctx.stop))
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
  final def cypherStringToString(input: String): String = {
    var pos = input.indexOf('\\')
    if (pos == -1) {
      input
    } else {
      var start = 0
      val builder = new java.lang.StringBuilder(input.length)
      while (pos != -1) {
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
          builder.append(input, start, pos).append(replacement)
          start = pos + 2
        }
        pos = input.indexOf('\\', pos + 2)
      }
      if (builder.isEmpty) input
      else if (start < input.length) builder.append(input, start, input.length).toString
      else builder.toString
    }
  }
}
