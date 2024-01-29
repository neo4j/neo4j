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
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.child
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener

trait LiteralBuilder extends CypherParserListener {

  final override def exitPathLengthLiteral(
    ctx: CypherParser.PathLengthLiteralContext
  ): Unit = {}

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
  ): Unit = {}

  final override def exitMapLiteral(
    ctx: CypherParser.MapLiteralContext
  ): Unit = {}

  override def exitNummericLiteral(
    ctx: CypherParser.NummericLiteralContext
  ): Unit = {}

  override def exitStringsLiteral(ctx: CypherParser.StringsLiteralContext): Unit =
    ctx.ast = child[AstRuleCtx](ctx, 0).ast

  override def exitStringLiteral(ctx: CypherParser.StringLiteralContext): Unit = {
    val text = ctx.start.getInputStream.getText(new Interval(ctx.start.getStartIndex + 1, ctx.stop.getStopIndex - 1))
    ctx.ast = StringLiteral(cypherStringToString(text))(pos(ctx))
  }

  override def exitOtherLiteral(
    ctx: CypherParser.OtherLiteralContext
  ): Unit = {}

  override def exitBooleanLiteral(
    ctx: CypherParser.BooleanLiteralContext
  ): Unit = {}

  override def exitKeywordLiteral(ctx: CypherParser.KeywordLiteralContext): Unit = {
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case CypherParser.INFINITY => Infinity()(pos(ctx))
      case CypherParser.NAN      => NaN()(pos(ctx))
      case CypherParser.NULL     => Null()(pos(ctx))
    }
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
        builder.append(input, start, pos)
        input.charAt(pos + 1) match {
          case 't'  => builder.append('\t')
          case 'b'  => builder.append('\b')
          case 'n'  => builder.append('\n')
          case 'r'  => builder.append('\r')
          case 'f'  => builder.append('\f')
          case '\'' => builder.append('\'')
          case '"'  => builder.append('"')
          case '\\' => builder.append('\\')
          case 'u' =>
            builder.appendCodePoint(Integer.parseInt(input.substring(pos + 2, pos + 6), 16))
            pos += 4
          case other => builder.append('\\').append(other)
        }
        start = pos + 2
        pos = input.indexOf('\\', start)
      }
      if (start < input.length) builder.append(input, start, input.length)
      builder.toString
    }
  }
}
