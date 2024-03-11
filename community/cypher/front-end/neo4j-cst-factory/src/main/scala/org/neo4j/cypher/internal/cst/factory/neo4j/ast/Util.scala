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

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.cst.factory.neo4j.CypherToken
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.reflect.ClassTag

object Util {
  @inline def cast[T](o: Any): T = o.asInstanceOf[T]

  @inline def child[T <: ParseTree](ctx: AstRuleCtx, index: Int): T = ctx.getChild(index).asInstanceOf[T]

  @inline def astOpt[T <: Any](ctx: AstRuleCtx): Option[T] = if (ctx == null) None else Some(ctx.ast[T]())
  @inline def astOpt[T](ctx: AstRuleCtx, default: => T): T = if (ctx == null) default else ctx.ast[T]()
  @inline def ctxChild(ctx: AstRuleCtx, index: Int): AstRuleCtx = ctx.getChild(index).asInstanceOf[AstRuleCtx]

  @inline def astChild[T <: ASTNode](ctx: AstRuleCtx, index: Int): T =
    ctx.getChild(index).asInstanceOf[AstRuleCtx].ast()

  def astSeq[T: ClassTag](list: java.util.List[_ <: ParseTree], from: Int = 0): ArraySeq[T] = {
    val size = list.size()
    val result = new Array[T]((size - from))
    var i = from; var dest = 0
    while (i < size) {
      result(dest) = list.get(i).asInstanceOf[AstRuleCtx].ast[T]()
      i += 1
      dest += 1
    }
    ArraySeq.unsafeWrapArray(result)
  }

  def astSeq[T: ClassTag](list: java.util.List[_ <: ParseTree], offset: Int, step: Int): ArraySeq[T] = {
    val size = list.size()
    val result = new Array[T](size / step)
    var i = offset; var dest = 0
    while (i < size) {
      result(dest) = list.get(i).asInstanceOf[AstRuleCtx].ast[T]()
      i += step
      dest += 1
    }
    ArraySeq.unsafeWrapArray(result)
  }

  def astChildListSet[T <: ASTNode](ctx: AstRuleCtx): ListSet[T] = {
    ListSet.from(collectAst(ctx))
  }

  def astCtxReduce[T <: ASTNode, C <: AstRuleCtx](ctx: AstRuleCtx, f: (T, C) => T): T = {
    val children = ctx.children; val size = children.size()
    var result = children.get(0).asInstanceOf[AstRuleCtx].ast[T]()
    var i = 1
    while (i < size) {
      result = f(result, children.get(i).asInstanceOf[C])
      i += 1
    }
    result
  }

  def astBinaryFold[T <: ASTNode](ctx: AstRuleCtx, f: (T, TerminalNode, T) => T): T = {
    ctx.children.size match {
      case 1 => ctxChild(ctx, 0).ast()
      case size =>
        val z = f(ctxChild(ctx, 0).ast(), child(ctx, 1), ctxChild(ctx, 2).ast())
        if (size == 3) z
        else ctx.children.asScala.drop(3).toSeq.grouped(2).foldLeft(z) {
          case (acc, Seq(token: TerminalNode, rhs: AstRuleCtx)) => f(acc, token, rhs.ast())
          case _ => throw new IllegalStateException(s"Unexpected parse result $ctx")
        }
    }
  }

  private def collectAst[T <: ASTNode](ctx: AstRuleCtx): Iterable[T] = {
    ctx.children.asScala.collect {
      case astCtx: AstRuleCtx => astCtx.ast[T]()
    }
  }
  @inline def nodeChild(ctx: AstRuleCtx, index: Int): TerminalNode = ctx.getChild(index).asInstanceOf[TerminalNode]

  @inline def nodeChildType(ctx: AstRuleCtx, index: Int): Int =
    ctx.getChild(index).asInstanceOf[TerminalNode].getSymbol.getType

  @inline def lastChild[T <: ParseTree](ctx: AstRuleCtx): T =
    ctx.children.get(ctx.children.size() - 1).asInstanceOf[T]

  def astPairs[A <: ASTNode, B <: ASTNode](
    as: java.util.List[_ <: AstRuleCtx],
    bs: java.util.List[_ <: AstRuleCtx]
  ): ArraySeq[(A, B)] = {
    val result = new Array[(A, B)](as.size())
    var i = 0; val length = as.size()
    while (i < length) {
      result(i) = (as.get(i).ast[A](), bs.get(i).ast[B]())
      i += 1
    }
    ArraySeq.unsafeWrapArray(result)
  }

  @inline def pos(token: Token): InputPosition = token.asInstanceOf[CypherToken].position()
  @inline def pos(ctx: ParserRuleContext): InputPosition = pos(ctx.start)

  @inline def pos(node: TerminalNode): InputPosition = pos(node.getSymbol)
}
