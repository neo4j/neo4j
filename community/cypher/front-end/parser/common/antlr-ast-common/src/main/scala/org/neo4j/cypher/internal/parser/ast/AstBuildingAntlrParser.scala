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

import org.antlr.v4.runtime.ANTLRErrorStrategy
import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeListener
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.AstBuildingAntlrParser.DEBUG

import scala.util.control.NonFatal

/** Helper trait for parsers that builds Neo4j AST. Fails fast. Optimised for memory by removing the parse as we go. */
trait AstBuildingAntlrParser extends Parser {
  private[this] var astBuilder: ParseTreeListener = _
  private[this] var checker: SyntaxChecker = _
  private[this] var hasFailed: Boolean = false
  private[this] var bailErrors: Boolean = false

  def createSyntaxChecker(): SyntaxChecker
  def createAstBuilder(): ParseTreeListener
  def isSafeToFreeChildren(ctx: ParserRuleContext): Boolean

  final override def exitRule(): Unit = {
    val localCtx = getContext
    super.exitRule()

    if (bailErrors) {
      // In this mode we care more about speed than correct error handling
      checker.exitEveryRule(localCtx)
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
    if (isSafeToFreeChildren(localCtx)) {
      localCtx.children = null
    }
  }

  private def buildAstWithErrorHandling(ctx: ParserRuleContext): Unit = {
    try {
      astBuilder.exitEveryRule(ctx)
      if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} AST=${ctx.asInstanceOf[AstRuleCtx].ast}")
    } catch {
      case NonFatal(e) =>
        if (DEBUG) println(s"Exit ${ctx.getClass.getSimpleName} FAILED! $e")
        hasFailed = true
        throw e
    }
  }

  final override def createTerminalNode(parent: ParserRuleContext, t: Token): TerminalNode = {
    t match {
      case ct: TerminalNode => ct
      case _                => super.createTerminalNode(parent, t)
    }
  }

  final override def reset(): Unit = {
    super.reset()
    astBuilder = createAstBuilder()
    checker = createSyntaxChecker()
    hasFailed = false
  }

  final override def addParseListener(listener: ParseTreeListener): Unit = throw new UnsupportedOperationException()

  final override def setErrorHandler(handler: ANTLRErrorStrategy): Unit = {
    super.setErrorHandler(handler)
    bailErrors = handler.isInstanceOf[BailErrorStrategy]
  }

  final override def notifyErrorListeners(offendingToken: Token, msg: String, e: RecognitionException): Unit = {
    hasFailed = true
    super.notifyErrorListeners(offendingToken, msg, e)
  }

  final def syntaxChecker(): SyntaxChecker = checker
}

object AstBuildingAntlrParser {
  final private val DEBUG = false
}

trait SyntaxChecker extends ParseTreeListener {
  def check(ctx: ParserRuleContext): Boolean
  def errors: Seq[Throwable]
}

object NoOpParseTreeListener extends ParseTreeListener {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
