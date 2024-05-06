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

import org.antlr.v4.runtime.ANTLRErrorStrategy
import org.antlr.v4.runtime.InputMismatchException
import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.RuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.Vocabulary
import org.antlr.v4.runtime.VocabularyImpl
import org.antlr.v4.runtime.misc.IntervalSet
import org.neo4j.cypher.internal.ast.factory.neo4j.completion.CodeCompletionCore
import org.neo4j.cypher.internal.parser.CypherParser

import java.util

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.math.Ordering.Implicits.seqOrdering
import scala.util.control.NonFatal

/**
 * Error strategy where we never try to recover.
 * On errors we only compute code completion at the offending token and report back.
 * Based on https://github.com/neo4j/cypher-language-support/blob/main/packages/language-support/src/syntaxValidation/completionCoreErrors.ts
 * Please consider updating there if you make improvements here.
 */
final class CypherErrorStrategy extends ANTLRErrorStrategy {
  private val vocabulary = new CypherErrorVocabulary
  private var inErrorMode = false

  override def reportError(parser: Parser, e: RecognitionException): Unit = {
    // Only reports first error
    if (!inErrorRecoveryMode(parser)) {
      beginErrorCondition()
      populateException(parser.getContext, e)
      parser.notifyErrorListeners(e.getOffendingToken, message(parser, e), e)
    }
  }

  private def beginErrorCondition(): Unit = {
    inErrorMode = true
  }

  private def message(parser: Parser, e: RecognitionException): String = {
    // println("Error at " + e.getCtx.getClass.getSimpleName)
    if (isUnclosedQuote(e.getOffendingToken)) {
      CypherErrorStrategy.quoteMismatchErrorMessage
    } else if (isUnclosedComment(e.getOffendingToken, parser)) {
      CypherErrorStrategy.commentMismatchErrorMessage
    } else {
      val offender = Option(e.getOffendingToken)
        .filter(t => t.getType != Token.EOF && t.getType != Token.EPSILON && t.getType != Token.INVALID_TYPE)
        .flatMap(t => Option(t.getText))
        .map(t => t.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t"))
        .getOrElse("")
      val expected = codeCompletion(parser, e) match {
        case Seq(e)          => s": expected $e"
        case e if e.nonEmpty => e.dropRight(1).mkString(": expected ", ", ", "") + " or " + e.last
        case _               => ""
      }
      s"Invalid input '$offender'$expected"
    }
  }

  override def recoverInline(parser: Parser): Token = {
    // We don't want to recover from errors, just throw here to stop parsing.
    // The parser will catch this exception and report it later.
    val e = new InputMismatchException(parser, parser.getState, parser.getContext)
    populateException(parser.getContext, e)
    throw e
  }

  override def recover(parser: Parser, e: RecognitionException): Unit = {
    // We don't want to recover from errors.
    // At this point the error has already been reported so do nothing.
    if (inErrorRecoveryMode(parser)) {
      // Consume a single token to prevent an infinite loop, this is a failsafe.
      parser.consume()
    }
  }

  override def sync(parser: Parser): Unit = {}

  override def inErrorRecoveryMode(recognizer: Parser): Boolean = inErrorMode

  override def reset(recognizer: Parser): Unit = inErrorMode = false

  override def reportMatch(recognizer: Parser): Unit = {}

  private def codeCompletion(parser: Parser, e: RecognitionException): Seq[String] = {
    try {
      val completion = new CodeCompletionCore(parser, vocabulary.rulesOfInterest, vocabulary.ignoredTokens)
      val tokenIndex = e.getOffendingToken.getTokenIndex
      vocabulary.expected(completion.collectCandidates(tokenIndex, e.getCtx.asInstanceOf[ParserRuleContext]))
    } catch {
      case NonFatal(_) =>
        // Hide bugs in code completion and fallback to default antlr expected tokens
        vocabulary.tokenDisplayNames(e.getExpectedTokens)
    }
  }

  private def isUnclosedQuote(offender: Token): Boolean = {
    offender.getText == "'" || offender.getText == "\""
  }

  private def isUnclosedComment(offender: Token, recognizer: Parser): Boolean = {
    (offender.getText == "/" && recognizer.getInputStream.LT(2).getText == "*") ||
    (offender.getText == "*" && recognizer.getInputStream.LT(-1).getText == "/")
  }

  @tailrec
  private def populateException(ctx: ParserRuleContext, e: RecognitionException): Unit = {
    if (ctx != null) {
      ctx.exception = e
      populateException(ctx.getParent, e)
    }
  }
}

object CypherErrorStrategy {

  val quoteMismatchErrorMessage =
    "Failed to parse string literal. The query must contain an even number of non-escaped quotes."
  val commentMismatchErrorMessage = "Failed to parse comment. A comment starting on `/*` must have a closing `*/`."
}

final class CypherErrorVocabulary extends Vocabulary {
  private val inner = CypherParser.VOCABULARY.asInstanceOf[VocabularyImpl]

  val rulesOfInterest: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    CypherParser.RULE_expression,
    CypherParser.RULE_expression1,
    CypherParser.RULE_expression2,
    CypherParser.RULE_expression3,
    CypherParser.RULE_expression4,
    CypherParser.RULE_expression5,
    CypherParser.RULE_expression6,
    CypherParser.RULE_expression7,
    CypherParser.RULE_expression8,
    CypherParser.RULE_expression9,
    CypherParser.RULE_expression10,
    CypherParser.RULE_expression11,
    CypherParser.RULE_stringLiteral,
    CypherParser.RULE_numberLiteral,
    CypherParser.RULE_parameter,
    CypherParser.RULE_variable,
    CypherParser.RULE_symbolicNameString,
    CypherParser.RULE_escapedSymbolicNameString,
    CypherParser.RULE_unescapedSymbolicNameString,
    CypherParser.RULE_symbolicLabelNameString,
    CypherParser.RULE_unescapedLabelSymbolicNameString,
    CypherParser.RULE_symbolicAliasName,
    CypherParser.RULE_pattern
  )

  val ignoredTokens: util.Set[Integer] = java.util.Set.of[java.lang.Integer](
    Token.EPSILON,
    CypherParser.SEMICOLON
  )

  def expected(candidates: CodeCompletionCore.CandidatesCollection): Seq[String] = {
    // println("Candidates:")
    // println("Tokens: " + candidates.tokens.keySet().asScala.map(t => getDisplayName(t)).mkString(", "))
    // println("Rules: " + candidates.rules.entrySet().asScala
    //   .map { e =>
    //     CypherParser.ruleNames(e.getKey) +
    //       " (callstack: " + e.getValue.asScala.map(r => CypherParser.ruleNames(r)).mkString(", ") + ")"
    //   }
    //   .mkString(", ")
    // )

    val ruleNames = candidates.rules.entrySet().asScala.toSeq
      .flatMap(e => ruleDisplayName(e.getKey, e.getValue))
      .sorted

    val tokenNames = candidates.tokens.entrySet().asScala.toSeq
      .map(e => e.getKey +: e.getValue.asScala.toSeq)
      // Make sure for example 'BTREE INDEX' and 'FULLTEXT INDEX' are next to each other
      .sortBy(_.reverse.map(t => getDisplayName(t)))
      .map(displayName)
    (ruleNames ++ tokenNames).distinct
  }

  private def ruleDisplayName(ruleIndex: Int, ruleCallStack: java.util.List[java.lang.Integer]): Option[String] = {
    def inStack(rules: Int*): Boolean = rules.forall(r => ruleCallStack.contains(r))
    ruleIndex match {
      case CypherParser.RULE_expression |
        CypherParser.RULE_expression1 |
        CypherParser.RULE_expression2 |
        CypherParser.RULE_expression3 |
        CypherParser.RULE_expression4 |
        CypherParser.RULE_expression5 |
        CypherParser.RULE_expression6 |
        CypherParser.RULE_expression7 |
        CypherParser.RULE_expression8 |
        CypherParser.RULE_expression9 |
        CypherParser.RULE_expression10 |
        CypherParser.RULE_expression11 =>
        Some("an expression")
      case CypherParser.RULE_stringLiteral => Some("a string")
      case CypherParser.RULE_numberLiteral => Some("a number")
      case CypherParser.RULE_parameter     => Some("a parameter")
      case CypherParser.RULE_variable      => Some("a variable name")
      case CypherParser.RULE_symbolicNameString |
        CypherParser.RULE_escapedSymbolicNameString |
        CypherParser.RULE_unescapedSymbolicNameString |
        CypherParser.RULE_symbolicLabelNameString |
        CypherParser.RULE_unescapedLabelSymbolicNameString =>
        if (inStack(CypherParser.RULE_labelExpression, CypherParser.RULE_relationshipPattern))
          Some("a relationship type name")
        else if (inStack(CypherParser.RULE_labelExpression, CypherParser.RULE_nodePattern))
          Some("a node label name")
        else if (inStack(CypherParser.RULE_labelExpression1))
          Some("a node label/relationship type name")
        else
          Some("an identifier")
      case CypherParser.RULE_symbolicAliasName => Some("a database name")
      case CypherParser.RULE_pattern           => Some("a graph pattern")
      case _                                   => None
    }
  }

  override def getMaxTokenType: Int = inner.getMaxTokenType
  override def getLiteralName(tokenType: Int): String = inner.getLiteralName(tokenType)
  override def getSymbolicName(tokenType: Int): String = inner.getSymbolicName(tokenType)

  def displayName(tokenTypes: Seq[Integer]): String = {
    if (tokenTypes.forall(t => getDisplayName(t) == "'" + getSymbolicName(t) + "'"))
      tokenTypes.map(t => getSymbolicName(t)).mkString("'", " ", "'")
    else getDisplayName(tokenTypes.head)
  }

  override def getDisplayName(tokenType: Int): String = {
    tokenType match {
      case CypherParser.SPACE                    => "' '"
      case CypherParser.SINGLE_LINE_COMMENT      => "'//'"
      case CypherParser.DECIMAL_DOUBLE           => "a float value"
      case CypherParser.UNSIGNED_DECIMAL_INTEGER => "an integer value"
      case CypherParser.UNSIGNED_HEX_INTEGER     => "a hexadecimal integer value"
      case CypherParser.UNSIGNED_OCTAL_INTEGER   => "an octal integer value"
      case CypherParser.IDENTIFIER               => "an identifier"
      case CypherParser.ARROW_LINE               => "'-'"
      case CypherParser.ARROW_LEFT_HEAD          => "'<'"
      case CypherParser.ARROW_RIGHT_HEAD         => "'>'"
      case CypherParser.MULTI_LINE_COMMENT       => "'/*'"
      case CypherParser.STRING_LITERAL1          => "a string value"
      case CypherParser.STRING_LITERAL2          => "a string value"
      case CypherParser.ESCAPED_SYMBOLIC_NAME    => "an identifier"
      case CypherParser.ALL_SHORTEST_PATHS       => "'allShortestPaths'"
      case CypherParser.SHORTEST_PATH            => "'shortestPath'"
      case CypherParser.LIMITROWS                => "'LIMIT'"
      case CypherParser.SKIPROWS                 => "'SKIP'"
      case Token.EOF                             => "<EOF>"
      case _ =>
        val displayNames = inner.getDisplayNames
        if (tokenType > 0 && tokenType < displayNames.length && displayNames(tokenType) != null) displayNames(tokenType)
        else {
          Option(inner.getLiteralName(tokenType))
            .orElse(Option(inner.getSymbolicName(tokenType)).map(n => "'" + n + "'"))
            .getOrElse(tokenType.toString)
        }
    }
  }

  @tailrec
  def parentSet(ctx: RuleContext, rules: util.BitSet = new util.BitSet()): util.BitSet = {
    ctx match {
      case null => rules
      case someCtx =>
        rules.set(someCtx.getRuleIndex)
        parentSet(someCtx.parent, rules)
    }
  }

  def tokenDisplayNames(set: IntervalSet): Seq[String] = {
    set.getIntervals.asScala
      .flatMap(i =>
        Range.inclusive(i.a, i.b)
          .filter(t => t != Token.EPSILON || t != CypherParser.ErrorChar)
          .map(getDisplayName)
      )
      .toSeq
      .sorted
  }
}
