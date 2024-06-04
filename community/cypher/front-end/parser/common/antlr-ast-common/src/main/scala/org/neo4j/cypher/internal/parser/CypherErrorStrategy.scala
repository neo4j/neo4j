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
package org.neo4j.cypher.internal.parser

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
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.CypherRuleGroup
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.DatabaseNameRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.GraphPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.IdentifierRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpression1Rule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.LabelExpressionRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NodePatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.NumberLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.ParameterRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.RelationshipPatternRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.StringLiteralRule
import org.neo4j.cypher.internal.parser.CypherErrorStrategy.VariableRule

import java.util

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.math.Ordering.Implicits.seqOrdering
import scala.util.control.NonFatal

/**
 * Error strategy where we never try to recover.
 * On errors we only compute code completion at the offending token and report back.
 * Based on https://github.com/neo4j/cypher-language-support/blob/main/packages/language-support/src/syntaxValidation/completionCoreErrors.ts
 * Please consider updating there if you make improvements here.
 */
final class CypherErrorStrategy(conf: CypherErrorStrategy.Conf) extends ANTLRErrorStrategy {
  private val vocabulary = new CypherErrorVocabulary(conf)
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
      val completion = new CodeCompletionCore(parser, conf.preferredRules.asJava, conf.ignoredTokens)
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

  trait Conf {
    def vocabulary: VocabularyImpl

    def preferredRules: Set[java.lang.Integer] = {
      val preferredGroups = Set[CypherRuleGroup](
        ExpressionRule,
        StringLiteralRule,
        NumberLiteralRule,
        ParameterRule,
        VariableRule,
        IdentifierRule,
        DatabaseNameRule,
        GraphPatternRule
      )
      ruleGroups.view
        .collect { case (i, group) if preferredGroups.contains(group) => java.lang.Integer.valueOf(i) }
        .toSet
    }
    def ignoredTokens: util.Set[Integer]
    def customTokenDisplayNames: Map[Int, String]
    def errorCharTokenType: Int
    def ruleGroups: Map[Int, CypherRuleGroup]
    def ruleNames: Array[String]
  }

  sealed trait CypherRuleGroup
  case object ExpressionRule extends CypherRuleGroup
  case object StringLiteralRule extends CypherRuleGroup
  case object NumberLiteralRule extends CypherRuleGroup
  case object ParameterRule extends CypherRuleGroup
  case object VariableRule extends CypherRuleGroup
  case object IdentifierRule extends CypherRuleGroup
  case object DatabaseNameRule extends CypherRuleGroup
  case object GraphPatternRule extends CypherRuleGroup
  case object LabelExpressionRule extends CypherRuleGroup
  case object LabelExpression1Rule extends CypherRuleGroup
  case object NodePatternRule extends CypherRuleGroup
  case object RelationshipPatternRule extends CypherRuleGroup

  val quoteMismatchErrorMessage =
    "Failed to parse string literal. The query must contain an even number of non-escaped quotes."
  val commentMismatchErrorMessage = "Failed to parse comment. A comment starting on `/*` must have a closing `*/`."
}

final class CypherErrorVocabulary(conf: CypherErrorStrategy.Conf) extends Vocabulary {

  def expected(candidates: CodeCompletionCore.CandidatesCollection): Seq[String] = {
    // println("Candidates:")
    // println("Tokens: " + candidates.tokens.keySet().asScala.map(t => getDisplayName(t)).mkString(", "))
    // println("Rules: " + candidates.rules.entrySet().asScala
    //   .map { e =>
    //     conf.ruleNames(e.getKey) +
    //       " (callstack: " + e.getValue.asScala.map(r => conf.ruleNames(r)).mkString(", ") + ")"
    //   }
    //   .mkString(", ")
    // )

    val ruleNames = candidates.rules.entrySet().asScala.toSeq
      .flatMap(e => ruleDisplayName(e.getKey, e.getValue.asScala))
      .sorted

    val tokenNames = candidates.tokens.entrySet().asScala.toSeq
      .map(e => e.getKey +: e.getValue.asScala.toSeq)
      // Make sure for example 'BTREE INDEX' and 'FULLTEXT INDEX' are next to each other
      .sortBy(_.reverse.map(t => getDisplayName(t)))
      .map(displayName)
    (ruleNames ++ tokenNames).distinct
  }

  private def ruleDisplayName(ruleIndex: Int, ruleCallStack: collection.Seq[java.lang.Integer]): Option[String] = {
    def inStack(gs: CypherRuleGroup*): Boolean =
      gs.forall(g => ruleCallStack.exists(r => conf.ruleGroups.get(r).contains(g)))

    conf.ruleGroups.get(ruleIndex).collect {
      case ExpressionRule    => "an expression"
      case StringLiteralRule => "a string"
      case NumberLiteralRule => "a number"
      case ParameterRule     => "a parameter"
      case VariableRule      => "a variable name"
      case IdentifierRule =>
        if (inStack(LabelExpressionRule, RelationshipPatternRule)) "a relationship type name"
        else if (inStack(LabelExpressionRule, NodePatternRule)) "a node label name"
        else if (inStack(LabelExpression1Rule)) "a node label/relationship type name"
        else "an identifier"
      case DatabaseNameRule => "a database name"
      case GraphPatternRule => "a graph pattern"
    }
  }

  override def getMaxTokenType: Int = conf.vocabulary.getMaxTokenType
  override def getLiteralName(tokenType: Int): String = conf.vocabulary.getLiteralName(tokenType)
  override def getSymbolicName(tokenType: Int): String = conf.vocabulary.getSymbolicName(tokenType)

  def displayName(tokenTypes: Seq[Integer]): String = {
    if (tokenTypes.forall(t => getDisplayName(t) == "'" + getSymbolicName(t) + "'"))
      tokenTypes.map(t => getSymbolicName(t)).mkString("'", " ", "'")
    else getDisplayName(tokenTypes.head)
  }

  override def getDisplayName(tokenType: Int): String = {
    conf.customTokenDisplayNames.get(tokenType) match {
      case Some(name) => name
      case _ =>
        val displayNames = conf.vocabulary.getDisplayNames
        if (tokenType > 0 && tokenType < displayNames.length && displayNames(tokenType) != null) displayNames(tokenType)
        else {
          Option(conf.vocabulary.getLiteralName(tokenType))
            .orElse(Option(conf.vocabulary.getSymbolicName(tokenType)).map(n => "'" + n + "'"))
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
      .flatMap { i =>
        Range.inclusive(i.a, i.b)
          .filter(t => t != Token.EPSILON || t != conf.errorCharTokenType)
          .map(getDisplayName)
      }
      .toSeq
      .sorted
  }
}
