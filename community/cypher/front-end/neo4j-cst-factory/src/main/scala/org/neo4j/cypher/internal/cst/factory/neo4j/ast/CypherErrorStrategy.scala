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

import org.antlr.v4.runtime.DefaultErrorStrategy
import org.antlr.v4.runtime.FailedPredicateException
import org.antlr.v4.runtime.InputMismatchException
import org.antlr.v4.runtime.NoViableAltException
import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.RuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.Vocabulary
import org.antlr.v4.runtime.VocabularyImpl
import org.antlr.v4.runtime.misc.IntervalSet
import org.neo4j.cypher.internal.ast.factory.neo4j.ContextAwareLl1Analyzer
import org.neo4j.cypher.internal.ast.factory.neo4j.ContextAwareLl1Analyzer.Look
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import java.util

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Success
import scala.util.Try

/**
 * We use this error strategy to produce correct error messages.
 * We rely on the recovery mechanisms from the default error strategy
 * to get some of the suggestions included in errors.
 * These suggestions are not ideal in all situations (there probably exists better approaches)
 * but they are deemed good enough to be on par with what we had in the previous parser.
 */
final class CypherErrorStrategy extends DefaultErrorStrategy {
  private val vocabulary = new CypherErrorVocabulary

  private def isUnclosedQuote(offender: Token): Boolean = {
    offender.getText == "'" || offender.getText == "\""
  }

  private def isUnclosedComment(offender: Token, recognizer: Parser): Boolean = {
    (offender.getText == "/" && recognizer.getInputStream.LT(2).getText == "*") ||
    (offender.getText == "*" && recognizer.getInputStream.LT(-1).getText == "/")
  }

  override protected def reportInputMismatch(recognizer: Parser, e: InputMismatchException): Unit = {
    val offender = e.getOffendingToken

    val msg =
      if (isUnclosedQuote(offender)) CypherErrorStrategy.qouteMismatchErrorMessage
      else if (isUnclosedComment(offender, recognizer))
        "Failed to parse comment. A comment starting on `/*` must have a closing `*/`."
      else errorMessage(offender, expected(recognizer, e.getOffendingState, e.getCtx), "Mismatched input")
    recognizer.notifyErrorListeners(offender, msg, e)
  }

  override protected def reportUnwantedToken(recognizer: Parser): Unit = {
    if (!inErrorRecoveryMode(recognizer)) {
      beginErrorCondition(recognizer)
      val t = recognizer.getCurrentToken
      val msg =
        if (isUnclosedQuote(t)) CypherErrorStrategy.qouteMismatchErrorMessage
        else errorMessage(t, expected(recognizer, recognizer.getState, recognizer.getContext), "Extraneous input")
      recognizer.notifyErrorListeners(t, msg, null)
    }
  }

  override protected def reportNoViableAlternative(recognizer: Parser, e: NoViableAltException): Unit = {
    def msg = {
      val tokens = recognizer.getInputStream
      val input = {
        if (tokens != null) {
          if (e.getStartToken.getType == Token.EOF) "<EOF>"
          else tokens.getText(e.getStartToken, e.getOffendingToken)
        } else {
          "<unknown input>"
        }
      }
      val inputPart = if (input.nonEmpty) " at input " + escapeWSAndQuote(input) else ""
      val expectedPart =
        vocabulary.displayNames(e.getCtx) match {
          case names if names.nonEmpty => s": expected ${names.mkString(", ")}"
          case _                       => ""
        }
      "No viable alternative" + inputPart + expectedPart
    }
    recognizer.notifyErrorListeners(e.getOffendingToken, Try(msg).getOrElse("No viable alternative"), e)
  }

  override protected def reportMissingToken(recognizer: Parser): Unit = {
    if (!inErrorRecoveryMode(recognizer)) {
      beginErrorCondition(recognizer)
      val t = recognizer.getCurrentToken
      def msg = {
        val expecting = expected(recognizer, recognizer.getState, recognizer.getContext)
        s"Missing ${expecting.mkString(", ")} at ${getTokenErrorDisplay(t)}"
      }
      recognizer.notifyErrorListeners(t, Try(msg).getOrElse("Missing token"), null)
    }
  }

  override protected def reportFailedPredicate(recognizer: Parser, e: FailedPredicateException): Unit = {
    // We don't use predicates
    super.reportFailedPredicate(recognizer, e)
  }

  private def expected(recognizer: Parser, offendingState: Int, ctx: RuleContext): Seq[String] = {
    val atn = recognizer.getATN
    new ContextAwareLl1Analyzer(atn)
      .LOOK(atn.states.get(offendingState), ctx, new ExpectedDisplayNameCollector(vocabulary, ctx)).result()
  }

  private def errorMessage(offender: Token, expecting: => Seq[String], desc: String): String = {
    val expected = Try(expecting) match {
      case Success(names) => names.mkString(": expected ", ", ", "")
      case _              => "" // Hide errors from computing expected tokens (it's a pretty complex thing)
    }
    s"$desc ${getTokenErrorDisplay(offender)}$expected"

  }
}

class ExpectedDisplayNameCollector(vocabulary: CypherErrorVocabulary, ctx: RuleContext) extends Look {
  private val expected = ListSet.newBuilder[String]
  private val parents = vocabulary.parentSet(ctx)

  override def expect(currentRule: Int, rules: java.util.BitSet, tokenType: Int): Unit = {
    expect(currentRule, rules, IntervalSet.of(tokenType))
  }

  override def expect(currentRule: Int, rules: java.util.BitSet, set: IntervalSet): Unit = {
    // We could optimise this,
    // at certain points its no use to continue looking at a certain branch and we could stop
    // println("Parents: " + parents.stream().mapToObj(r => CypherParser.ruleNames(r)).collect(Collectors.joining(",")))
    // println("Children: " + rules.stream().mapToObj(r => CypherParser.ruleNames(r)).collect(Collectors.joining(",")))
    // println("Tokens: " + vocabulary.tokenDisplayNames(set))
    expected.addAll(vocabulary.displayNames(currentRule, parents, rules, set))
  }

  def result(): Seq[String] = {
    expected.result().toSeq
  }
}

object CypherErrorStrategy {

  val qouteMismatchErrorMessage =
    "Failed to parse string literal. The query must contain an even number of non-escaped quotes."
}

final class CypherErrorVocabulary extends Vocabulary {
  private val inner = CypherParser.VOCABULARY.asInstanceOf[VocabularyImpl]

  private val expressionRules = {
    val rules = new java.util.BitSet()
    rules.set(CypherParser.RULE_expression)
    rules.set(CypherParser.RULE_expression1)
    rules.set(CypherParser.RULE_expression2)
    rules.set(CypherParser.RULE_expression3)
    rules.set(CypherParser.RULE_expression4)
    rules.set(CypherParser.RULE_expression5)
    rules.set(CypherParser.RULE_expression6)
    rules.set(CypherParser.RULE_expression7)
    rules.set(CypherParser.RULE_expression8)
    rules.set(CypherParser.RULE_expression9)
    rules.set(CypherParser.RULE_expression10)
    rules.set(CypherParser.RULE_expression11)
    rules
  }

  override def getMaxTokenType: Int = inner.getMaxTokenType
  override def getLiteralName(tokenType: Int): String = inner.getLiteralName(tokenType)
  override def getSymbolicName(tokenType: Int): String = inner.getSymbolicName(tokenType)

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
      case Token.EOF                             => "<EOF>"
      case CypherParser.ErrorChar                => "<ErrorChar>"
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

  private def isSymbolicName(rules: java.util.BitSet): Boolean = {
    rules.get(CypherParser.RULE_unescapedSymbolicNameString) || rules.get(CypherParser.RULE_symbolicNameString)
  }

  /**
   * Returns display names for the specified rule context and expected tokens.
   *
   * @param currentRule rule index of the rule we're currently looking at
   * @param parentRules rule indexes of the rules where we failed
   * @param childRules rule indexes of the rules we're we are currently looking for expectations
   * @param tokens expected tokens for the current rule
   */
  def displayNames(
    currentRule: Int,
    parentRules: java.util.BitSet,
    childRules: java.util.BitSet,
    tokens: IntervalSet
  ): Seq[String] = {

    if (childRules.intersects(expressionRules) || expressionRules.get(currentRule)) {
      // Special treatment of expressions to get clearer suggestions.
      // We ignore parent rules here because we're still interested in
      // suggestions if we started in an expression.
      Seq("an expression")
    } else if (isSymbolicName(parentRules) || isSymbolicName(childRules)) {
      // Special treatment of symbolic names,
      // because we don't have reserved words these suggestions are very noisy otherwise.
      val rules = new java.util.BitSet()
      rules.or(parentRules)
      rules.or(childRules)
      if (rules.get(CypherParser.RULE_parameter)) {
        Seq("a parameter name")
      } else if (rules.get(CypherParser.RULE_variable)) {
        Seq("a variable name")
      } else if (
        rules.get(CypherParser.RULE_labelExpression1) ||
        rules.get(CypherParser.RULE_labelExpression1Is) ||
        rules.get(CypherParser.RULE_symbolicLabelNameString)
      ) {
        if (
          rules.get(CypherParser.RULE_nodePattern) ||
          rules.get(CypherParser.RULE_nodeLabelsIs) ||
          rules.get(CypherParser.RULE_labelType) ||
          rules.get(CypherParser.RULE_insertNodeLabelExpression)
        ) {
          Seq("a node label name")
        } else if (
          rules.get(CypherParser.RULE_relationshipPattern) ||
          rules.get(CypherParser.RULE_insertRelationshipLabelExpression)
        ) {
          Seq("a relationship type name")
        } else {
          Seq("a node label name", "a relationship type name")
        }
      } else {
        Seq("an identifier")
      }
    } else {
      tokenDisplayNames(tokens)
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

  def displayNames(ctx: RuleContext): Seq[String] =
    displayNames(
      currentRule = Option(ctx).map(_.getRuleIndex).getOrElse(-1),
      parentRules = new java.util.BitSet(),
      childRules = parentSet(ctx),
      tokens = IntervalSet.EMPTY_SET
    )

  def tokenDisplayNames(set: IntervalSet): Seq[String] = {
    set.getIntervals.asScala
      .flatMap(i =>
        Range.inclusive(i.a, i.b)
          .filter(_ != Token.EPSILON)
          .map(getDisplayName)
      )
      .toSeq
  }
}
