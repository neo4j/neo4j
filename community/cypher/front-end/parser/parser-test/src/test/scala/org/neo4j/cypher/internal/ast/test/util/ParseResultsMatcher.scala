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
package org.neo4j.cypher.internal.ast.test.util

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ObfuscationPolicy
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseFailure
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseResult
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseSuccess
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.ast.test.util.AstParsing.parseAst
import org.neo4j.cypher.internal.ast.test.util.MatchResults.merge
import org.neo4j.cypher.internal.ast.test.util.VerifyAstPositionTestSupport.findPosMismatch
import org.neo4j.cypher.internal.ast.test.util.VerifyStatementUseGraph.findUseGraphMismatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.label_expressions.BinaryLabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.MultiOperatorLabelExpression
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.parser.ast.AstBuildingAntlrParser
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.GqlExceptionMatcher
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.InvalidSyntaxStatus
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.kernel.database.DatabaseReference
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.a
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.must.Matchers.startWith
import org.scalatest.matchers.should.Matchers.should

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** ScalaTest Matcher for ParseResults */
case class ParseResultsMatcher[T <: ASTNode : ClassTag](
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty,
  override val support: ParserInTest => Boolean = _ => true,
  override val ignorePrettifier: Boolean = false
) extends Matcher[ParseResults[_]] with FluentMatchers[ParseResultsMatcher[T], T] {
  type Self = ParseResultsMatcher[T]
  override def apply(results: ParseResults[_]): MatchResult = merge(matchers.map(_.apply(results)))
  override protected def createForParser(s: ParserInTest): Self = copy(matchers = Seq.empty, support = _ == s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

/** ScalaTest Matcher for Cypher strings */
case class ParseStringMatcher[T <: ASTNode : ClassTag](
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty,
  override val support: ParserInTest => Boolean = _ => true,
  override val ignorePrettifier: Boolean = false
)(implicit p: Parsers[T]) extends Matcher[String] with FluentMatchers[ParseStringMatcher[T], T] {
  type Self = ParseStringMatcher[T]

  override def apply(cypher: String): MatchResult = {
    Try(parseAst[T](cypher)) match {
      case Success(results)   => ParseResultsMatcher(matchers, ignorePrettifier = ignorePrettifier).apply(results)
      case Failure(exception) => throw new RuntimeException(s"Test framework failed\nCypher: $cypher", exception)
    }
  }
  override protected def createForParser(s: ParserInTest): Self = copy(matchers = Seq.empty, support = _ == s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

trait FluentMatchers[Self <: FluentMatchers[Self, T], T <: ASTNode] extends AstMatchers { self: Self =>
  private val supportedParsers: Seq[ParserInTest] = ParserInTest.AllParsers.filter(support)

  def in(f: ParserInTest => Self => Self): Self = ParserInTest.AllParsers.foldLeft(self) {
    case (acc, p) => acc.addAll(f(p)(acc.createForParser(p)).matchers)
  }
  def withoutErrors: Self = and(successMatcher)
  def withAstLike(assertion: T => Unit): Self = and(haveAstLike(assertion))
  def withPositionOf[S <: ASTNode : ClassTag](expected: InputPosition*): Self = and(haveAstPositions[S](expected: _*))

  def toAst(expected: ASTNode): Self = toAstWith(expected)

  def toAstWith(
    expected: ASTNode,
    prettifierRoundTrip: Boolean = true,
    comparePositions: Boolean = true,
    obfuscator: Boolean = true
  ): Self = {
    var matchers = and(haveAst(expected))
    if (comparePositions) matchers = matchers.withEqualPositions
    if (prettifierRoundTrip && !ignorePrettifier) matchers = matchers.withPrettifierRoundTrip
    if (obfuscator) matchers = matchers.withObfuscatorSanity
    matchers.and(haveEqualWithGraph(expected))
  }

  def toAstIgnorePos(expected: ASTNode, obfuscator: Boolean = true): Self =
    toAstWith(expected, comparePositions = false, obfuscator = obfuscator)

  def toAstPositioned(expected: T, obfuscator: Boolean = true): Self =
    toAstIgnorePos(expected, obfuscator).and(havePositionedAst(expected))
  def toAsts(expected: PartialFunction[ParserInTest, T]): Self = and(expected.andThen(haveAst(_)))
  def containing[C <: ASTNode : ClassTag](expected: C*): Self = and(haveAstContaining(expected: _*))
  def withPrettifierRoundTrip: Self = and(PrettifyToTheSameAst)
  def withObfuscatorSanity: Self = and(ObfuscatorSanity)
  def errorShould(matcher: Matcher[Throwable]): Self = and(failLike(matcher))
  def messageShould(matcher: Matcher[String]): Self = errorShould(matcher.compose(t => norm(t.getMessage)))
  def withError(assertion: Throwable => Unit): Self = errorShould(beLike(assertion))
  def withMessage(expected: String): Self = messageShould(be(norm(expected)))
  def withMessageStart(expected: String): Self = messageShould(startWith(norm(expected)))
  def withMessageContaining(expected: String): Self = messageShould(include(norm(expected)))
  def throws[E <: Throwable](implicit ct: ClassTag[E]): Self = and(AstMatchers.beFailure[E])
  def throws(expected: Class[_ <: Throwable]): Self = and(AstMatchers.beFailure(expected))
  def similarTo(expected: Throwable): Self = throws(expected.getClass).withMessage(expected.getMessage)
  def withAnyFailure: Self = and(failureMatcher)
  def withEqualPositions: Self = addIfMultiParsers(haveEqualPositions(supportedParsers))
  def withSyntaxError(message: String): Self = throws[SyntaxException].withMessage(message)
  def withSyntaxErrorContaining(message: String): Self = throws[SyntaxException].withMessageContaining(message)

  def withGqlStatus(gqlStatusMatcher: GqlExceptionMatcher): Self = {
    withError(throwable => {
      throwable should be(a[ErrorGqlStatusObject])
      throwable.asInstanceOf[ErrorGqlStatusObject] should be(gqlStatusMatcher)
    })
  }

  def withSyntaxErrorGqlStatus(gqlStatusMatcher: GqlExceptionMatcher): Self = {
    withGqlStatus(InvalidSyntaxStatus.withCause(gqlStatusMatcher))
  }

  def withSyntaxErrorContaining(
    message: String,
    causeGql: GqlStatusInfoCodes,
    causeStatusDescription: String,
    position: Option[InputPosition] = None,
    fuzzyStatusDescr: Boolean = false
  ): Self = {
    throws[SyntaxException]
      .withError(throwable => {
        val gqlMatcher = InvalidSyntaxStatus.withCause(causeGql, causeStatusDescription, fuzzyStatusDescr)
        val gqlMatcherWithMaybePos =
          position.map(pos => gqlMatcher.withPosition(pos.offset, pos.line, pos.column)).getOrElse(gqlMatcher)
        throwable.asInstanceOf[Exception] should be(
          gqlException(message, gqlMatcherWithMaybePos, fuzzyMsg = true)
        )
      })
  }

  def withSyntaxError(
    message: String,
    causeGql: GqlStatusInfoCodes,
    causeStatusDescription: String
  ): Self = {
    throws[SyntaxException]
      .withMessage(message)
      .withError(throwable => {
        val gqlMatcher = InvalidSyntaxStatus.withCause(causeGql, causeStatusDescription)
        throwable.asInstanceOf[SyntaxException] should be(gqlMatcher)
      })
  }

  def withOldSyntax(message: String): Self = {
    withSyntaxErrorContaining(
      message,
      GqlStatusInfoCodes.STATUS_42I52,
      "error: syntax error or access rule violation - no longer valid syntax. " + message
    )
  }

  def withOldSyntaxWithPosition(message: String, query: String, position: InputPosition): Self = {
    // the + 1 is to compensate for the " at the start of the query text in the error message
    val padding = " ".repeat(position.offset + 1)
    val legacyMessage =
      s"""$message (line ${position.line}, column ${position.column} (offset: ${position.offset}))
         |"$query"
         |$padding^""".stripMargin

    withSyntaxErrorContaining(
      legacyMessage,
      GqlStatusInfoCodes.STATUS_42I52,
      "error: syntax error or access rule violation - no longer valid syntax. " + message
    )
  }

  def ignored: Self = and(beIgnored)

  final private def and(matcher: Matcher[ParseResult]): Self =
    addAll(supportedParsers.map(asResultsMatcher(_, matcher)))

  final private def and(matchers: PartialFunction[ParserInTest, Matcher[ParseResult]]): Self = {
    addAll(ParserInTest.AllParsers.collect {
      case parser if matchers.isDefinedAt(parser) => asResultsMatcher(parser, matchers(parser))
    })
  }

  final private def add(matcher: Matcher[ParseResults[_]]): Self = copyWith(this.matchers :+ matcher)

  final private def addAll(matchers: Seq[Matcher[ParseResults[_]]]): Self = copyWith(this.matchers ++ matchers)
  private def addIfMultiParsers(m: => Matcher[ParseResults[_]]): Self = if (supportedParsers.size > 1) add(m) else this

  def support: ParserInTest => Boolean
  protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self
  protected def createForParser(parser: ParserInTest): Self
  protected def matchers: Seq[Matcher[ParseResults[_]]]
  protected def ignorePrettifier: Boolean
}

object FluentMatchers {
  type ParseMatcher = (ParserInTest, Matcher[ParseResult])
}

/**
 * Poor mans scalatest matchers.
 * A lot of awkward compositions to avoid having to write custom failure messages.
 * We patch the lacking failure message by adding contexts in the main result matcher.
 */
trait AstMatchers {

  val successMatcher: Matcher[ParseResult] = be.a(Symbol("success")).compose(_.toTry)
  val failureMatcher: Matcher[ParseResult] = be.a(Symbol("failure")).compose(_.toTry)

  def beIgnored: Matcher[ParseResult] = new Matcher[ParseResult] {
    override def apply(left: ParseResult): MatchResult = left match {
      case _ => MatchResult(matches = true, s"Ignored", s"Ignored")
    }
  }

  def beFailure[T <: Throwable](implicit ct: ClassTag[T]): Matcher[ParseResult] =
    beFailure(ct.runtimeClass.asInstanceOf[Class[T]])

  def beFailure(expectedClass: Class[_ <: Throwable]): Matcher[ParseResult] =
    be(Right(expectedClass)).compose[ParseResult](_.toTry.toEither.swap.map(_.getClass))

  def haveAst(expected: ASTNode): Matcher[ParseResult] =
    be(Success(expected)).compose(_.toTry)

  def havePositionedAst(expected: ASTNode): Matcher[ParseResult] =
    be(Right(None)).compose(r => resultAsEither[ASTNode](r).map(findPosMismatch(expected, _)))

  def haveAstLike[T <: ASTNode](assertion: T => Unit): Matcher[ParseResult] =
    be(Right(Success(()))).compose(r => resultAsEither[T](r).map(ast => Try(assertion(ast))))

  def haveAstPositions[S <: ASTNode : ClassTag](expected: InputPosition*): Matcher[ParseResult] =
    be(Right(expected)).compose(r => resultAsEither[ASTNode](r).map(ast => subAsts[S](ast).map(_.position)))

  def haveAstContaining[S <: ASTNode : ClassTag](expected: S*): Matcher[ParseResult] =
    be(Right(expected)).compose(r => resultAsEither[ASTNode](r).map(subAsts[S]))

  def beLike[T](assertion: T => Unit): Matcher[T] =
    be(Success(())).compose[T](v => Try(assertion(v)))

  def bePositioned(expected: ASTNode): Matcher[ASTNode] =
    be(None).compose(findPosMismatch(expected, _))

  def havePositions[S <: ASTNode : ClassTag](exp: InputPosition*): Matcher[ASTNode] =
    be(exp).compose(subAsts)

  def containAst[S <: ASTNode : ClassTag](expected: S*): Matcher[ASTNode] = be(expected).compose(subAsts[S])

  def haveEqualPositions(parsers: Seq[ParserInTest]): Matcher[ParseResults[_]] = new Matcher[ParseResults[_]] {

    override def apply(results: ParseResults[_]): MatchResult = {
      val mismatch = results.result.toSeq
        .filter { case (parser, _) => parsers.contains(parser) }
        .sliding(2)
        .flatMap {
          case Seq((parserA, ParseSuccess(resultA)), (parserB, ParseSuccess(resultB))) =>
            findPosMismatch(resultA, resultB).map(m => Vector[Any](Seq(parserA, parserB), m, results.cypher))
          case Seq((_, _), (_, _)) => Some(Vector[Any](parsers, Some("Parsing failed"), results.cypher))
          case _                   => None
        }
        .nextOption()

      MatchResult(
        mismatch.isEmpty,
        """Expected parsers {0} to have equal positions but found mismatch:
          |{1}
          |
          |Cypher:{2}""".stripMargin,
        "Expected parsers {0} to not have equal positions but found no mismatch",
        mismatch.getOrElse(Vector(parsers, None, results.cypher))
      )
    }
  }

  def haveEqualWithGraph(expected: ASTNode): Matcher[ParseResult] =
    be(Right(None)).compose(r => resultAsEither[ASTNode](r).map(findUseGraphMismatch(expected, _)))

  def failLike(matcher: Matcher[Throwable]): Matcher[ParseResult] = new Matcher[ParseResult] {

    override def apply(left: ParseResult): MatchResult = left match {
      case ParseSuccess(_) => MatchResult(
          matches = false,
          s"Expected to fail, but parsed successfully",
          s"Parsed successfully"
        )
      case failure: ParseFailure => matcher.apply(failure.throwable)
    }
  }

  def asResultsMatcher(parser: ParserInTest, matcher: Matcher[ParseResult]): Matcher[ParseResults[_]] =
    new Matcher[ParseResults[_]] {

      override def apply(left: ParseResults[_]): MatchResult =
        MatchResults.decorate(matcher.apply(left.result(parser)), parser, left)
    }

  protected def resultAsEither[T <: ASTNode](result: ParseResult): Either[ParseFailure, T] = result match {
    case ParseSuccess(ast)     => Right(ast.asInstanceOf[T])
    case failure: ParseFailure => Left(failure)
  }

  protected def resultAsEitherMapped[T <: ASTNode, R](f: T => R)(result: ParseResult): Either[Throwable, R] =
    result.toTry.toEither.map(ast => f(ast.asInstanceOf[T]))
  protected def astOrNull[T <: ASTNode](result: ParseResult): T = result.toTry.getOrElse(null).asInstanceOf[T]
  protected def toTryUnit(result: ParseResult): Try[Unit] = result.toTry.map(_ => ())
  protected def subAsts[S <: ASTNode : ClassTag](ast: ASTNode): Seq[S] = ast.folder.findAllByClass[S]
  protected def norm(in: String): String = if (in == null) "" else in.replaceAll("\\r?\\n", "\n")
}

object AstMatchers extends AstMatchers

object MatchResults {

  private def lazyArg[T](o: T)(f: T => String): AnyRef = new {
    override def toString: String = f(o)
  }

  def merge(results: Seq[MatchResult]): MatchResult = {
    (results.find(_.matches), results.find(!_.matches)) match {
      case (Some(firstMatch), Some(firstNonMatch)) =>
        firstNonMatch.copy(
          rawNegatedFailureMessage = firstMatch.rawFailureMessage,
          negatedFailureMessageArgs = firstMatch.failureMessageArgs,
          rawMidSentenceNegatedFailureMessage = firstMatch.rawMidSentenceFailureMessage,
          midSentenceNegatedFailureMessageArgs = firstMatch.midSentenceFailureMessageArgs
        )
      case (Some(firstMatch), None)    => firstMatch
      case (None, Some(firstNonMatch)) => firstNonMatch
      case (None, None)                => throw new IllegalArgumentException("No assertions!")
    }
  }

  def decorate(result: MatchResult, parser: ParserInTest, parse: ParseResults[_]): MatchResult = {
    val message =
      """{0}
        |
        |Failed assertion
        |################
        |{1}
        |""".stripMargin

    MatchResult(
      matches = result.matches,
      rawFailureMessage = message,
      rawNegatedFailureMessage = message,
      args = Vector[Any](
        lazyArg((parser, parse)) { case (parser, parse) => describe(parser, parse) },
        lazyArg(result)(_.failureMessage)
      )
    )
  }

  private def describe(parser: ParserInTest, parse: ParseResults[_]): String = {
    s"""Parsing results (scroll way down to see assertion failure)
       |##########################################################
       |
       |Failing parser: $parser
       |${describe(parse)}""".stripMargin
  }

  def describe(results: ParseResults[_]): String = {
    val parserResults = results.result.toSeq.map {
      case (parser, ParseSuccess(ast)) =>
        s"""$parser result
           |${"-".repeat(parser.toString.length + 7)}
           |${pprint.apply(ast).render}
           |""".stripMargin
      case (parser, ParseFailure(throwable)) =>
        val hint = throwable match {
          case _: NullPointerException =>
            Some(
              s"NullPointerExceptions can occur because of how ${classOf[AstBuildingAntlrParser]}.isSafeToFreeChildren is implemented"
            )
          case _ => None
        }
        s"""$parser result
           |${"-".repeat(parser.toString.length + 7)}
           |Parsing failed with the following stacktrace, scroll past stacktrace to see assertion error:${hint.map(h =>
            s"\nHint! $h"
          ).getOrElse("")}
           |${ExceptionUtils.getStackTrace(throwable)}
           |""".stripMargin
    }
    s"""Cypher:
       |${results.cypher}
       |
       |${parserResults.mkString("\n")}""".stripMargin
  }
}

/** Asserts that the query can be prettified and the prettified query is parsed to the same AST. */
object PrettifyToTheSameAst extends Matcher[ParseResult] with AstParsing {
  private val expressionStringifier = ExpressionStringifier(alwaysParens = true, alwaysBacktick = true)
  private val prettifier = Prettifier(expressionStringifier)

  override def apply(result: ParseResult): MatchResult = {
    val mismatch = result match {
      case ParseSuccess(ast: Statements) if ast.statements.size == 1 =>
        prettifierMismatch[Statement](result, ast.statements.head, prettifier.asString, StatementParsers)
      case ParseSuccess(ast: Statement) =>
        prettifierMismatch[Statement](result, ast, prettifier.asString, StatementParsers)
      case ParseSuccess(ast: Expression) =>
        prettifierMismatch[Expression](result, ast, expressionStringifier.apply, ExpressionParsers)
      case ParseFailure(throwable) => None
      case _                       => None
    }

    MatchResult(
      mismatch.isEmpty,
      """Expected parser {0} to parse prettified AST.
        |
        |Prettified Query:
        |{1}
        |
        |Parsed Prettified Query (not equal to expected AST):
        |{2}
        |""".stripMargin,
      "Expected AST to NOT prettify into the same AST",
      mismatch.getOrElse(Vector(result.parser, "Not available", "Not available"))
    )
  }

  private def prettifierMismatch[T <: ASTNode](
    result: ParseResult,
    ast: T,
    prettify: T => String,
    parsers: Parsers[T]
  ): Option[Vector[Any]] = {
    val prettified = Try(prettify(ast))
    val rewrittenAst = ast.endoRewrite(rewriter)
    val reParsed = prettified.map(parsers.parse(result.parser, _).endoRewrite(rewriter))
    Option.when(!reParsed.toOption.contains(rewrittenAst)) {
      (prettified, reParsed) match {
        case (Failure(prettifierError), _) =>
          Vector(
            result.parser,
            "Failed to prettify query:\n" + Exceptions.stringify(prettifierError),
            "Not available (prettifying failed)"
          )
        case (Success(pretty), Failure(reParseError)) =>
          Vector(result.parser, pretty, "Failed to parse prettified query:\n" + Exceptions.stringify(reParseError))
        case (Success(pretty), Success(newAst)) =>
          Vector(result.parser, pretty, pprint.apply(newAst).render)
      }
    }
  }

  val rewriter: Rewriter = bottomUp(Rewriter.lift({
    // Ignore input text of return items
    case i @ UnaliasedReturnItem(e, _) => UnaliasedReturnItem(e, "")(i.position)

    // Not sure if this is a bug or not, but the round trip will change `containsIs` so we ignore that
    case labelExpression: LabelExpression if labelExpression.containsIs =>
      labelExpression match {
        case e: BinaryLabelExpression => e match {
            case e: LabelExpression.ColonConjunction => e.copy(containsIs = false)(e.position)
            case e: LabelExpression.ColonDisjunction => e.copy(containsIs = false)(e.position)
          }
        case e: MultiOperatorLabelExpression => e match {
            case e: LabelExpression.Disjunctions => e.copy(containsIs = false)(e.position)
            case e: LabelExpression.Conjunctions => e.copy(containsIs = false)(e.position)
          }
        case e: LabelExpression.Negation    => e.copy(containsIs = false)(e.position)
        case e: LabelExpression.Wildcard    => e.copy(containsIs = false)(e.position)
        case e: LabelExpression.Leaf        => e.copy(containsIs = false)
        case e: LabelExpression.DynamicLeaf => e.copy(containsIs = false)
      }

    // The prettifier hides these
    case e: SensitiveStringLiteral => e.copy("******".getBytes)(e.position)

    case _: InputPosition => InputPosition.NONE
  }))
}

/** Assert that the query can be obfuscated and that the obfuscated query is parsable if the ****** is replaced. */
object ObfuscatorSanity extends Matcher[ParseResult] with AstParsing {
  private val IntReplacement = "(?i)(SKIP|LIMIT|OFFSET) +\\$obfuscationReplacement".r
  private val ReplacementPlusWord = "\\$obfuscationReplacement\\w".r

  override def apply(result: ParseResult): MatchResult = {
    val mismatch = result match {
      case ParseSuccess(ast: Statement) if supportedQuery(result.query) =>
        args(result.query, ast, result.parser)
      case ParseSuccess(ast: Statements) if ast.statements.size == 1 && supportedQuery(result.query) =>
        args(result.query, ast.statements.head, result.parser)
      case _ => None
    }
    MatchResult(
      mismatch.isEmpty,
      s"""Expected obfuscation to not break query.
         |
         |Obfuscation Metadata (all offsets needs to have correct length):
         |{0}
         |
         |Obfuscated Query:
         |{1}
         |
         |Parameterized Obfuscated Query (expected to be parsable):
         |{2}
         |{3}
         |""".stripMargin,
      "Expected obfuscation to break query.",
      mismatch.getOrElse(Vector("-", "-", "-", "-", ""))
    )
  }

  private def supportedQuery(query: String): Boolean = !query.contains("*")

  private def args(query: String, ast: Statement, parser: ParserInTest): Option[Vector[Any]] = {
    val context = new BaseContext {
      override def cypherVersion: CypherVersion = parser match {
        case AstParsing.Cypher5  => CypherVersion.Cypher5
        case AstParsing.Cypher25 => CypherVersion.Cypher25
      }
      override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
      override def notificationLogger: InternalNotificationLogger = devNullLogger
      override def cypherExceptionFactory: CypherExceptionFactory = Neo4jCypherExceptionFactory(query, None)
      override def monitors: Monitors = null
      override def errorHandler: Seq[SemanticErrorDef] => Unit = _ => ()
      override def errorMessageProvider: ErrorMessageProvider = null
      override def cancellationChecker: CancellationChecker = CancellationChecker.neverCancelled()
      override def internalUsageStats: InternalUsageStats = null
      override def sessionDatabase: DatabaseReference = null
      override def semanticFeatures: Seq[SemanticFeature] = Seq.empty
      override def isScopeQuery: Boolean = false
      override def shadowedFunctions: Set[String] = Set.empty
      override def isDebugSession: Boolean = false
    }

    // Try to collect obfuscation metadata
    val obfMetadata = Try(ObfuscationMetadataCollection
      .transform(InitialState(query, null, new AnonymousVariableNameGenerator).withStatement(ast), context)
      .obfuscationMetadata())

    // Obfuscate query
    val obfQuery =
      obfMetadata.toOption.map(m =>
        new CypherQueryObfuscator(m, ObfuscationPolicy.FullLiteralsAlways)
          .fullyObfuscatedQuery(query, org.neo4j.values.virtual.MapValue.EMPTY, 0)
          .text()
      )

    // Replace obfuscation chars with literals
    val paramQuery = obfQuery.map { obfQ =>
      var q = obfQ.replace("******", "$obfuscationReplacement")
      q = IntReplacement.replaceAllIn(q, m => m.group(1) + " 1")
      q
    }

    val replacementsAreIntact = paramQuery.forall(q => ReplacementPlusWord.findFirstIn(q).isEmpty)

    // Parse the parameterized query
    val parsedParamQuery = paramQuery.map(q => Try(StatementsParsers.parse(parser, q)))
    val parsedParamQueryFailure = parsedParamQuery
      .flatMap(_.failed.map(f => "Failed to parse nulled query: " + Exceptions.stringify(f)).toOption)
      .getOrElse("")

    Option.when(!parsedParamQuery.exists(_.isSuccess) ||
      obfMetadata.toOption.exists(_.allLiteralOffsets.exists(_.length.isEmpty)) ||
      !replacementsAreIntact)(
      Vector(
        obfMetadata,
        obfQuery.getOrElse("Not Available"),
        paramQuery.getOrElse("Not Available"),
        parsedParamQueryFailure
      )
    )
  }
}
