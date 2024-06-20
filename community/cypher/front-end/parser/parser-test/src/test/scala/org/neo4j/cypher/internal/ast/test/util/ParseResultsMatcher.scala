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
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseFailure
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseResult
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseSuccess
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.ast.test.util.AstParsing.parseAst
import org.neo4j.cypher.internal.ast.test.util.MatchResults.merge
import org.neo4j.cypher.internal.ast.test.util.VerifyAstPositionTestSupport.findPosMismatch
import org.neo4j.cypher.internal.ast.test.util.VerifyStatementUseGraph.findUseGraphMismatch
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.must.Matchers.startWith

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** ScalaTest Matcher for ParseResults */
case class ParseResultsMatcher[T <: ASTNode : ClassTag](
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty,
  override val support: ParserInTest => Boolean = _ => true
) extends Matcher[ParseResults[_]] with FluentMatchers[ParseResultsMatcher[T], T] {
  type Self = ParseResultsMatcher[T]
  override def apply(results: ParseResults[_]): MatchResult = merge(matchers.map(_.apply(results)))
  override protected def createForParser(s: ParserInTest): Self = ParseResultsMatcher(support = _ == s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

/** ScalaTest Matcher for Cypher strings */
case class ParseStringMatcher[T <: ASTNode : ClassTag](
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty,
  override val support: ParserInTest => Boolean = _ => true
)(implicit p: Parsers[T]) extends Matcher[String] with FluentMatchers[ParseStringMatcher[T], T] {
  type Self = ParseStringMatcher[T]

  override def apply(cypher: String): MatchResult = {
    Try(parseAst[T](cypher)) match {
      case Success(results)   => ParseResultsMatcher(matchers).apply(results)
      case Failure(exception) => throw new RuntimeException(s"Test framework failed\nCypher: $cypher", exception)
    }
  }
  override protected def createForParser(s: ParserInTest): Self = ParseStringMatcher(support = _ == s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

trait FluentMatchers[Self <: FluentMatchers[Self, T], T <: ASTNode] extends AstMatchers { self: Self =>
  private val supportedParsers: Seq[ParserInTest] = ParserInTest.AllParsers.filter(support)

  def in(f: ParserInTest => Self => Self): Self = ParserInTest.AllParsers.foldLeft(self) {
    case (acc, p) => acc.addAll(f(p)(acc.createForParser(p)).matchers)
  }
  def withoutErrors: Self = and(beSuccess)
  def withAstLike(assertion: T => Unit): Self = and(haveAstLike(assertion))
  def withPositionOf[S <: ASTNode : ClassTag](expected: InputPosition*): Self = and(haveAstPositions[S](expected: _*))
  def toAst(expected: ASTNode): Self = and(haveAst(expected)).withEqualPositions.and(haveEqualWithGraph(expected))
  def toAstIgnorePos(expected: ASTNode): Self = and(haveAst(expected)).and(haveEqualWithGraph(expected))
  def toAstPositioned(expected: T): Self = toAstIgnorePos(expected).and(havePositionedAst(expected))
  def toAsts(expected: PartialFunction[ParserInTest, T]): Self = and(expected.andThen(haveAst(_)))
  def containing[C <: ASTNode : ClassTag](expected: C*): Self = and(haveAstContaining(expected: _*))
  def errorShould(matcher: Matcher[Throwable]): Self = and(failLike(matcher))
  def messageShould(matcher: Matcher[String]): Self = errorShould(matcher.compose(t => norm(t.getMessage)))
  def withError(assertion: Throwable => Unit): Self = errorShould(beLike(assertion))
  def withSyntaxError(message: String): Self = throws[SyntaxException].withMessage(message)
  def withSyntaxErrorContaining(message: String): Self = throws[SyntaxException].withMessageContaining(message)
  def withMessage(expected: String): Self = messageShould(be(norm(expected)))
  def withMessageStart(expected: String): Self = messageShould(startWith(norm(expected)))
  def withMessageContaining(expected: String): Self = messageShould(include(norm(expected)))
  def throws[E <: Throwable](implicit ct: ClassTag[E]): Self = and(AstMatchers.beFailure[E])
  def throws(expected: Class[_ <: Throwable]): Self = and(AstMatchers.beFailure(expected))
  def similarTo(expected: Throwable): Self = throws(expected.getClass).withMessage(expected.getMessage)
  def withAnyFailure: Self = and(beFailure)
  def withEqualPositions: Self = addIfMultiParsers(haveEqualPositions(supportedParsers))

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

  val beSuccess: Matcher[ParseResult] = be.a(Symbol("success")).compose(_.toTry)
  val beFailure: Matcher[ParseResult] = be.a(Symbol("failure")).compose(_.toTry)

  def beFailure[T <: Throwable](implicit ct: ClassTag[T]): Matcher[ParseResult] =
    beFailure(ct.runtimeClass.asInstanceOf[Class[T]])

  def beFailure(expectedClass: Class[_ <: Throwable]): Matcher[ParseResult] =
    be(Right(expectedClass)).compose[ParseResult](_.toTry.toEither.swap.map(_.getClass))

  def haveAst(expected: ASTNode): Matcher[ParseResult] =
    be(ParseSuccess(expected))

  def havePositionedAst(expected: ASTNode): Matcher[ParseResult] =
    be(Right(None)).compose(r => resultAsEither[ASTNode](r).map(findPosMismatch(expected, _)))

  def haveAstLike[T <: ASTNode](assertion: T => Unit): Matcher[ParseResult] =
    be(Right(Success())).compose(r => resultAsEither[T](r).map(ast => Try(assertion(ast))))

  def haveAstPositions[S <: ASTNode : ClassTag](expected: InputPosition*): Matcher[ParseResult] =
    be(Right(expected)).compose(r => resultAsEither[ASTNode](r).map(ast => subAsts[S](ast).map(_.position)))

  def haveAstContaining[S <: ASTNode : ClassTag](expected: S*): Matcher[ParseResult] =
    be(Right(expected)).compose(r => resultAsEither[ASTNode](r).map(subAsts[S]))

  def beLike[T](assertion: T => Unit): Matcher[T] =
    be(Success()).compose[T](v => Try(assertion(v)))

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
          case Seq((parserA, resultA), (parserB, resultB)) =>
            findPosMismatch(resultA.toTry.get, resultB.toTry.get)
              .map(m => Vector[Any](Seq(parserA, parserB), m, results.cypher))
          case _ => None
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
    s"""Parsing results
       |###############
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
          case _: NullPointerException => Some("NullPointerExceptions can occur because of how def isSafeToFreeChildren is implemented")
          case _ => None
        }
        s"""$parser result
           |${"-".repeat(parser.toString.length + 7)}
           |Parsing failed with the following stacktrace, scroll past stacktrace to see assertion error:${hint.map(h => s"\nHint! $h").getOrElse("")}
           |${ExceptionUtils.getStackTrace(throwable)}
           |""".stripMargin
    }
    s"""Cypher:
       |${results.cypher}
       |
       |${parserResults.mkString("\n")}""".stripMargin
  }
}
