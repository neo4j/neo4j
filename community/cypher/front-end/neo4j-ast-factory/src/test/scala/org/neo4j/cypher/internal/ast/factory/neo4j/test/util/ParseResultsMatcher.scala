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
package org.neo4j.cypher.internal.ast.factory.neo4j.test.util

import org.neo4j.cypher.internal.ast.factory.neo4j.VerifyAstPositionTestSupport.findPosMismatch
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing._
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.MatchResults.merge
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.Explicit
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.internal.helpers.Exceptions
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.must.Matchers.startWith

import scala.reflect.ClassTag
import scala.util.Success
import scala.util.Try

/** ScalaTest Matcher for ParseResults */
case class ParseResultsMatcher[T <: ASTNode : ClassTag](
  override val support: ParserSupport,
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty
) extends Matcher[ParseResults[_]] with FluentMatchers[ParseResultsMatcher[T], T] {
  type Self = ParseResultsMatcher[T]
  override def apply(results: ParseResults[_]): MatchResult = merge(matchers.map(_.apply(results)))
  override protected def createForParser(s: ParserSupport): Self = ParseResultsMatcher(s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

/** ScalaTest Matcher for Cypher strings */
case class ParseStringMatcher[T <: ASTNode : ClassTag](
  override val support: ParserSupport,
  override val matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty
) extends Matcher[String] with FluentMatchers[ParseStringMatcher[T], T] {
  type Self = ParseStringMatcher[T]
  override def apply(cypher: String): MatchResult = ParseResultsMatcher(support, matchers).apply(parseAst[T](cypher))
  override protected def createForParser(s: ParserSupport): Self = ParseStringMatcher(s)
  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self = copy(matchers = matchers)
}

trait FluentMatchers[Self <: FluentMatchers[Self, T], T <: ASTNode] extends AstMatchers { self: Self =>
  private val supportedParsers: Seq[ParserInTest] = ParserInTest.AllParsers.filterNot(support.ignore)

  def parseIn(s: ParserInTest)(f: Self => Self): Self = addAll(f(createForParser(Explicit(s))).matchers)
  def withoutErrors: Self = and(beSuccess)
  def withAstLike(assertion: T => Unit): Self = and(haveAstLike(assertion))
  def withPositionOf[S <: ASTNode : ClassTag](expected: InputPosition*): Self = and(haveAstPositions[S](expected: _*))
  def toAst(expected: ASTNode): Self = and(haveAst(expected)).withEqualPositions
  def toAstIgnorePos(expected: ASTNode): Self = and(haveAst(expected))
  def toAstPositioned(expected: T): Self = toAstIgnorePos(expected).and(havePositionedAst(expected))
  def toAsts(expected: PartialFunction[ParserInTest, T]): Self = and(expected.andThen(haveAst(_)))
  def containing[C <: ASTNode : ClassTag](expected: C*): Self = and(haveAstContaining(expected: _*))
  def errorShould(matcher: Matcher[Throwable]): Self = and(matcher.compose(forceError))
  def messageShould(matcher: Matcher[String]): Self = errorShould(matcher.compose(t => norm(t.getMessage)))
  def withError(assertion: Throwable => Unit): Self = errorShould(beLike(assertion))
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

  def support: ParserSupport
  protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Self
  protected def createForParser(parser: ParserSupport): Self
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
  protected def forceAst[T <: ASTNode](result: ParseResult): T = result.toTry.get.asInstanceOf[T]
  protected def forceError(result: ParseResult): Throwable = result.toTry.failed.get
  protected def errorMessage(result: ParseResult): String = forceError(result).getMessage
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
        |Assertion failed:
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
    val result = parse(parser) match {
      case ParseSuccess(ast) => if (ast == null) null else ast.toString
      case f: ParseFailure =>
        s"""Failed parsing (${f.getClass.getSimpleName}):
           |${Exceptions.stringify(f.throwable)}
           |""".stripMargin
    }
    s"""Parser: $parser
       |Cypher: ${parse.cypher}
       |Result: $result""".stripMargin
  }
}

sealed trait ParserSupport {

  /** Returns true if this parser should be completely ignored */
  def ignore(parser: ParserInTest): Boolean = false
}

object ParserSupport {
  case object All extends ParserSupport

  case class Explicit(parsers: ParserInTest*) extends ParserSupport {
    override def ignore(parser: ParserInTest): Boolean = !parsers.contains(parser)
  }

  // Indicates limited support in antlr during development.
  // NOTE!! This has very special meanings in some places, see usages for details.
  case object NotAntlr extends ParserSupport {
    override def ignore(parser: ParserInTest): Boolean = parser == Antlr
  }

  case object NotAnyAntlr extends ParserSupport {
    override def ignore(parser: ParserInTest): Boolean = parser == Antlr
  }
}
